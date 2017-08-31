/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.solace;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.NotNull;
import com.google.common.collect.Maps;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.netlet.util.DTThrowable;

@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractSolaceGuaranteedIdempotentInputOperator<T> extends AbstractSolaceBaseInputOperator<T> implements InputOperator, Operator.ActivationListener<Context.OperatorContext>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSolaceGuaranteedIdempotentInputOperator.class);

  @NotNull
  protected String endpointName;


  //@NotNull
  //protected transient EndpointType endpointType = EndpointType.QUEUE;
  private transient Endpoint endpoint;
  @NotNull
  public transient FlowCallbackMessageHandler flowHandler = new FlowCallbackMessageHandler();
  @NotNull
  protected transient EndpointProperties endpointProperties = new EndpointProperties();
  private transient BytesXMLMessage recentMessage = null;

  private transient long[] operatorRecoveredWindows;
  protected transient long currentWindowId;
  @SuppressWarnings("unused")
  private final transient AtomicReference<Throwable> throwable;
  protected transient ArrayBlockingQueue<BytesXMLMessage> arrivedMessagesToProcess;
  protected transient BlockingQueue<BytesXMLMessage> unackedMessages;

  protected transient TreeMap<Long, BytesXMLMessage> lastMessages = new TreeMap<Long, BytesXMLMessage>();
  @SuppressWarnings("unused")
  private transient long windowTime;
  protected final transient Map<Long, T> currentWindowRecoveryState;
  protected transient LinkedList<T> inFlightRecoveryMessages = new LinkedList<T>(); // used by partition that restarts for duplicate detection
  protected transient LinkedList<T> inFlightRecoveryMessagesPartition = new LinkedList<T>(); // used by partitions that didn't restart for duplicate detection
  protected transient LinkedList<T> inFlightRecoveryMessagesDR = new LinkedList<T>(); // used by partition that detect DR fail over for duplicate detection

  private transient Context.OperatorContext context;
  private transient int partitionCount = 0;

  protected static final transient int DEFAULT_BUFFER_SIZE = 500;
  protected transient int drCounterSize = 2 * DEFAULT_BUFFER_SIZE;
  protected transient int drCounter = 0;

  protected transient volatile boolean doneDups = false;
  protected transient volatile boolean doneDupsPartitioned = true;
  protected transient volatile boolean donePartitionCheck = false;
  protected transient volatile boolean doneDupsDR = true;


  public AbstractSolaceGuaranteedIdempotentInputOperator()
  {
    throwable = new AtomicReference<Throwable>();
    currentWindowRecoveryState = Maps.newLinkedHashMap();
  }


  protected boolean messageConsumed(BytesXMLMessage message) throws JCSMPException
  {
    if (message.getRedelivered()) {
      return false;
    }
    return true;
  }


  @Override
  public void setup(Context.OperatorContext context)
  {
    this.context = context;

    LOG.info("Initial Partition Count: {}", currentpartitionCount());

    inFlightRecoveryMessages.clear();
    inFlightRecoveryMessagesPartition.clear();
    inFlightRecoveryMessagesDR.clear();


    //super.setUnackedMessageLimit(this.unackedMessageLimit);
    //setup info for HA and DR at the transport level
    //super.setConnectRetries(this.connectRetries);
    //super.setReconnectRetries(this.reconnectRetries);


    super.setup(context);

    LOG.info("Operator ID = " + context.getId());

    windowTime = context.getValue(OperatorContext.APPLICATION_WINDOW_COUNT) * context.getValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS);

    arrivedMessagesToProcess = new ArrayBlockingQueue<BytesXMLMessage>(unackedMessageLimit);

    unackedMessages = new ArrayBlockingQueue<BytesXMLMessage>(unackedMessageLimit * (context.getValue(OperatorContext.APPLICATION_WINDOW_COUNT)) * 2);

    endpoint = factory.createQueue(this.endpointName);

    /*
    if (currentWindowId > idempotentStorageManager.getLargestRecoveryWindow()) {
      super.startConsumer();
    }
    */

    try {
      operatorRecoveredWindows = idempotentStorageManager.getWindowIds(context.getId());
      if (operatorRecoveredWindows != null) {
        Arrays.sort(operatorRecoveredWindows);
      }
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  protected T processMessage(BytesXMLMessage message)
  {
    T payload = super.processMessage(message);
    if (payload != null) {
      currentWindowRecoveryState.put(message.getMessageIdLong(), payload);
    }
    recentMessage = message;
    return payload;
  }

  @Override
  public void beginWindow(long windowId)
  {
    //LOG.debug("Largest Recovery Wndow is : {} for current window: {}", idempotentStorageManager.getLargestRecoveryWindow(), windowId);
    super.beginWindow(windowId);
    if (windowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      //LOG.debug("About to handle recovery, current windowID is: {} largested recovered ID is: {}" + currentWindowId, idempotentStorageManager.getLargestRecoveryWindow());
      handleRecovery(windowId);
    } else {
      if (super.consumer == null) {
        super.startConsumer();
        LOG.debug("Started Flow Consumer after recovery is complete");
      }
    }
  }


  @SuppressWarnings("unchecked")
  protected void handleRecovery(long windowId)
  {
    LOG.info("Handle Recovery called");

    Map<Long, T> recoveredData;
    try {
      recoveredData = (Map<Long, T>)idempotentStorageManager.load(operatorId, windowId);

      if (recoveredData == null) {
        return;
      }
      for (Map.Entry<Long, T> recoveredEntry : recoveredData.entrySet()) {
        emitTuple(recoveredEntry.getValue());
        inFlightRecoveryMessages.add(recoveredEntry.getValue());
      }

    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }

  }


  @Override
  public void endWindow()
  {
    @SuppressWarnings("unused")
    boolean stateSaved = false;
    boolean ackCompleted = false;
    int messagesToAck = unackedMessages.size();

    if (currentWindowId > idempotentStorageManager.getLargestRecoveryWindow()) {


      if (recentMessage != null) {
        lastMessages.put(currentWindowId, recentMessage);
      }

      try {

        if (recentMessage != null) {
          idempotentStorageManager.save(currentWindowRecoveryState, operatorId, currentWindowId);
          stateSaved = true;
          LOG.debug("Saved for window: " + currentWindowId);
          currentWindowRecoveryState.clear();
          LOG.debug("acking messages");
          ackCompleted = ackMessages();
          LOG.debug("Acked status: " + ackCompleted + " on window " + currentWindowId + " ack count: : " + messagesToAck);
        }
      } catch (Throwable t) {
        if (!ackCompleted) {
          LOG.info("confirm recovery of {} for {} does not exist", operatorId, currentWindowId, t);
        }
        DTThrowable.rethrow(t);
      }


      emitCount = 0; //reset emit count
    } else {
      currentWindowRecoveryState.clear();
      ackCompleted = ackMessages();
      LOG.debug("acking messages completed successfully: " + ackCompleted);
    }
  }


  @Override
  public void emitTuples()
  {
    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      return;
    }
    //If in HA or DR fail-over, block until Solace TCP connection is reestablished so recovery windows are not lost as empty windows
    int sleepCounter = 0;
    while (tcpDisconnected) {
      sleepCounter++;
      try {
        Thread.sleep(super.reconnectRetryMillis);
      } catch (InterruptedException e) {
        DTThrowable.rethrow(e);
      }
      if (sleepCounter % 10 == 0) {
        LOG.info("Sleeping for another 30 seconds waiting for TCP reconnect for Solace with milliseconds sleep per cycle = {}", super.reconnectRetryMillis);
      }
      LOG.info("Queued messages to process: {}", arrivedMessagesToProcess.size());
    }


    BytesXMLMessage message;

    try {
      // process messages, window is defined by emitCount or timeout of waiting for messages for 10 milliseocnds
      while (emitCount < DEFAULT_BUFFER_SIZE && ((message = (BytesXMLMessage)arrivedMessagesToProcess.poll(10, TimeUnit.MILLISECONDS)) != null)) {


        boolean goodToGo = true;
        if (message != null) {

          //LOG.debug("To be processed: {} sequence number: {}  Received message with ID: {} and AppID: {} redelivered: {}", arrivedMessagesToProcess.size(), message.getSequenceNumber(), message.getMessageIdLong(), message.getApplicationMessageId(), message.getRedelivered());
          //LOG.debug(" AppID = {}  redelivered: {}", message.getApplicationMessageId(), message.getRedelivered());


          // Checking for duplicates after recovery from operator restart looking for re-delivered messages in restarted operator
          if (message.getRedelivered() && inFlightRecoveryMessages.size() > 0) {
            T payload = convert(message);
            if (inFlightRecoveryMessages.contains(payload)) {
              LOG.info("Redelivered Message Duplicate possibly due to input operator restart");
              goodToGo = false;
              if (message.getDeliveryMode() == DeliveryMode.PERSISTENT || message.getDeliveryMode() == DeliveryMode.NON_PERSISTENT) {
                unackedMessages.add(message);
              }

              recentMessage = message;

            }


          } else {
            doneDups = true;
          }

          /*
          if(message.getRedelivered() ) {
              LOG.debug("In FLight Size: {} Current Part Count: {} Dups: {} and {}", inFlightRecoveryMessages.size(), currentpartitionCount(), doneDupsPartitioned, donePartitionCheck);
          }
          */

          //Operator was not restarted, re-delivered messages are a result of another partitioned operator restart
          if (message.getRedelivered() && inFlightRecoveryMessages.size() == 0 && currentpartitionCount() > 1 && doneDupsPartitioned && donePartitionCheck == false) {
            try {
              doneDupsPartitioned = loadPartitionReplayCheck();
            } catch (IOException e) {
              DTThrowable.rethrow(e);
            }
            donePartitionCheck = true;
          }

          if (message.getRedelivered() && doneDupsPartitioned == false && inFlightRecoveryMessagesPartition.size() >= 0) {
            T payload = convert(message);
            if (inFlightRecoveryMessagesPartition.contains(payload)) {
              LOG.info("Redelivered Message Duplicate possibly due to input operator restart in another partition");
              goodToGo = false;
              if (message.getDeliveryMode() == DeliveryMode.PERSISTENT || message.getDeliveryMode() == DeliveryMode.NON_PERSISTENT) {
                unackedMessages.add(message);
              }

              recentMessage = message;

            }
          } else {
            doneDupsPartitioned = true;
            donePartitionCheck = false; //Get ready in case another partition restarts and results in replayed messages
          }


          // Checking for duplicates after recovery from DR looking for redelivered messages
          if (drFailover && !(message.getRedelivered()) && doneDupsDR && donePartitionCheck == false) {
            try {
              doneDupsDR = loadPartitionReplayCheck();
            } catch (IOException e) {
              DTThrowable.rethrow(e);
            }
            donePartitionCheck = true;
            drCounterSize = drCounterSize + arrivedMessagesToProcess.size();
          }


          if (inFlightRecoveryMessagesDR.size() == 0 && drFailover) {
            drFailover = false;
            doneDupsDR = true;
            donePartitionCheck = false;
            inFlightRecoveryMessagesDR.clear();
            LOG.info("Cleared in flight recovery messages, no more possible duplicate messages detected after DR fail over");
          }

          if (!(message.getRedelivered()) && doneDupsDR == false && inFlightRecoveryMessagesDR.size() > 0 && drFailover && drCounter < drCounterSize) {
            drCounter++;
            T payload = convert(message);
            if (inFlightRecoveryMessagesDR.contains(payload)) {
              LOG.info("Message Duplicate detected after Solace DR fail over");
              goodToGo = false;
              if (message.getDeliveryMode() == DeliveryMode.PERSISTENT || message.getDeliveryMode() == DeliveryMode.NON_PERSISTENT) {
                unackedMessages.add(message);
              }

              recentMessage = message;

            }
            //Reset DR processing for duplicates after 2 windows worth of message checks
          } else if (drCounter == drCounterSize) {
            //Once there are no more duplicates detected there will be no more duplicates due to DR fail over
            doneDupsDR = true;
            donePartitionCheck = false;
            inFlightRecoveryMessagesDR.clear();
            drFailover = false;
            drCounter = 0;
            drCounterSize = 2 * DEFAULT_BUFFER_SIZE;
            LOG.info("Cleared in flight recovery messages, no more possible duplicate messages detected after DR fail over");
          }


          if (goodToGo) {
            //if the redelivery flag is no no longer on the messages we can dispose of the inFLightRecoveryMessages
            if (message.getRedelivered() == false && inFlightRecoveryMessages.size() > 0 && doneDups) {
              inFlightRecoveryMessages.clear();
              LOG.info("Cleared in flight recovery messages, no more redelivered or DR recovery messages");
              doneDups = false;
            }
            if (message.getRedelivered() == false && inFlightRecoveryMessagesPartition.size() > 0 && doneDupsPartitioned) {
              inFlightRecoveryMessagesPartition.clear();
              LOG.info("Cleared in flight recovery messages, no more redelivered  messages");
              doneDupsPartitioned = false;
            }

            processMessage(message);
            if (message.getDeliveryMode() == DeliveryMode.PERSISTENT || message.getDeliveryMode() == DeliveryMode.NON_PERSISTENT) {
              unackedMessages.add(message);
            }
            emitCount++;
          }
        }
      }
    } catch (InterruptedException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void committed(long window)
  {
    if (recentMessage == null) {
      return;
    }

    Set<Long> windows = lastMessages.keySet();
    Iterator<Long> iterator = windows.iterator();
    while (iterator.hasNext()) {
      if (iterator.next() <= window) {
        iterator.remove();
      } else {
        break;
      }
    }

    super.committed(window);
  }

  private boolean ackMessages()
  {
    boolean processedOK = false;


    BytesXMLMessage messageToAckUpTo = lastMessages.get(currentWindowId);

    if (messageToAckUpTo != null) {
      if (unackedMessages.size() > 0) {
        while (unackedMessages.peek() != messageToAckUpTo) {
          try {
            BytesXMLMessage taken = unackedMessages.take();
            //LOG.debug("Acking: {}", taken.getApplicationMessageId());
            taken.ackMessage();
          } catch (InterruptedException e) {
            DTThrowable.rethrow(e);
          }
        }
        if (unackedMessages.peek() == messageToAckUpTo) {
          try {
            BytesXMLMessage taken = unackedMessages.take();
            taken.ackMessage();
          } catch (InterruptedException e) {
            DTThrowable.rethrow(e);
          }
        }
        processedOK = true;
      } else {
        LOG.debug("Unacked Array is size zero");

      }
    } else {
      LOG.info("messageToAckUpTo is null -- possibly due to being in recovery stage");
    }


    return processedOK;
  }

  public void setEndpointName(String endpointName)
  {
    this.endpointName = endpointName;
    LOG.info("enpointName: {}", this.endpointName);
  }

  public int currentpartitionCount()
  {

    Attribute<Partitioner<? extends Operator>> parts = OperatorContext.PARTITIONER;

    //If only one partition the attribute is null
    if (context.getValue(parts) != null) {
      partitionCount = ((StatelessPartitioner<?>)context.getValue(parts)).getPartitionCount();
      LOG.debug("Current Partition Count: " + partitionCount);
    } else {
      partitionCount = 1;
      LOG.debug("Current Partition Count: " + partitionCount);
    }
    return partitionCount;
  }


  @SuppressWarnings("unchecked")
  public boolean loadPartitionReplayCheck() throws IOException
  {
    if (!(drFailover)) {
      LOG.info("Received redelivered message from Solace, another parition must have restarted");
    } else {
      LOG.info("Received DR fail over event from Solace, the partiions are now talking to another Solace Router");
    }
    FSOpsIdempotentStorageManager snapshotState = idempotentStorageManager.getIdempotentStateSnapshot();
    boolean _doneDupsPartitioned = true;
    LOG.info("Largest recovery window: {}", snapshotState.getLargestRecoveryWindow());

    LOG.info("Recovery Path: {}", snapshotState.getRecoveryPath());


    Set<Integer> opIds = snapshotState.getOperatorIds();
    LOG.info("Received {} operatorIDs, with values:", opIds.size());

    int[] arrOpIds = new int[opIds.size()];
    int index = 0;
    for (Integer i : opIds) {
      arrOpIds[index++] = i;
    }
    for (int x = 0; x < arrOpIds.length; x++) {
      LOG.info(Integer.toString(arrOpIds[x]));
    }


    for (int i = 0; i < arrOpIds.length; i++) {


      long[] wins = snapshotState.getOrderedWindowIds(arrOpIds[i]);
      try {
        //Get last two recovery windows

        LOG.info("Window to recover: {}  for partition: {}", ((long)wins[wins.length - 1]), arrOpIds[i]);
        Map<Long, T> recoveredData = (Map<Long, T>)snapshotState.load(arrOpIds[i], ((long)wins[wins.length - 1]));

        if (recoveredData == null) {
          LOG.info("Recovered data is null for window: {}", ((long)wins[wins.length - 1]));

        } else {
          for (Map.Entry<Long, T> recoveredEntry : recoveredData.entrySet()) {
            if (!(drFailover)) {
              inFlightRecoveryMessagesPartition.add(recoveredEntry.getValue());
            } else {
              inFlightRecoveryMessagesDR.add(recoveredEntry.getValue());
            }
          }


          LOG.info("Recovered data is {} messages for window: {}" + recoveredData.size(), ((long)wins[wins.length - 1]));
        }

        LOG.info("Window to recover: {}  for partition: {}", ((long)wins[wins.length - 2]), arrOpIds[i]);
        recoveredData = (Map<Long, T>)snapshotState.load(arrOpIds[i], ((long)wins[wins.length - 2]));

        if (recoveredData == null) {
          LOG.info("Recovered data is null for window: {}", ((long)wins[wins.length - 2]));
          //continue;
        } else {
          for (Map.Entry<Long, T> recoveredEntry : recoveredData.entrySet()) {
            if (!(drFailover)) {
              inFlightRecoveryMessagesPartition.add(recoveredEntry.getValue());
            } else {
              inFlightRecoveryMessagesDR.add(recoveredEntry.getValue());
            }
          }
          LOG.info("Recovered data is {}  messages for window: {}", recoveredData.size(), ((long)wins[wins.length - 2]));
        }
        _doneDupsPartitioned = false;

      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }


      LOG.info("Added parition data from partition: {}", arrOpIds[i]);
    }

    if (!(drFailover)) {
      LOG.info("Total Recovery Partition Data Records: {} ", inFlightRecoveryMessagesPartition.size());
    } else {
      LOG.info("Total Recovery DR fail over Data Records: {}", inFlightRecoveryMessagesDR.size());
    }
    snapshotState.teardown();
    return _doneDupsPartitioned;
  }


  // Start the actual consumption of Solace messages from the Queue
  protected Consumer getConsumer() throws JCSMPException
  {
    ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();

    consumerFlowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
    consumerFlowProperties.setEndpoint(this.endpoint);

    FlowReceiver f_receiver = session.createFlow(flowHandler, consumerFlowProperties);
    f_receiver.start();
    LOG.info("Flow started on queue: {}", f_receiver.getDestination());
    return f_receiver;
  }

  //public void setIdempotentStorageManager(IdempotentStorageManager storageManager)
  public void setIdempotentStorageManager(FSOpsIdempotentStorageManager storageManager)
  {
    this.idempotentStorageManager = storageManager;
  }


  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return this.idempotentStorageManager;
  }


  public class FlowCallbackMessageHandler implements XMLMessageListener
  {
    @Override
    public void onException(JCSMPException e)
    {
      DTThrowable.rethrow(e);
    }

    @Override
    public void onReceive(BytesXMLMessage message)
    {
      try {
        arrivedMessagesToProcess.put(message);
      } catch (InterruptedException e) {
        DTThrowable.rethrow(e);
      }
    }
  }


}
