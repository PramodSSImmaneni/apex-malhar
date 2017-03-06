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
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPReconnectEventHandler;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.netlet.util.DTThrowable;

@SuppressWarnings("unused")
public abstract class AbstractSolaceBaseInputOperator<T> extends BaseOperator implements
    InputOperator, Operator.ActivationListener<Context.OperatorContext>, CheckpointListener
{

  private static final Logger logger = LoggerFactory.getLogger(AbstractSolaceBaseInputOperator.class);

  @NotNull
  protected JCSMPProperties properties = new JCSMPProperties();
  protected String connectRetries;
  protected String reconnectRetries;
  protected String unackedMessageLimit;


  //protected IdempotentStorageManager idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
  protected FSOpsIdempotentStorageManager idempotentStorageManager = new FSOpsIdempotentStorageManager();

  protected transient JCSMPFactory factory;
  protected transient JCSMPSession session;

  protected transient Consumer consumer;
  protected transient Consumer reliableConsumer;

  protected transient int operatorId;
  protected long windowId;
  protected transient long lastCompletedWId;

  protected transient int emitCount;

  protected transient volatile boolean DRFailover = false;
  protected transient volatile boolean TCPDisconnected = false;

  protected transient BlockingQueue<BytesXMLMessage> unackedMessages; // hosts the Solace messages that need to be acked when the streaming window is OK to remove
  protected LinkedList<Long> inFlightMessageId = new LinkedList<Long>(); //keeps track of all in flight IDs since they are not necessarily sequential
  protected transient ArrayBlockingQueue<BytesXMLMessage> arrivedTopicMessagesToProcess;

  //protected transient com.solace.dt.operator.DTSolaceOperatorInputOutput.ArrayBlockingQueue<BytesXMLMessage> arrivedMessagesToProcess;

  private transient ReconnectCallbackHandler rcHandler = new ReconnectCallbackHandler();

  private transient CallbackMessageHandler cbHandler = new CallbackMessageHandler();

  int spinMillis;

  protected transient int reconnectRetryMillis = 0;

  @Override
  public void setup(Context.OperatorContext context)
  {

    operatorId = context.getId();
    logger.info("OperatorID from Base class: {}", operatorId);
    spinMillis = context.getValue(com.datatorrent.api.Context.OperatorContext.SPIN_MILLIS);
    factory = JCSMPFactory.onlyInstance();

    //Required for HA and DR to try forever if set to "-1"
    JCSMPChannelProperties channelProperties = (JCSMPChannelProperties)this.properties.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
    channelProperties.setConnectRetries(Integer.parseInt(this.connectRetries));
    channelProperties.setReconnectRetries(Integer.parseInt(this.reconnectRetries));

    reconnectRetryMillis = channelProperties.getReconnectRetryWaitInMillis();


    try {
      session = factory.createSession(this.properties, null, new PrintingSessionEventHandler());
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }

    //logger.debug("Properties Raw: \n{}", properties.toProperties());
    logger.debug("Properties:\n" + properties.toString());
    //logger.debug("\n===============================================\n");

    idempotentStorageManager.setup(context);
    lastCompletedWId = idempotentStorageManager.getLargestRecoveryWindow();
    //logger.debug("++++++++++++++++++++Largest Completed: " + lastCompletedWId);


  }

  @Override
  public void checkpointed(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void committed(long window)
  {
    try {
      idempotentStorageManager.deleteUpTo(operatorId, window);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }

  }

  protected T processMessage(BytesXMLMessage message)
  {
    T tuple = convert(message);
    if (tuple != null) {
      emitTuple(tuple);
    }
    return tuple;
  }


  @Override
  public void activate(Context.OperatorContext context)
  {
    try {
      session.connect();
      reliableConsumer = session.getMessageConsumer(rcHandler, cbHandler);
      //consumer = getConsumer();
      reliableConsumer.start();

    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void deactivate()
  {
    try {
      consumer.stop();
      clearConsumer();
      consumer.close();
      reliableConsumer.close();
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void teardown()
  {

    idempotentStorageManager.teardown();
    session.closeSession();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.windowId = windowId;
  }


  protected abstract T convert(BytesXMLMessage message);

  protected abstract void emitTuple(T tuple);

  protected abstract Consumer getConsumer() throws JCSMPException;

  protected abstract void clearConsumer() throws JCSMPException;

  public void setProperties(JCSMPProperties properties)
  {
    this.properties = properties;

  }


  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return idempotentStorageManager;
  }

  public void setUnackedMessageLimit(String unackedMessageLimit)
  {
    this.unackedMessageLimit = unackedMessageLimit;
  }

  public String getUnackedMessageLimit()
  {
    return unackedMessageLimit;
  }

  public void setConnectRetries(String connectRetries)
  {
    this.connectRetries = connectRetries;
    logger.info("+++++++++++++++++++reconnectRetries: {}", this.connectRetries);
  }

  public void setReconnectRetries(String reconnectRetries)
  {
    this.reconnectRetries = reconnectRetries;
    logger.info("+++++++++++++++++++reconnectRetries: {}", this.reconnectRetries);
  }

  public void setReapplySubscriptions(boolean state)
  {
    this.properties.setBooleanProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, state);
  }

  public void startConsumer()
  {
    try {
      consumer = getConsumer();
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }


  public class ReconnectCallbackHandler implements JCSMPReconnectEventHandler
  {


    @Override
    public void postReconnect() throws JCSMPException
    {

      logger.info("++++++++++++++Solace client now Reconnected --  possibe Solace HA or DR fail-over +++++++++++++");
      TCPDisconnected = false;

    }

    @Override
    public boolean preReconnect() throws JCSMPException
    {
      DRFailover = false;
      logger.info("++++++++++++++Solace client now in Pre Reconnect state -- possibe Solace HA or DR fail-over +++++++++++++");
      TCPDisconnected = true;
      return true;
    }

  }

  public class PrintingSessionEventHandler implements SessionEventHandler
  {


    public void handleEvent(SessionEventArgs event)
    {
      logger.info("Received Session Event %s with info %s\n", event.getEvent(), event.getInfo());

      // Received event possibly due to DR fail-ver complete
      if (event.getEvent() == SessionEvent.VIRTUAL_ROUTER_NAME_CHANGED) {
        DRFailover = true; // may or may not need recovery
        TCPDisconnected = false;
      }

    }
  }

  public class CallbackMessageHandler implements XMLMessageListener
  {

    @Override
    public void onException(JCSMPException e)
    {
      DTThrowable.rethrow(e);

    }

    @Override
    public void onReceive(BytesXMLMessage msg)
    {
      arrivedTopicMessagesToProcess.add(msg);

    }

  }


}
