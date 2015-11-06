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
package com.datatorrent.contrib.kafka;

import com.beust.jcommander.internal.Maps;
import com.datatorrent.netlet.util.DTThrowable;
import com.google.common.collect.Lists;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.elasticsearch.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * ZooKeeperOffsetManager
 */
public class ZooKeeperOffsetManager implements OffsetManager {

    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperOffsetManager.class);

    @NotNull
    private String connectString;
    private int sessionTimeout = 30000;

    @NotNull
    private String parentPath = "";

    private ZooKeeperWatcher zooKeeperWatcher;

    private transient ZooKeeper zooKeeper;

    @Override
    public Map<KafkaPartition, Long> loadInitialOffsets() {
        chkInitZooKeeper();
        Map<KafkaPartition, Long> offsetsOfPartitions = Maps.newHashMap();
        try {
            List<String> descriptors = Lists.newArrayList();
            populateOffsets(parentPath, descriptors, offsetsOfPartitions);
        } catch (Exception e) {
           DTThrowable.rethrow(e);
        }
        return offsetsOfPartitions;
    }

    @Override
    public void updateOffsets(Map<KafkaPartition, Long> offsetsOfPartitions) {
        chkInitZooKeeper();
        for (Map.Entry<KafkaPartition, Long> offsetOfPartition : offsetsOfPartitions.entrySet()) {
            try {
                String path = chkCreatePath(offsetOfPartition.getKey());
                zooKeeper.setData(path, Longs.toByteArray(offsetOfPartition.getValue()), -1);
            } catch (KeeperException e) {
                logger.error("Error storing offset {}", e);
            } catch (InterruptedException e) {
                logger.error("Error storing offset {}", e);
            }
        }
    }

    private void populateOffsets(String path, List<String> descriptors, Map<KafkaPartition, Long> offsetsOfPartitions) throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(path.equals(parentPath) ? "/" : path, false);
        for (String child : children) {
            String childPath = path + "/" + child;
            if (descriptors.size() == 2) {
                KafkaPartition kafkaPartition = new KafkaPartition(descriptors.get(0), descriptors.get(1), Integer.valueOf(child));
                byte[] boffset = zooKeeper.getData(childPath, false, new Stat());
                offsetsOfPartitions.put(kafkaPartition, Longs.fromByteArray(boffset));
            } else {
                descriptors.add(child);
                populateOffsets(childPath, descriptors, offsetsOfPartitions);
                descriptors.remove(descriptors.size() - 1);
            }
        }
    }

    private void chkInitZooKeeper() {
        if (zooKeeper == null) {
            try {
                zooKeeperWatcher = new ZooKeeperWatcher();
                zooKeeper = new ZooKeeper(connectString, sessionTimeout, zooKeeperWatcher);
            } catch (IOException e) {
                DTThrowable.rethrow(e);
            }
        }
    }

    private String chkCreatePath(KafkaPartition kafkaPartition) throws KeeperException, InterruptedException {
        StringBuilder sb = new StringBuilder(parentPath);
        chkCreatePath(sb, kafkaPartition.getClusterId());
        chkCreatePath(sb, kafkaPartition.getTopic());
        chkCreatePath(sb, "" + kafkaPartition.getPartitionId());
        return sb.toString();
    }

    private void chkCreatePath(StringBuilder pathBuilder, String element) throws KeeperException, InterruptedException {
        pathBuilder.append("/").append(element);
        String path = pathBuilder.toString();
        if (zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private class ZooKeeperWatcher implements Watcher {

        @Override
        public void process(WatchedEvent watchedEvent) {
            logger.info("Event : {}", watchedEvent);
        }
    }

    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public String getParentPath() {
        return parentPath;
    }

    public void setParentPath(String parentPath) {
        this.parentPath = parentPath;
    }
}
