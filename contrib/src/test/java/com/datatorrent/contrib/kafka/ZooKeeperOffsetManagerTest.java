package com.datatorrent.contrib.kafka;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.datatorrent.netlet.util.DTThrowable;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by pramod on 11/5/15.
 */
public class ZooKeeperOffsetManagerTest {

    private int zkPort = 2181;
    private int numBrokers = 5;
    private int startPort = 20000;

    private static File testDir;
    private File workDir;

    private ServerCnxnFactory cnxnFactory;
    private ZooKeeperServer zooKeeperServer;
    private String zooKeeperAddress;

    private KafkaServer[] kafkaServers;

    @Rule
    public TestName name = new TestName() {
        @Override
        protected void starting(Description d) {
            testDir = new File("target", d.getClassName());
            testDir.mkdir();
            workDir = new File(testDir, d.getMethodName());
            try {
                cnxnFactory = NIOServerCnxnFactory.createFactory(zkPort, 6);
                File zkSnapshotDir = new File(workDir, "zksnaps");
                File zkLogDir = new File(workDir, "zklogs");
                zooKeeperServer = new ZooKeeperServer(zkSnapshotDir, zkLogDir, 500);
                cnxnFactory.startup(zooKeeperServer);
            } catch (IOException e) {
                DTThrowable.rethrow(e);
            } catch (InterruptedException e) {
                DTThrowable.rethrow(e);
            }
            zooKeeperAddress = "localhost:" + zkPort;
            /*
            kafkaServers = new KafkaServer[numBrokers];
            for (int i = 0; i < numBrokers; ++i) {
                File kafkaLogDir = new File(workDir, "kafkalogs-" + i);
                Properties properties = new Properties();
                properties.setProperty("broker.id", "" + (i + 1));
                properties.setProperty("host.name", "localhost");
                properties.setProperty("port", "" + (startPort + i));
                properties.setProperty("zookeeper.connect", zooKeeperAddress);
                properties.setProperty("log.dir", kafkaLogDir.getAbsolutePath());
                KafkaConfig kafkaConfig = new KafkaConfig(properties);
                KafkaServer kafkaServer = new KafkaServer(kafkaConfig, new KafkaTime());
                kafkaServers[i] = kafkaServer;
                kafkaServer.startup();
            }
            */
        }

        @Override
        protected void finished(Description description) {
            /*
            for (KafkaServer kafkaServer : kafkaServers) {
                kafkaServer.shutdown();
            }
            */
            cnxnFactory.shutdown();
            FileUtils.deleteQuietly(workDir);
            FileUtils.deleteQuietly(testDir);
        }
    };

    @Test
    public void testComponent() throws InterruptedException {
        //KafkaServer kafkaServer = new KafkaServer();
        ZooKeeperOffsetManager offsetManager = new ZooKeeperOffsetManager();
        offsetManager.setConnectString(zooKeeperAddress);
        Map<KafkaPartition, Long> offsetsOfPartitions = Maps.newHashMap();
        Random random = new Random();
        List<Long> offsets = Lists.newArrayList();
        for (int i = 0; i < 3; ++i) {
            KafkaPartition kafkaPartition = new KafkaPartition("test", i);
            long offset = random.nextLong();
            offsetsOfPartitions.put(kafkaPartition, offset);
            offsets.add(offset);
        }
        offsetManager.updateOffsets(offsetsOfPartitions);
        Map<KafkaPartition, Long> roffsetsOfParitions = offsetManager.loadInitialOffsets();
        Assert.assertEquals("Number of paritions", offsets.size(), roffsetsOfParitions.size());
        for (Map.Entry<KafkaPartition, Long> roffsetOfPartition : roffsetsOfParitions.entrySet()) {
            KafkaPartition kafkaPartition = roffsetOfPartition.getKey();
            Assert.assertEquals("Topic", "test", kafkaPartition.getTopic());
            int partitionId = kafkaPartition.getPartitionId();
            Assert.assertTrue("Partition id within range", partitionId < offsets.size());
            Assert.assertEquals("Partition Offset", offsets.get(partitionId), roffsetOfPartition.getValue());
        }
    }

    private class KafkaTime implements Time {

        @Override
        public long milliseconds() {
            return System.currentTimeMillis();
        }

        @Override
        public long nanoseconds() {
            return System.nanoTime();
        }

        @Override
        public void sleep(long l) {
            try {
                Thread.sleep(l);
            } catch (InterruptedException e) {
                DTThrowable.rethrow(e);
            }
        }
    }
}
