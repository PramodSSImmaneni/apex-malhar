package com.datatorrent.contrib.kafka;

import com.datatorrent.netlet.util.DTThrowable;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by pramod on 11/5/15.
 */
public class ZooKeeperOffsetManagerTest {

    private int zkPort = 2181;
    private int numBrokers = 5;
    private int startPort = 20000;

    private File testDir;
    private File workDir;

    private ServerCnxnFactory cnxnFactory;
    private ZooKeeperServer zooKeeperServer;

    private KafkaServer[] kafkaServers;

    @Rule
    public TestName name = new TestName() {
        @Override
        protected void starting(Description d) {
            testDir = new File("target", d.getClassName());
            testDir.mkdir();
        }

        @Override
        protected void finished(Description description) {
            try {
                FileUtils.deleteDirectory(testDir);
            } catch (IOException e) {
                DTThrowable.rethrow(e);
            }
        }
    };

    @Before
    public void setupEnv() throws IOException, InterruptedException {
        workDir = new File(testDir, name.getMethodName());

        cnxnFactory = NIOServerCnxnFactory.createFactory(zkPort, 6);
        File zkSnapshotDir = new File(workDir, "zksnaps");
        File zkLogDir = new File(workDir, "zklogs");
        zooKeeperServer = new ZooKeeperServer(zkSnapshotDir, zkLogDir, 500);
        cnxnFactory.startup(zooKeeperServer);

        String zooKeeperAddress = "localhost:" + zkPort;

        kafkaServers = new KafkaServer[numBrokers];
        for (int i = 0; i < numBrokers; ++i) {
            File kafkaLogDir = new File(workDir, "kafkalogs-" + i);
            Properties properties = new Properties();
            properties.setProperty("broker.id", "broker-" + i);
            properties.setProperty("host.name", "localhost");
            properties.setProperty("port", "" + (startPort + i));
            properties.setProperty("zookeeper.connect", zooKeeperAddress);
            properties.setProperty("log.dir", kafkaLogDir.getAbsolutePath());
            KafkaConfig kafkaConfig = new KafkaConfig(properties);
            KafkaServer kafkaServer = new KafkaServer(kafkaConfig, new KafkaTime());
            kafkaServers[i] = kafkaServer;
            kafkaServer.startup();
        }
    }

    @After
    public void takedownEnv() throws IOException {
        for (KafkaServer kafkaServer : kafkaServers) {
            kafkaServer.shutdown();
        }
        cnxnFactory.shutdown();
        FileUtils.deleteDirectory(workDir);
    }

    @Test
    public void testComponent() throws InterruptedException {
        //KafkaServer kafkaServer = new KafkaServer();
        Thread.sleep(2000);
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
