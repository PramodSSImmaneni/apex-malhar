package com.datatorrent.contrib.kafka;

import com.datatorrent.netlet.util.DTThrowable;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;

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

    private KafkaServer[] kafkaServers;

    @ClassRule
    public static TestWatcher className = new TestWatcher() {
        @Override
        protected void starting(Description d) {
            System.out.println("classname " + d.getClassName());
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

    @Rule
    public TestName name = new TestName() {
        @Override
        protected void starting(Description d) {
            workDir = new File(testDir, d.getMethodName());
            System.out.println("methodname " + d.getMethodName());
        }

        @Override
        protected void finished(Description description) {
            FileUtils.deleteQuietly(workDir);
        }
    };

    @Rule
    public ExternalResource externalResource = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            System.out.println(3);
            /*
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
        protected void after() {
            /*
            for (KafkaServer kafkaServer : kafkaServers) {
                kafkaServer.shutdown();
            }
            cnxnFactory.shutdown();
            */
        }
    };

    @Test
    public void testComponent() throws InterruptedException {
        System.out.println("a");
        //KafkaServer kafkaServer = new KafkaServer();
        Thread.sleep(2000);
    }

    @Test
    public void testComponent2() throws InterruptedException {
        System.out.println("b");
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
