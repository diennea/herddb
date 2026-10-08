/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.utils.ZKTestEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Every ZooKeeper handle owns a dedicated send thread, so counting those threads tells how many
 * ZooKeeper sessions are alive. These tests measure the difference across the operation under test
 * instead of an absolute count, because other components of the JVM may hold sessions of their own.
 */
public class ZooKeeperSessionLeakTest {

    /**
     * Name fragment of the thread ZooKeeper dedicates to a single client session.
     */
    private static final String ZOOKEEPER_SEND_THREAD_MARKER = "-SendThread(";

    /**
     * Upper bound for the wait on an asynchronous session shutdown.
     */
    private static final long SESSION_SHUTDOWN_TIMEOUT_MS = 30000L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void closingTheProviderReleasesEveryZooKeeperSession() throws Exception {
        int sessionsBefore = stableZooKeeperSessions();

        ZookeeperClientSideMetadataProvider provider = new ZookeeperClientSideMetadataProvider(
                testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());
        try {
            assertNotNull(provider.getZooKeeper());
            assertTrue(countZooKeeperSessions() > sessionsBefore);
        } finally {
            provider.close();
        }

        awaitZooKeeperSessions(sessionsBefore, "the provider left a ZooKeeper session behind");
    }

    @Test
    public void closingAClusterClientReleasesEveryZooKeeperSession() throws Exception {
        int sessionsBefore = stableZooKeeperSessions();

        HDBClient client = new HDBClient(newClusterConfiguration());
        try {
            assertTrue(client.getClientSideMetadataProvider() instanceof ZookeeperClientSideMetadataProvider);
            assertTrue(countZooKeeperSessions() > sessionsBefore);
        } finally {
            client.close();
        }

        awaitZooKeeperSessions(sessionsBefore, "the client left a ZooKeeper session behind");
    }

    @Test
    public void closingAClientReleasesTheProviderInstalledThroughTheSetter() throws Exception {
        int sessionsBefore = stableZooKeeperSessions();

        HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
        try {
            client.setClientSideMetadataProvider(new ZookeeperClientSideMetadataProvider(
                    testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath()));
            assertTrue(countZooKeeperSessions() > sessionsBefore);
        } finally {
            client.close();
        }

        awaitZooKeeperSessions(sessionsBefore, "the client left a ZooKeeper session behind");
    }

    @Test
    public void aClientWhoseProviderHasBeenReplacedClosesCleanly() throws Exception {
        int sessionsBefore = stableZooKeeperSessions();

        ClientSideMetadataProvider replaced;
        HDBClient client = new HDBClient(newClusterConfiguration());
        try {
            replaced = client.getClientSideMetadataProvider();
            assertTrue(replaced instanceof ZookeeperClientSideMetadataProvider);
            client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider("localhost", 1234, false));
        } finally {
            client.close();
        }

        // the client closes the provider it holds at close time, the replaced one belongs to the caller
        replaced.close();

        awaitZooKeeperSessions(sessionsBefore, "a replaced provider left a ZooKeeper session behind");
    }

    private ClientConfiguration newClusterConfiguration() throws Exception {
        ClientConfiguration configuration = new ClientConfiguration(folder.newFolder().toPath());
        configuration.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
        configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        return configuration;
    }

    private static int countZooKeeperSessions() {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && thread.getName().contains(ZOOKEEPER_SEND_THREAD_MARKER)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Samples the number of live sessions until two consecutive samples agree, so that a session
     * which is still shutting down is not counted as part of the baseline.
     */
    private static int stableZooKeeperSessions() throws InterruptedException {
        int previous = countZooKeeperSessions();
        for (int i = 0; i < 40; i++) {
            Thread.sleep(250);
            int current = countZooKeeperSessions();
            if (current == previous) {
                return current;
            }
            previous = current;
        }
        return previous;
    }

    private static void awaitZooKeeperSessions(int expected, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + SESSION_SHUTDOWN_TIMEOUT_MS;
        int current = countZooKeeperSessions();
        while (current > expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            current = countZooKeeperSessions();
        }
        assertTrue(message + ", expected at most " + expected + " live ZooKeeper sessions, found " + current, current <= expected);
    }

}
