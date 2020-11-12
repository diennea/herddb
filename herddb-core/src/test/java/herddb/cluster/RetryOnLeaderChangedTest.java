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

package herddb.cluster;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.ClientSideConnectionPeer;
import herddb.client.ClientSideMetadataProviderException;
import herddb.client.GetResult;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.client.ScanResultSet;
import herddb.core.ActivatorRunRequest;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.core.TestUtils;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.network.Channel;
import herddb.proto.Pdu;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.ServerSideConnectionPeer;
import herddb.utils.DataAccessor;
import herddb.utils.ZKTestEnv;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests about retrying queries in case of leader changes
 *
 * @author enrico.olivelli
 */
public class RetryOnLeaderChangedTest {

    private static final Logger LOG = Logger.getLogger(RetryOnLeaderChangedTest.class.getName());

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
        // as expectedreplicacount is 2 we need at least two bookies
        testEnv.startNewBookie();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void test() throws Exception {

        TestStatsProvider statsProvider = new TestStatsProvider();

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                TestUtils.execute(server_1.getManager(),
                        "CREATE TABLESPACE 'ttt','leader:" + server_2.getNodeId() + "','expectedreplicacount:2'",
                        Collections.emptyList());

                // wait for server_2 to wake up
                for (int i = 0; i < 40; i++) {
                    TableSpaceManager tableSpaceManager2 = server_2.getManager().getTableSpaceManager("ttt");
                    if (tableSpaceManager2 != null
                            && tableSpaceManager2.isLeader()) {
                        break;
                    }
                    Thread.sleep(500);
                }
                assertTrue(server_2.getManager().getTableSpaceManager("ttt") != null
                        && server_2.getManager().getTableSpaceManager("ttt").isLeader());

                // wait for server_1 to announce as follower
                waitClusterStatus(server_1.getManager(), server_1.getNodeId(), "follower");

                ClientConfiguration clientConfiguration = new ClientConfiguration();
                clientConfiguration.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
                clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
                clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
                clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

                StatsLogger logger = statsProvider.getStatsLogger("ds");
                try (HDBClient client1 = new HDBClient(clientConfiguration, logger)) {
                    try (HDBConnection connection = client1.openConnection()) {

                        // create table and insert data
                        connection.executeUpdate(TableSpace.DEFAULT, "CREATE TABLE ttt.t1(k1 int primary key, n1 int)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(1,1)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(2,1)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(3,1)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());

                        // use client2, so that it opens a connection to current leader
                        try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM ttt.t1",
                                false, Collections.emptyList(), TransactionContext.NOTRANSACTION_ID, 0, 0, true)) {
                            assertEquals(3, scan.consume().size());
                        }

                        // change leader
                        switchLeader(server_1.getNodeId(), server_2.getNodeId(), server_1.getManager());

                        // perform operation
                        try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM ttt.t1",
                                false, Collections.emptyList(), TransactionContext.NOTRANSACTION_ID, 0, 0, true)) {
                            assertEquals(3, scan.consume().size());
                        }
                        // check the client handled "not leader error"
                        assertEquals(1, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_2.getNodeId(), server_1.getNodeId(), server_1.getManager());

                        // perform operation
                        GetResult get = connection.executeGet(TableSpace.DEFAULT, "SELECT * FROM ttt.t1 where k1=1",
                                TransactionContext.NOTRANSACTION_ID, false, Collections.emptyList());
                        assertTrue(get.isFound());

                        // check the client handled "not leader error"
                        assertEquals(2, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_1.getNodeId(), server_2.getNodeId(), server_1.getManager());

                        // perform operation
                        assertEquals(1,
                                connection.executeUpdate(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=3 where k1=1",
                                        TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList()).updateCount);

                        // check the client handled "not leader error"
                        assertEquals(3, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_2.getNodeId(), server_1.getNodeId(), server_1.getManager());

                        // perform operation
                        assertEquals(1,
                                connection.executeUpdateAsync(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=3 where k1=1",
                                        TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList()).
                                        get().updateCount);

                        // check the client handled "not leader error"
                        assertEquals(4, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_1.getNodeId(), server_2.getNodeId(), server_1.getManager());

                        // perform operation
                        assertEquals(1,
                                connection.executeUpdates(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=3 where k1=1",
                                        TransactionContext.NOTRANSACTION_ID, false, false, Arrays.asList(Collections.
                                                emptyList())).get(0).updateCount);

                        // check the client handled "not leader error"
                        assertEquals(5, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_2.getNodeId(), server_1.getNodeId(), server_1.getManager());

                        // perform operation
                        assertEquals(1,
                                connection.executeUpdatesAsync(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=3 where k1=1",
                                        TransactionContext.NOTRANSACTION_ID, false, false, Arrays.asList(Collections.
                                                emptyList())).get().get(0).updateCount);

                        // check the client handled "not leader error"
                        assertEquals(6, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                    }

                }

            }

        }
    }

    @Test
    public void testKillLeaderDuringScan() throws Exception {
        testKillLeader((connection) -> {
            try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                    "SELECT * FROM ttt.t1", false, Collections.emptyList(),
                    TransactionContext.NOTRANSACTION_ID, 0, 0, true)) {
                assertEquals(3, scan.consume().size());
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        });
    }

    @Test
    public void testKillLeaderDuringUpdate() throws Exception {
        testKillLeader((connection) -> {

            try {
                connection.executeUpdate(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=2",
                        TransactionContext.NOTRANSACTION_ID, false, true, Collections.emptyList());
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        });
    }

    @Test
    public void testKillLeaderDuringUpdates() throws Exception {
        testKillLeader((connection) -> {

            try {
                connection.executeUpdates(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=2",
                        TransactionContext.NOTRANSACTION_ID, false, true, Arrays.asList(Collections.emptyList())).get(0);
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        });
    }

    @Test
    public void testKillLeaderDuringAsyncUpdate() throws Exception {
        testKillLeader((connection) -> {

            try {
                connection.executeUpdateAsync(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=2",
                        TransactionContext.NOTRANSACTION_ID, false, true, Collections.emptyList()).get();
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        });
    }

    @Test
    public void testKillLeaderDuringAsyncUpdates() throws Exception {
        testKillLeader((connection) -> {

            try {
                connection.executeUpdatesAsync(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=2",
                        TransactionContext.NOTRANSACTION_ID, false, true, Arrays.asList(Collections.emptyList()))
                        .get().get(0);
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        });
    }

    private void testKillLeader(Consumer<HDBConnection> operation) throws Exception {

        TestStatsProvider statsProvider = new TestStatsProvider();

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                TestUtils.execute(server_1.getManager(),
                        "CREATE TABLESPACE 'ttt','leader:" + server_2.getNodeId() + "','expectedreplicacount:2'",
                        Collections.emptyList());

                // wait for server_2 to wake up
                for (int i = 0; i < 40; i++) {
                    TableSpaceManager tableSpaceManager2 = server_2.getManager().getTableSpaceManager("ttt");
                    if (tableSpaceManager2 != null
                            && tableSpaceManager2.isLeader()) {
                        break;
                    }
                    Thread.sleep(500);
                }
                assertTrue(server_2.getManager().getTableSpaceManager("ttt") != null
                        && server_2.getManager().getTableSpaceManager("ttt").isLeader());

                // wait for server_1 to announce as follower
                waitClusterStatus(server_1.getManager(), server_1.getNodeId(), "follower");

                ClientConfiguration clientConfiguration = new ClientConfiguration();
                clientConfiguration.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
                clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
                clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
                clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
                clientConfiguration.set(ClientConfiguration.PROPERTY_MAX_OPERATION_RETRY_COUNT, 1000);

                StatsLogger logger = statsProvider.getStatsLogger("ds");
                try (HDBClient client1 = new HDBClient(clientConfiguration, logger)) {
                    try (HDBConnection connection = client1.openConnection()) {

                        // create table and insert data
                        connection.executeUpdate(TableSpace.DEFAULT, "CREATE TABLE ttt.t1(k1 int primary key, n1 int)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(1,1)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(2,1)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(3,1)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());

                        // use client2, so that it opens a connection to current leader
                        try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM ttt.t1",
                                false, Collections.emptyList(), TransactionContext.NOTRANSACTION_ID, 0, 0, true)) {
                            assertEquals(3, scan.consume().size());
                        }

                        // change leader
                        switchLeader(server_1.getNodeId(), null, server_1.getManager());

                        // make server_2 (current leader) die
                        server_2.close();

                        // perform operation, it will eventually succeed, because
                        // the client will automatically wait for the new leader to be up
                        operation.accept(connection);

                    }

                }

            }

        }
    }

    @Test
    public void testSwitchLeaderAndAuthTimeout() throws Exception {

        TestStatsProvider statsProvider = new TestStatsProvider();

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        final AtomicBoolean suspendProcessing = new AtomicBoolean(false);
        try (Server server_1 = new Server(serverconfig_1)) {

            server_1.start();
            server_1.waitForStandaloneBoot();

            try (Server server_2 = new Server(serverconfig_2) {
                @Override
                protected ServerSideConnectionPeer buildPeer(Channel channel) {
                    return new ServerSideConnectionPeer(channel, this) {

                        @Override
                        public void requestReceived(Pdu message, Channel channel) {
                            if (suspendProcessing.get()) {
                                LOG.log(Level.INFO, "dropping message type " + message.type + " id " + message.messageId);
                                message.close();
                                return;
                            }
                            super.requestReceived(message, channel);
                        }

                    };
                }
            }) {

                server_2.start();

                TestUtils.execute(server_1.getManager(),
                        "CREATE TABLESPACE 'ttt','leader:" + server_2.getNodeId() + "','expectedreplicacount:2'",
                        Collections.emptyList());

                // wait for server_2 to wake up
                for (int i = 0; i < 40; i++) {
                    TableSpaceManager tableSpaceManager2 = server_2.getManager().getTableSpaceManager("ttt");
                    if (tableSpaceManager2 != null
                            && tableSpaceManager2.isLeader()) {
                        break;
                    }
                    Thread.sleep(500);
                }
                assertTrue(server_2.getManager().getTableSpaceManager("ttt") != null
                        && server_2.getManager().getTableSpaceManager("ttt").isLeader());

                // wait for server_1 to announce as follower
                waitClusterStatus(server_1.getManager(), server_1.getNodeId(), "follower");

                ClientConfiguration clientConfiguration = new ClientConfiguration();
                clientConfiguration.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_CLUSTER);
                clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
                clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
                clientConfiguration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
                clientConfiguration.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 2);
                clientConfiguration.set(ClientConfiguration.PROPERTY_TIMEOUT, 2000);

                StatsLogger logger = statsProvider.getStatsLogger("ds");
                try (HDBClient client1 = new HDBClient(clientConfiguration, logger) {
                    @Override
                    public HDBConnection openConnection() {
                        HDBConnection con = new VisibleRouteHDBConnection(this);
                        registerConnection(con);
                        return con;
                    }

                }) {
                    try (VisibleRouteHDBConnection connection = (VisibleRouteHDBConnection) client1.openConnection()) {

                        // create table and insert data
                        connection.executeUpdate(TableSpace.DEFAULT, "CREATE TABLE ttt.t1(k1 int primary key, n1 int)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(1,1)",
                                TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());

                        assertEquals("server2", connection.getRouteToTableSpace("ttt").getNodeId());

                        // change leader
                        switchLeader(server_1.getNodeId(), server_2.getNodeId(), server_1.getManager());


                        try (VisibleRouteHDBConnection connection2 = (VisibleRouteHDBConnection) client1.openConnection()) {

                            // connection routing still point to old leader (now follower)
                            assertEquals("server2", connection2.getRouteToTableSpace("ttt").getNodeId());

                            // suspend server_2 authentication
                            suspendProcessing.set(true);

                            // attempt an insert with old routing. Suspended autentication generates a timeout
                            // and routing will be reevaluated
                            assertEquals(1, connection2.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(2,2)",
                                    TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList()).updateCount);

                            // right routing to current master
                            assertEquals("server1", connection2.getRouteToTableSpace("ttt").getNodeId());

                            suspendProcessing.set(false);

                        }
                    }
                }
            }
        }
    }

    void switchLeader(String newLeader, String newFollower, DBManager matadataServer) throws DataScannerException, InterruptedException {
        // change leader
        TestUtils.execute(matadataServer, "ALTER TABLESPACE 'ttt','leader:" + newLeader + "'", Collections.emptyList());

        // wait for server to announce as leader
        waitClusterStatus(matadataServer, newLeader, "leader");
        if (newFollower != null) {
            // wait for server to announce as follower
            waitClusterStatus(matadataServer, newFollower, "follower");
        }

    }

    void waitClusterStatus(final DBManager matadataServer, String node, String expectedMode) throws DataScannerException, InterruptedException {
        for (int i = 0; i < 100; i++) {
            try (DataScanner scan = scan(matadataServer,
                    "SELECT * FROM SYSTABLESPACEREPLICASTATE where tablespace_name='ttt' and nodeId='" + node + "'",
                    Collections.emptyList())) {
                List<DataAccessor> tuples = scan.consume();
                assertEquals(1, tuples.size());
                if (tuples.get(0).get("mode").equals(expectedMode)) {
                    break;
                }
            }
            Thread.sleep(1000);
        }
    }


    public static final class VisibleRouteHDBConnection extends HDBConnection {

        public VisibleRouteHDBConnection(HDBClient client) {
            super(client);
        }

        @Override
        public ClientSideConnectionPeer getRouteToTableSpace(String tableSpace)
                throws ClientSideMetadataProviderException, HDBException {
            return super.getRouteToTableSpace(tableSpace);
        }

    }

     @Test
    public void testExpectedReplicaCount() throws Exception {

        TestStatsProvider statsProvider = new TestStatsProvider();

        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

         ServerConfiguration serverconfig_3 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server3")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1);
                Server server_2 = new Server(serverconfig_2);
                Server server_3 = new Server(serverconfig_3)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            server_2.start();
            server_3.start();

            // wait for all of the three nodes to announce
            herddb.utils.TestUtils.waitForCondition(() -> {
                List<NodeMetadata> listNodes = server_3.getMetadataStorageManager().listNodes();
                System.out.println("NODES: " + listNodes);
                return listNodes.size() == 3;
            }, herddb.utils.TestUtils.NOOP, 100);

            // create the tablespace
            TestUtils.execute(server_1.getManager(),
                        "CREATE TABLESPACE 'ttt','leader:" + server_1.getNodeId() + "','expectedreplicacount:2'",
                        Collections.emptyList());

            server_2.getManager().triggerActivator(ActivatorRunRequest.FULL);

            // wait for the cluster to settle to 2 replicas
            herddb.utils.TestUtils.waitForCondition(() -> {
                TableSpace ts = server_3.getMetadataStorageManager().describeTableSpace("ttt");
                System.out.println("TS: " + ts);
                assertTrue(ts.replicas.size() <= 2);
                return ts.replicas.size() == 2;
            }, herddb.utils.TestUtils.NOOP, 100);

        }
    }
}
