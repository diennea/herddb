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

import herddb.client.ClientConfiguration;
import herddb.client.GetResult;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ScanResultSet;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.core.TestUtils;
import static herddb.core.TestUtils.scan;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import herddb.utils.ZKTestEnv;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookie();
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

        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                TestUtils.execute(server_1.getManager(), "CREATE TABLESPACE 'ttt','leader:" + server_2.getNodeId() + "','expectedreplicacount:2'", Collections.emptyList());

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
                try (HDBClient client1 = new HDBClient(clientConfiguration, logger);) {
                    try (HDBConnection connection = client1.openConnection();) {

                        // create table and insert data
                        connection.executeUpdate(TableSpace.DEFAULT, "CREATE TABLE ttt.t1(k1 int primary key, n1 int)", TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(1,1)", TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(2,1)", TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());
                        connection.executeUpdate(TableSpace.DEFAULT, "INSERT INTO ttt.t1(k1,n1) values(3,1)", TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList());

                        // use client2, so that it opens a connection to current leader
                        try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM ttt.t1", false, Collections.emptyList(), TransactionContext.NOTRANSACTION_ID, 0, 0);) {
                            assertEquals(3, scan.consume().size());
                        }

                        // change leader
                        switchLeader(server_1.getNodeId(), server_2.getNodeId(), server_1.getManager());

                        // perform operation
                        try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM ttt.t1", false, Collections.emptyList(), TransactionContext.NOTRANSACTION_ID, 0, 0);) {
                            assertEquals(3, scan.consume().size());
                        }
                        // check the client handled "not leader error"
                        assertEquals(1, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_2.getNodeId(), server_1.getNodeId(), server_1.getManager());

                        // perform operation
                        GetResult get = connection.executeGet(TableSpace.DEFAULT, "SELECT * FROM ttt.t1 where k1=1", TransactionContext.NOTRANSACTION_ID, false, Collections.emptyList());
                        assertTrue(get.isFound());

                        // check the client handled "not leader error"
                        assertEquals(2, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_1.getNodeId(), server_2.getNodeId(), server_1.getManager());

                        // perform operation
                        assertEquals(1,
                                connection.executeUpdate(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=3 where k1=1", TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList()).updateCount);

                        // check the client handled "not leader error"
                        assertEquals(3, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_2.getNodeId(), server_1.getNodeId(), server_1.getManager());

                        // perform operation
                        assertEquals(1,
                                connection.executeUpdateAsync(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=3 where k1=1", TransactionContext.NOTRANSACTION_ID, false, false, Collections.emptyList()).get().updateCount);

                        // check the client handled "not leader error"
                        assertEquals(4, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_1.getNodeId(), server_2.getNodeId(), server_1.getManager());

                        // perform operation
                        assertEquals(1,
                                connection.executeUpdates(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=3 where k1=1", TransactionContext.NOTRANSACTION_ID, false, false, Arrays.asList(Collections.emptyList())).get(0).updateCount);

                        // check the client handled "not leader error"
                        assertEquals(5, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

                        // change leader
                        switchLeader(server_2.getNodeId(), server_1.getNodeId(), server_1.getManager());

                        // perform operation
                        assertEquals(1,
                                connection.executeUpdatesAsync(TableSpace.DEFAULT, "UPDATE ttt.t1 set n1=3 where k1=1", TransactionContext.NOTRANSACTION_ID, false, false, Arrays.asList(Collections.emptyList())).get().get(0).updateCount);

                        // check the client handled "not leader error"
                        assertEquals(6, logger.scope("hdbclient").getCounter("leaderChangedErrors").get().intValue());

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
        waitClusterStatus(matadataServer, newFollower, "follower");
    }

    void waitClusterStatus(final DBManager matadataServer, String node, String expectedMode) throws DataScannerException, InterruptedException {
        for (int i = 0; i < 100; i++) {
            try (DataScanner scan = scan(matadataServer, "SELECT * FROM SYSTABLESPACEREPLICASTATE where tablespace_name='ttt' and nodeId='" + node + "'", Collections.emptyList());) {
                List<DataAccessor> tuples = scan.consume();
                assertEquals(1, tuples.size());
                if (tuples.get(0).get("mode").equals(expectedMode)) {
                    break;
                }
            }
            Thread.sleep(1000);
        }
    }
}
