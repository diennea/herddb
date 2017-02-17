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
package herddb.server;

import herddb.core.TableSpaceManager;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.core.TestUtils;
import herddb.model.DataScanner;
import herddb.model.Tuple;
import herddb.utils.ZKTestEnv;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Booting two servers, one table space
 *
 * @author enrico.olivelli
 */
public class MaxLeaderInactivityTest {

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
    public void test_auto_heal1() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

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

                TestUtils.execute(server_1.getManager(), "ALTER TABLESPACE 'ttt','maxLeaderInactivityTime:5000'", Collections.emptyList());

                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM SYSTABLESPACEREPLICASTATE where tablespace_name='ttt' and nodeId='" + server_2.getNodeId() + "'", Collections.emptyList());) {
                    List<Tuple> tuples = scan.consume();
//                    for (Tuple t : tuples) {
//                        System.out.println("tuple:" + t);
//                    }
                    assertEquals(1, tuples.size());

                }

                // wait for server_1 to announce as follower
                for (int i = 0; i < 100; i++) {
                    try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM SYSTABLESPACEREPLICASTATE where tablespace_name='ttt' and nodeId='" + server_1.getNodeId() + "'", Collections.emptyList());) {
                        List<Tuple> tuples = scan.consume();
//                        for (Tuple t : tuples) {
//                            System.out.println("tuple:" + t);
//                        }
                        assertEquals(1, tuples.size());
                        if (tuples.get(0).get("mode").equals("follower")) {
                            break;
                        }
                    }
                    Thread.sleep(1000);
                }

                // server_2 dies
            }

            // wait for server_1 to announce as leader
            for (int i = 0; i < 100; i++) {
                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM SYSTABLESPACEREPLICASTATE "
                    + "where tablespace_name='ttt' and nodeId='" + server_1.getNodeId() + "'", Collections.emptyList());) {
                    List<Tuple> tuples = scan.consume();
                    for (Tuple t : tuples) {
                        System.out.println("tuple2:" + t);
                    }
                    assertEquals(1, tuples.size());
                    if (tuples.get(0).get("mode").equals("leader")) {
                        break;
                    }
                }
                Thread.sleep(1000);
            }

            assertTrue(server_1.getManager().getTableSpaceManager("ttt").isLeader());

        }
    }

    @Test
    public void test_auto_no_heal() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

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

                TestUtils.execute(server_1.getManager(), "CREATE TABLESPACE 'ttt','leader:" + server_2.getNodeId() + "','expectedreplicacount:2','wait:60000'", Collections.emptyList());

                // wait for server_2 to wake up
                for (int i = 0; i < 100; i++) {
                    if (server_2.getManager().getTableSpaceManager("ttt") != null && server_2.getManager().getTableSpaceManager("ttt").isLeader()) {
                        break;
                    }
                    Thread.sleep(500);
                }
                assertTrue(server_2.getManager().getTableSpaceManager("ttt") != null && server_2.getManager().getTableSpaceManager("ttt").isLeader());

                TestUtils.execute(server_1.getManager(), "ALTER TABLESPACE 'ttt','maxLeaderInactivityTime:10000'", Collections.emptyList());

                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM SYSTABLESPACEREPLICASTATE where "
                    + "tablespace_name='ttt' and nodeId='" + server_2.getNodeId() + "'", Collections.emptyList());) {
                    List<Tuple> tuples = scan.consume();
//                    for (Tuple t : tuples) {
//                        System.out.println("tuple:" + t);
//                    }
                    assertEquals(1, tuples.size());

                }

                // we want server 2 to be leader forever
                for (int i = 0; i < 20; i++) {
                    try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM SYSTABLESPACEREPLICASTATE where tablespace_name='ttt'", Collections.emptyList());) {
                        List<Tuple> tuples = scan.consume();
//                        for (Tuple t : tuples) {
//                            System.out.println("tuple:" + t);
//                        }
                        assertEquals(2, tuples.size());
                    }
                    TableSpaceManager tableSpaceManager_1 = server_1.getManager().getTableSpaceManager("ttt");
                    if (tableSpaceManager_1 != null) {
                        assertTrue(!tableSpaceManager_1.isLeader());
                    }
                    TableSpaceManager tableSpaceManager_2 = server_2.getManager().getTableSpaceManager("ttt");
                    assertNotNull(tableSpaceManager_2);
                    assertTrue(tableSpaceManager_2.isLeader());

                    Thread.sleep(1000);
                }

            }

        }
    }

}
