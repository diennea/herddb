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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManagerException;
import herddb.utils.DataAccessor;
import herddb.utils.TestUtils;
import herddb.utils.ZKTestEnv;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Booting two servers, one table space
 *
 * @author enrico.olivelli
 */
public class TablespaceReplicasStateTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void testCluster() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate", Collections.emptyList())) {
                    List<DataAccessor> results = scan.consume();
                    assertEquals(2, results.size());
                }

                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate "
                        + "where nodeId='" + server_2.getNodeId() + "' and mode='follower'", Collections.emptyList())) {
                    assertEquals(1, scan.consume().size());
                }

                try (DataScanner scan = scan(server_2.getManager(), "SELECT * FROM systablespacereplicastate "
                        + "where nodeId='" + server_2.getNodeId() + "' and mode='follower'", Collections.emptyList())) {
                    assertEquals(1, scan.consume().size());
                }

                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate "
                        + "where nodeId='" + server_1.getNodeId() + "' and mode='leader'", Collections.emptyList())) {
                    assertEquals(1, scan.consume().size());
                }

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                for (int i = 0; i < 100; i++) {
                    try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate where nodeId='" + server_2.getNodeId() + "' and mode='stopped'", Collections.emptyList())) {
                        if (scan.consume().size() > 0) {
                            break;
                        }
                    }
                }
                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate where nodeId='" + server_2.getNodeId() + "' and mode='stopped'", Collections.emptyList())) {
                    assertEquals(1, scan.consume().size());
                }
            }
        }
    }


    @Test
    public void testDisklessCluster() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate", Collections.emptyList())) {
                    List<DataAccessor> results = scan.consume();
                    assertEquals(2, results.size());
                }

                // test that server2 even if it is a follower it starts the TableSpaceManager
                // and it writing its state to metadata
                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate "
                        + "where nodeId='" + server_2.getNodeId() + "' and mode='follower'", Collections.emptyList())) {
                    assertEquals(1, scan.consume().size());
                }

                try (DataScanner scan = scan(server_2.getManager(), "SELECT * FROM systablespacereplicastate "
                        + "where nodeId='" + server_2.getNodeId() + "' and mode='follower'", Collections.emptyList())) {
                    assertEquals(1, scan.consume().size());
                }

                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate "
                        + "where nodeId='" + server_1.getNodeId() + "' and mode='leader'", Collections.emptyList())) {
                    assertEquals(1, scan.consume().size());
                }

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                for (int i = 0; i < 100; i++) {
                    try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate where nodeId='" + server_2.getNodeId() + "' and mode='stopped'", Collections.emptyList())) {
                        if (scan.consume().size() > 0) {
                            break;
                        }
                    }
                }
                try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate where nodeId='" + server_2.getNodeId() + "' and mode='stopped'", Collections.emptyList())) {
                    assertEquals(1, scan.consume().size());
                }

                // add again server2 as follower
                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                for (int i = 0; i < 100; i++) {
                    try (DataScanner scan = scan(server_1.getManager(), "SELECT * FROM systablespacereplicastate where nodeId='" + server_2.getNodeId() + "' and mode='follower'", Collections.emptyList())) {
                        if (scan.consume().size() > 0) {
                            break;
                        }
                    }
                }

                // make server2 leader
                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server2", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);


                server_2.waitForTableSpaceBoot(TableSpace.DEFAULT, true);
                server_1.waitForTableSpaceBoot(TableSpace.DEFAULT, false);

                for (int i = 0; i < 100; i++) {
                    try (DataScanner scan = scan(server_2.getManager(), "SELECT * FROM systablespacereplicastate where nodeId='" + server_2.getNodeId() + "' and mode='leader'", Collections.emptyList())) {
                        if (scan.consume().size() > 0) {
                            break;
                        }
                    }
                }


                for (int i = 0; i < 100; i++) {
                    try (DataScanner scan = scan(server_2.getManager(), "SELECT * FROM systablespacereplicastate where nodeId='" + server_1.getNodeId() + "' and mode='follower'", Collections.emptyList())) {
                        if (scan.consume().size() > 0) {
                            break;
                        }
                    }
                }

                // the TableSpaceManager for a follower must not be able to perform a checkpoint
                DataStorageManagerException err = TestUtils.expectThrows(DataStorageManagerException.class,
                        () -> {
                            server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).checkpoint(true, false, false);
                        });
                // ZooKeeper BadVersionException is expected because
                // server1 is not holding the expected version of znode metdata
                assertThat(err.getCause(), instanceOf(KeeperException.BadVersionException.class));

            }
        }
    }
}
