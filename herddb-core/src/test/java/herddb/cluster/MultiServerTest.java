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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.AbstractTableManager;
import herddb.core.TableSpaceManager;
import herddb.index.SecondaryIndexSeek;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.GetResult;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.ZKTestEnv;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
public class MultiServerTest {

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
    public void test_leader_online_log_available() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("s", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            Index index = Index
                    .builder()
                    .onTable(table)
                    .type(Index.TYPE_BRIN)
                    .column("s", ColumnTypes.STRING)
                    .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "s", "1")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "s", "2")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "s", "3")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "s", "4")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            DMLStatementExecutionResult executeUpdateTransaction = server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 5, "s", "5")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            server_1.getManager().executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, executeUpdateTransaction.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // force BK LAC
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 6, "s", "6")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(5), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                    if (found.found()) {
                        break;
                    }
                    Thread.sleep(100);
                }
                for (int i = 1; i <= 5; i++) {
                    System.out.println("checking key c=" + i);
                    assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(i), null, false),
                            StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.NO_TRANSACTION).found());
                }

                TranslatedQuery translated = server_1.getManager().getPlanner().translate(TableSpace.DEFAULT,
                        "SELECT * FROM " + TableSpace.DEFAULT + ".t1 WHERE s=1",
                        Collections.emptyList(), true, true, false, -1);
                ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(statement.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan = server_1.getManager().scan(statement, translated.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(1, scan.consume().size());
                }

            }

        }
    }

    @Test
    public void testForLastConfirmedBackground() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        // send a dummy write after 1 seconds of inactivity
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 1000);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            DMLStatementExecutionResult executeUpdateTransaction = server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            server_1.getManager().executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, executeUpdateTransaction.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(5), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                    if (found.found()) {
                        break;
                    }
                    Thread.sleep(100);
                }
                for (int i = 1; i <= 5; i++) {
                    System.out.println("checking key c=" + i);
                    assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(i), null, false),
                            StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.NO_TRANSACTION).found());
                }

            }

        }
    }

    @Test
    public void test_leader_offline_log_available() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("s", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            Index index = Index
                    .builder()
                    .onTable(table)
                    .type(Index.TYPE_BRIN)
                    .column("s", ColumnTypes.STRING)
                    .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "s", "1")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "s", "2")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "s", "3")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "s", "4")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated = server_1.getManager().getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT * FROM " + TableSpace.DEFAULT + ".t1 WHERE s=1",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(statement.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan = server_1.getManager().scan(statement, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(1, scan.consume().size());
            }
        }

        try (Server server_2 = new Server(serverconfig_2)) {
            server_2.start();

            assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

            // wait for data to arrive on server_2
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                if (found.found()) {
                    break;
                }
                Thread.sleep(100);
            }
            assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());

            TranslatedQuery translated = server_2.getManager().getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT * FROM " + TableSpace.DEFAULT + ".t1 WHERE s=1",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(statement.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan = server_2.getManager().scan(statement, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(1, scan.consume().size());
            }
        }
    }

    @Test
    public void test_leader_online_log_no_more_available() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD, 1);
        serverconfig_1.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);
        Table table = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .column("s", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
        Index index = Index
                .builder()
                .onTable(table)
                .type(Index.TYPE_BRIN)
                .column("s", ColumnTypes.STRING)
                .build();
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "s", "1")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "s", "2")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "s", "3")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "s", "4")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated = server_1.getManager().getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT * FROM " + TableSpace.DEFAULT + ".t1 WHERE s=1",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(statement.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan = server_1.getManager().scan(statement, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(1, scan.consume().size());
            }
        }

        String tableSpaceUUID;
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                tableSpaceUUID = man.describeTableSpace(TableSpace.DEFAULT).uuid;
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
            }
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 5, "s", "5")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().checkpoint();
        }
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
            }
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 6, "s", "6")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
            }
            server_1.getManager().checkpoint();
        }
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
                assertTrue(!ledgersList.getActiveLedgers().contains(ledgersList.getFirstLedger()));
            }

            // data will be downloaded from the other server
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                    if (found.found()) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());


                TableSpaceManager tsm1 = server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT);
                TableSpaceManager tsm2 = server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT);

                Set<String> indexes1 = tsm1.getIndexesOnTable("t1").keySet();
                Set<String> indexes2 = tsm2.getIndexesOnTable("t1").keySet();
                assertEquals(indexes1, indexes2);

                TranslatedQuery translated = server_2.getManager().getPlanner().translate(TableSpace.DEFAULT,
                        "SELECT * FROM " + TableSpace.DEFAULT + ".t1 WHERE s=1", Collections.emptyList(),
                        true, true, false, -1);
                ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(statement.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan = server_2.getManager().scan(statement, translated.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(1, scan.consume().size());
                }
            }
        }

    }

    @Test
    public void test_follow_multiple_tablespaces() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);

        try (Server server_1 = new Server(serverconfig_1);
             Server server_2 = new Server(serverconfig_2)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            server_2.start();

            // two tablespaces, leader is server_1
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", new HashSet<>(Arrays.asList(server_1.getNodeId(), server_2.getNodeId())), server_1.getNodeId(), 1, 0, 0);
            server_1.getManager().executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            CreateTableSpaceStatement st2 = new CreateTableSpaceStatement("tblspace2", new HashSet<>(Arrays.asList(server_1.getNodeId(), server_2.getNodeId())), server_1.getNodeId(), 1, 0, 0);
            server_2.getManager().executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.waitForTableSpaceBoot("tblspace1", 30000, true);
            server_1.waitForTableSpaceBoot("tblspace2", 30000, true);

            server_2.waitForTableSpaceBoot("tblspace1", 30000, false);
            server_2.waitForTableSpaceBoot("tblspace2", 30000, false);

            Table table1 = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .tablespace("tblspace1")
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table1), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            Table table2 = Table.builder()
                    .name("t2")
                    .column("c", ColumnTypes.INTEGER)
                    .tablespace("tblspace2")
                    .primaryKey("c")
                    .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table2), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            DMLStatementExecutionResult executeUpdateTransaction = server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            server_1.getManager().executeStatement(new CommitTransactionStatement("tblspace1", executeUpdateTransaction.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            DMLStatementExecutionResult executeUpdateTransaction2 = server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            server_1.getManager().executeStatement(new CommitTransactionStatement("tblspace2", executeUpdateTransaction2.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // force BK LAC
            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // wait for data to arrive on server_2
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(new GetStatement("tblspace1", "t1", Bytes.from_int(5), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                if (found.found()) {
                    break;
                }
                Thread.sleep(100);
            }
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(new GetStatement("tblspace2", "t2", Bytes.from_int(5), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                if (found.found()) {
                    break;
                }
                Thread.sleep(100);
            }
            for (int i = 1; i <= 5; i++) {
                System.out.println("checking key c=" + i);
                assertTrue(server_2.getManager().get(new GetStatement("tblspace1", "t1", Bytes.from_int(i), null, false),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION).found());

                assertTrue(server_2.getManager().get(new GetStatement("tblspace2", "t2", Bytes.from_int(i), null, false),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION).found());
            }

        }
    }

    @Test
    public void test_follower_catchup_after_restart_after_longtime() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD, 1);
        serverconfig_1.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);
        Table table = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        }

        String tableSpaceUUID;
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                tableSpaceUUID = man.describeTableSpace(TableSpace.DEFAULT).uuid;
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
            }
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().checkpoint();
        }
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
            }
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                assertEquals(2, ledgersList.getActiveLedgers().size());
            }
            server_1.getManager().checkpoint();
        }
        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                System.out.println("current ledgersList: " + ledgersList);
                assertEquals(2, ledgersList.getActiveLedgers().size());
                assertTrue(!ledgersList.getActiveLedgers().contains(ledgersList.getFirstLedger()));
            }

            // data will be downloaded from the other server
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                    if (found.found()) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            }

            // write more data, server_2 is down, it will need to catch up
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 8)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 9)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 10)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // truncate Bookkeeper logs, we want the follower to download again the dump
            BookkeeperCommitLog bklog = (BookkeeperCommitLog) server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog();
            bklog.rollNewLedger();
            Thread.sleep(1000);
            server_1.getManager().checkpoint();

            {
                ZookeeperMetadataStorageManager man = (ZookeeperMetadataStorageManager) server_1.getMetadataStorageManager();
                LedgersInfo ledgersList = ZookeeperMetadataStorageManager.readActualLedgersListFromZookeeper(man.getZooKeeper(), testEnv.getPath() + "/ledgers", tableSpaceUUID);
                System.out.println("current ledgersList: " + ledgersList);
                assertEquals(1, ledgersList.getActiveLedgers().size());
                assertTrue(!ledgersList.getActiveLedgers().contains(ledgersList.getFirstLedger()));
            }

            Table table1 = server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableManager("t1").getTable();
            try (DataScanner scan = server_1.getManager()
                    .scan(new ScanStatement(TableSpace.DEFAULT, table1, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)) {
                List<DataAccessor> all = scan.consume();
                all.forEach(d -> {
                    System.out.println("RECORD ON SERVER 1: " + d.toMap());
                });
            }

            // data will be downloaded again, but it has to be merged with stale data
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));
                AbstractTableManager tableManager = server_2.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableManager("t1");
                Table table2 = tableManager.getTable();

                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {

                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(10), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                    if (found.found()) {
                        break;
                    }

                    try (DataScanner scan = server_2.getManager()
                            .scan(new ScanStatement(TableSpace.DEFAULT, table2, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)) {
                        List<DataAccessor> all = scan.consume();
                        System.out.println("WAIT #" + i);
                        all.forEach(d -> {
                            System.out.println("RECORD ON SERVER 2: " + d.toMap());
                        });
                    }

                    Thread.sleep(1000);
                }
                assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(10), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            }

        }

    }

    @Test
    public void testFollowAfterLedgerRollback() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        // send a dummy write after 1 seconds of inactivity
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 1000);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                TableSpaceManager tableSpaceManager = server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT);
                BookkeeperCommitLog logServer1 = (BookkeeperCommitLog) tableSpaceManager.getLog();
                logServer1.rollNewLedger();
                logServer1.rollNewLedger();
                logServer1.rollNewLedger();

                DMLStatementExecutionResult executeUpdateTransaction = server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
                server_1.getManager().executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, executeUpdateTransaction.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(5), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                    if (found.found()) {
                        break;
                    }
                    Thread.sleep(100);
                }
                for (int i = 1; i <= 5; i++) {
                    System.out.println("checking key c=" + i);
                    assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(i), null, false),
                            StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.NO_TRANSACTION).found());
                }
                BookkeeperCommitLog logServer2 = (BookkeeperCommitLog) tableSpaceManager.getLog();
                LogSequenceNumber lastSequenceNumberServer1 = logServer1.getLastSequenceNumber();
                LogSequenceNumber lastSequenceNumberServer2 = logServer2.getLastSequenceNumber();
                System.out.println("POS AT SERVER 1: " + lastSequenceNumberServer1);
                System.out.println("POS AT SERVER 2: " + lastSequenceNumberServer2);
                assertTrue(lastSequenceNumberServer1.equals(lastSequenceNumberServer2)
                        || lastSequenceNumberServer1.after(lastSequenceNumberServer2)); // maybe after due to NOOPs

            }

        }
    }

}
