/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.cluster.follower;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.AbstractTableManager;
import herddb.core.ActivatorRunRequest;
import herddb.core.TableSpaceManager;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.GetResult;
import herddb.model.Index;
import herddb.model.Record;
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
import herddb.model.commands.UpdateStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.storage.FullTableScanConsumer;
import herddb.utils.BooleanHolder;
import herddb.utils.Bytes;
import herddb.utils.TestUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SimpleFollowerTest extends MultiServerBase {

    @Test
    public void testFollowMultipleTablespaces() throws Exception {
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

            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            DMLStatementExecutionResult executeUpdateTransaction = server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 5)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            server_1.getManager().executeStatement(new CommitTransactionStatement("tblspace1", executeUpdateTransaction.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            DMLStatementExecutionResult executeUpdateTransaction2 = server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 5)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            server_1.getManager().executeStatement(new CommitTransactionStatement("tblspace2", executeUpdateTransaction2.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            // force BK LAC
            server_1.getManager().executeUpdate(new InsertStatement("tblspace1", "t1", RecordSerializer.makeRecord(table1, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement("tblspace2", "t2", RecordSerializer.makeRecord(table2, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            // wait for data to arrive on server_2
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(new GetStatement("tblspace1", "t1", Bytes.from_int(5), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
                if (found.found()) {
                    break;
                }
                Thread.sleep(100);
            }
            for (int i = 0; i < 100; i++) {
                GetResult found = server_2.getManager().get(new GetStatement("tblspace2", "t2", Bytes.from_int(5), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
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
    public void testLeaderOnlineLogAvailableMultipleVersionsActivePages() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 1000);
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD, 1); // delete ledgers soon
        serverconfig_1.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0);
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, 0); // disabled
        /*
         * Disable page compaction (avoid compaction of dirty page)
         */
        serverconfig_1.set(ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD, 0.0D);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT, true)
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868);
        Table table = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .column("s", ColumnTypes.STRING)
                .primaryKey("c")
                .build();

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            TableSpaceManager tableSpaceManager = server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            AbstractTableManager tableManager = tableSpaceManager.getTableManager("t1");
            // fill table
            long tx = herddb.core.TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);
            for (int i = 0; i < 1000; i++) {
                server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", i, "s", "1")), StatementEvaluationContext.
                        DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            }
            herddb.core.TestUtils.commitTransaction(server_1.getManager(), TableSpace.DEFAULT, tx);

            // we want to be in the case that the same key is present on more than one active datapage
            // when we send the dump to the follower we must send only the latest version of the record
            for (int i = 0; i < 10; i++) {
                tableManager.flush();
                server_1.getManager().executeUpdate(new UpdateStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "s", "2" + i), null), StatementEvaluationContext.
                        DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            }

            BooleanHolder foundDuplicate = new BooleanHolder(false);
            server_1.getManager().getDataStorageManager().fullTableScan(tableSpaceManager.getTableSpaceUUID(), tableManager.getTable().uuid, new FullTableScanConsumer() {

                Map<Bytes, Long> recordPage = new HashMap<>();

                @Override
                public void acceptPage(long pageId, List<Record> records) {
                    for (Record record : records) {
                        Long prev = recordPage.put(record.key, pageId);
                        if (prev != null) {
                            foundDuplicate.value = true;
                        }
                    }
                }

            });
            assertTrue(foundDuplicate.value);

            // data will be downloaded from the server_1 (PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT)
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertTrue(server_2.getManager().isTableSpaceLocallyRecoverable(server_1.getMetadataStorageManager().describeTableSpace(TableSpace.DEFAULT)));

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));
            }
        }
    }

    @Test
    public void testCheckpointFollower() throws Exception {
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

            int size = 1000;
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                LogSequenceNumber lastSequenceNumberServer1 = server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog().getLastSequenceNumber();

                for (int i = 0; i < size; i++) {
                    server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", i, "s", "1" + i)), StatementEvaluationContext.
                            DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                    if (i % 30 == 0) {
                        server_2.getManager().checkpoint();
                    }
                }

                LogSequenceNumber lastSequenceNumberServer2 = server_2.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog().getLastSequenceNumber();
                while (!lastSequenceNumberServer2.after(lastSequenceNumberServer1)) {
                    System.out.println("WAITING FOR server2 to be in sync....now it is a " + lastSequenceNumberServer2 + " vs " + lastSequenceNumberServer1);
                    lastSequenceNumberServer2 = server_2.getManager().getTableSpaceManager(TableSpace.DEFAULT).getLog().getLastSequenceNumber();
                    Thread.sleep(1000);
                }
            }

            // reboot follower˙
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();
                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                for (int i = 0; i < size; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(i), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.NO_TRANSACTION);
                    if (found.found()) {
                        break;
                    }
                    Thread.sleep(100);
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
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            DMLStatementExecutionResult executeUpdateTransaction = server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 5)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            server_1.getManager().executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, executeUpdateTransaction.transactionId), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(5), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.NO_TRANSACTION);
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
    public void followerMustNoRollbackAbandonedTransactions() throws Exception {
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
                .set(ServerConfiguration.PROPERTY_ABANDONED_TRANSACTIONS_TIMEOUT, 1)
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
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                    new HashSet<>(Arrays.asList("server1", "server2")), "server1", 1, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);



            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                // start a transaction
                DMLStatementExecutionResult executeUpdateRes =
                    server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.AUTOTRANSACTION_TRANSACTION);
                long tx = executeUpdateRes.transactionId;
                assertTrue(tx > 0);

                // wait for transaction to arrive on server_2
                TestUtils.waitForCondition(() -> {
                    return server_2.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTransaction(tx) != null;
                }, TestUtils.NOOP, 100);

                // make the transaction appear "abandoned" to server_2
                Thread.sleep(1000 + (int) server_2.getManager().getAbandonedTransactionsTimeout());

                server_2.getManager().triggerActivator(ActivatorRunRequest.FULL);

                assertFalse(server_2.getManager().getTableSpaceManager(TableSpace.DEFAULT).isFailed());


            }

            // restart the follower again
            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();
                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));
            }
        }
    }
}
