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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.backup.BackupUtils;
import herddb.backup.ProgressListener;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.codec.RecordSerializer;
import herddb.core.DBManager;
import herddb.core.TestUtils;
import herddb.model.ColumnTypes;
import herddb.model.ConstValueRecordFunction;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import herddb.utils.ZKTestEnv;

/**
 * Tests around backup/restore
 *
 * @author enrico.olivelli
 */
public class BackupRestoreTest {

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
    public void test_backup_restore() throws Exception {
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

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
            Index index = Index.builder().onTable(table).column("d", ColumnTypes.INTEGER).type(Index.TYPE_BRIN).build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                try (HDBClient client = new HDBClient(client_configuration);
                    HDBConnection connection = client.openConnection()) {

                    assertEquals(4, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, oo, new ProgressListener() {
                    });
                    byte[] backupData = oo.toByteArray();

                    connection.executeUpdate(TableSpace.DEFAULT, "DELETE FROM t1", 0, false, true, Collections.emptyList());

                    assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    BackupUtils.restoreTableSpace("newts", server_2.getNodeId(), connection, new ByteArrayInputStream(backupData), new ProgressListener() {
                    });

                    assertEquals(4, connection.executeScan("newts", "SELECT * FROM newts.t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    assertEquals(1, server_2.getManager().getTableSpaceManager("newts").getIndexesOnTable("t1").size());
                }

            }

        }
    }

    /** Check that restore a dirty delete doesn't revive a record (it is: a phantom deleted record on a dirty page) */
    @Test
    public void test_backup_restore_with_deletes() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        /* Disable page compaction (avoid compaction of dirty page) */
        serverconfig_1.set(ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD, 0.0D);

        ServerConfiguration serverconfig_2 = serverconfig_1
            .copy()
            .set(ServerConfiguration.PROPERTY_NODEID, "server2")
            .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
            .set(ServerConfiguration.PROPERTY_PORT, 7868);

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();

            DBManager manager = server_1.getManager();

            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            manager.checkpoint();

            /* Check that new data isn't in a dirty page  */
            assertEquals(0, manager.getTableSpaceManager(TableSpace.DEFAULT).getTableManager(table.name).getStats().getDirtypages());

            manager.executeUpdate(new DeleteStatement(TableSpace.DEFAULT, table.name, Bytes.from_int(1), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 5, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            /* Now we have a dirty page in the checkpoint */
            manager.checkpoint();

            /* Check that a dirty page exists */
            assertEquals(1, manager.getTableSpaceManager(TableSpace.DEFAULT).getTableManager(table.name).getStats().getDirtypages());


            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                try (HDBClient client = new HDBClient(client_configuration);
                    HDBConnection connection = client.openConnection()) {

                    assertEquals(4, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, oo, new ProgressListener() {
                    });
                    byte[] backupData = oo.toByteArray();

                    connection.executeUpdate(TableSpace.DEFAULT, "DELETE FROM t1", 0, false, true, Collections.emptyList());

                    assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    BackupUtils.restoreTableSpace("newts", server_2.getNodeId(), connection, new ByteArrayInputStream(backupData), new ProgressListener() {
                    });

                    /* No new insert AND no delete... if 5 it did see the new insert but missed the delete! */
                    assertEquals(4, connection.executeScan("newts", "SELECT * FROM newts.t1", true, Collections.emptyList(), 0, 0, 10).consume().size());
                }

            }

        }
    }

    /** Check that restore a dirty update restores the latest record version */
    @Test
    public void test_backup_restore_with_updates() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        /* Disable page compaction (avoid compaction of dirty page) */
        serverconfig_1.set(ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD, 0.0D);

        ServerConfiguration serverconfig_2 = serverconfig_1
            .copy()
            .set(ServerConfiguration.PROPERTY_NODEID, "server2")
            .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath())
            .set(ServerConfiguration.PROPERTY_PORT, 7868);

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();

            DBManager manager = server_1.getManager();

            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            manager.checkpoint();

            /* Check that new data isn't in a dirty page  */
            assertEquals(0, manager.getTableSpaceManager(TableSpace.DEFAULT).getTableManager(table.name).getStats().getDirtypages());

            final Record update = RecordSerializer.makeRecord(table, "c", 2, "d", 22);

            manager.executeUpdate(new InsertStatement(TableSpace.DEFAULT, table.name, RecordSerializer.makeRecord(table, "c", 5, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeUpdate(new UpdateStatement(TableSpace.DEFAULT, table.name, new ConstValueRecordFunction(update.key), new ConstValueRecordFunction(update.value), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            /* Now we have a dirty page in the checkpoint */
            manager.checkpoint();

            /* Check that a dirty page exists */
            assertEquals(1, manager.getTableSpaceManager(TableSpace.DEFAULT).getTableManager(table.name).getStats().getDirtypages());


            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                try (HDBClient client = new HDBClient(client_configuration);
                    HDBConnection connection = client.openConnection()) {

                    assertEquals(5, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, oo, new ProgressListener() {
                    });
                    byte[] backupData = oo.toByteArray();

                    connection.executeUpdate(TableSpace.DEFAULT, "DELETE FROM t1", 0, false, true, Collections.emptyList());

                    assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    BackupUtils.restoreTableSpace("newts", server_2.getNodeId(), connection, new ByteArrayInputStream(backupData), new ProgressListener() {
                    });

                    /* No new insert AND no delete... if 5 it did see the new insert but missed the delete! */
                    assertEquals(5, connection.executeScan("newts", "SELECT * FROM newts.t1", true, Collections.emptyList(), 0, 0, 10).consume().size());
                }

            }

        }
    }

    @Test
    public void test_backup_restore_recover_from_tx_log() throws Exception {
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

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table1 = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
            Index index = Index.builder().onTable(table1).column("d", ColumnTypes.INTEGER).type(Index.TYPE_BRIN).build();

            Table table2 = Table.builder()
                .name("t2")
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table1), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateTableStatement(table2), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                try (HDBClient client = new HDBClient(client_configuration);
                    HDBConnection connection = client.openConnection()) {

                    assertEquals(4, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, oo, new ProgressListener() {
                        @Override
                        public void log(String type, String message, Map<String, Object> context) {
                            System.out.println("PROGRESS: " + type + " " + message + " context:" + context);
                            if (type.equals("beginTable") && "t1".equals(context.get("table"))) {
                                try {
                                    server_1.getManager().executeUpdate(
                                        new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 5, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                                        TransactionContext.NO_TRANSACTION);
                                } catch (StatementExecutionException err) {
                                    throw new RuntimeException(err);
                                }
                            }
                        }

                    });
                    byte[] backupData = oo.toByteArray();

                    connection.executeUpdate(TableSpace.DEFAULT, "DELETE FROM t1", 0, false, true, Collections.emptyList());

                    assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    BackupUtils.restoreTableSpace("newts", server_2.getNodeId(), connection, new ByteArrayInputStream(backupData), new ProgressListener() {
                    });

                    assertEquals(5, connection.executeScan("newts", "SELECT * FROM newts.t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    assertEquals(1, server_2.getManager().getTableSpaceManager("newts").getIndexesOnTable("t1").size());

                }

            }

        }
    }

    @Test
    public void test_backup_restore_recover_from_tx_log_with_transaction() throws Exception {
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

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table1 = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
            Index index = Index.builder().onTable(table1).column("d", ColumnTypes.INTEGER).type(Index.TYPE_BRIN).build();

            Table table2 = Table.builder()
                .name("t2")
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table1), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateTableStatement(table2), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            long tx1 = TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                try (HDBClient client = new HDBClient(client_configuration);
                    HDBConnection connection = client.openConnection()) {

                    assertEquals(4, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, oo, new ProgressListener() {
                        @Override
                        public void log(String type, String message, Map<String, Object> context) {
                            System.out.println("PROGRESS: " + type + " " + message + " context:" + context);
                            if (type.equals("beginTable") && "t1".equals(context.get("table"))) {
                                try {
                                    server_1.getManager().executeUpdate(
                                        new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 5, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                                        new TransactionContext(tx1));
                                    TestUtils.commitTransaction(server_1.getManager(), TableSpace.DEFAULT, tx1);
                                } catch (StatementExecutionException err) {
                                    throw new RuntimeException(err);
                                }
                            }
                        }

                    });
                    byte[] backupData = oo.toByteArray();

                    connection.executeUpdate(TableSpace.DEFAULT, "DELETE FROM t1", 0, false, true, Collections.emptyList());

                    assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    BackupUtils.restoreTableSpace("newts", server_2.getNodeId(), connection, new ByteArrayInputStream(backupData), new ProgressListener() {
                    });

                    assertEquals(5, connection.executeScan("newts", "SELECT * FROM newts.t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    assertEquals(1, server_2.getManager().getTableSpaceManager("newts").getIndexesOnTable("t1").size());

                }

            }

        }
    }

    @Test
    public void test_backup_restore_recover_from_tx_log_with_newtransaction_during_dump() throws Exception {
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

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table1 = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
            Index index = Index.builder().onTable(table1).column("d", ColumnTypes.INTEGER).type(Index.TYPE_BRIN).build();

            Table table2 = Table.builder()
                .name("t2")
                .column("c", ColumnTypes.INTEGER)
                .column("d", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();

            server_1.getManager().executeStatement(new CreateTableStatement(table1), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateTableStatement(table2), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                try (HDBClient client = new HDBClient(client_configuration);
                    HDBConnection connection = client.openConnection()) {

                    assertEquals(4, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, oo, new ProgressListener() {
                        @Override
                        public void log(String type, String message, Map<String, Object> context) {
                            System.out.println("PROGRESS: " + type + " " + message + " context:" + context);
                            if (type.equals("beginTable") && "t1".equals(context.get("table"))) {
                                try {
                                    long tx1 = TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);
                                    server_1.getManager().executeUpdate(
                                        new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table1, "c", 5, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                                        new TransactionContext(tx1));
                                    TestUtils.commitTransaction(server_1.getManager(), TableSpace.DEFAULT, tx1);
                                } catch (StatementExecutionException err) {
                                    throw new RuntimeException(err);
                                }
                            }
                        }

                    });
                    byte[] backupData = oo.toByteArray();

                    connection.executeUpdate(TableSpace.DEFAULT, "DELETE FROM t1", 0, false, true, Collections.emptyList());

                    assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    BackupUtils.restoreTableSpace("newts", server_2.getNodeId(), connection, new ByteArrayInputStream(backupData), new ProgressListener() {
                    });

                    assertEquals(5, connection.executeScan("newts", "SELECT * FROM newts.t1", true, Collections.emptyList(), 0, 0, 10).consume().size());

                    assertEquals(1, server_2.getManager().getTableSpaceManager("newts").getIndexesOnTable("t1").size());

                }

            }

        }
    }

}
