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
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.utils.ZKTestEnv;
import java.util.Map;

/**
 * Booting two servers, one table space
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

                    assertEquals(4, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", Collections.emptyList(), 0, 0, 10).consume().size());

                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, oo, new ProgressListener() {
                    });
                    byte[] backupData = oo.toByteArray();

                    connection.executeUpdate(TableSpace.DEFAULT, "DELETE FROM t1", 0, false, Collections.emptyList());

                    assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", Collections.emptyList(), 0, 0, 10).consume().size());

                    BackupUtils.restoreTableSpace("newts", server_2.getNodeId(), connection, new ByteArrayInputStream(backupData), new ProgressListener() {
                    });

                    assertEquals(4, connection.executeScan("newts", "SELECT * FROM newts.t1", Collections.emptyList(), 0, 0, 10).consume().size());

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

                    assertEquals(4, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", Collections.emptyList(), 0, 0, 10).consume().size());

                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BackupUtils.dumpTableSpace(TableSpace.DEFAULT, 64 * 1024, connection, oo, new ProgressListener() {
                        @Override
                        public void log(String message, Map<String, Object> context) {
                            System.out.println("PROGRESS: " + message + " context:" + context);
                            if (message.startsWith("table t1")) {
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

                    connection.executeUpdate(TableSpace.DEFAULT, "DELETE FROM t1", 0, false, Collections.emptyList());

                    assertEquals(0, connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1", Collections.emptyList(), 0, 0, 10).consume().size());

                    BackupUtils.restoreTableSpace("newts", server_2.getNodeId(), connection, new ByteArrayInputStream(backupData), new ProgressListener() {
                    });

                    assertEquals(5, connection.executeScan("newts", "SELECT * FROM newts.t1", Collections.emptyList(), 0, 0, 10).consume().size());

                }

            }

        }
    }

}
