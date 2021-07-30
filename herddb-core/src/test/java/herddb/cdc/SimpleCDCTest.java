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

package herddb.cdc;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.codec.RecordSerializer;
import herddb.core.TestUtils;
import herddb.log.LogSequenceNumber;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import herddb.utils.ZKTestEnv;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Tests around backup/restore
 *
 * @author enrico.olivelli
 */
public class SimpleCDCTest {

    private static final Logger LOG = Logger.getLogger(SimpleCDCTest.class.getName());

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
    public void testBasicCaptureDataChange() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

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
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            long tx = TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));

            List<ChangeDataCapture.Mutation> mutations = new ArrayList<>();
            try (final ChangeDataCapture cdc = new ChangeDataCapture(
                    server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableSpaceUUID(),
                    client_configuration,
                    new ChangeDataCapture.MutationListener() {
                        @Override
                        public void accept(ChangeDataCapture.Mutation mutation) {
                            LOG.log(Level.INFO, "mutation " + mutation);
                            assertTrue(mutation.getTimestamp() > 0);
                            assertNotNull(mutation.getLogSequenceNumber());
                            assertNotNull(mutation.getTable());
                            mutations.add(mutation);
                        }
                    },
                    LogSequenceNumber.START_OF_TIME);) {

                cdc.start();

                cdc.run();

                // we are missing the last entry, because it is not confirmed yet on BookKeeper at this point
                // also the mutations in the transaction are not visible
                assertEquals(3, mutations.size());

                // commit the transaction
                TestUtils.commitTransaction(server_1.getManager(), TableSpace.DEFAULT, tx);

                server_1.getManager().executeUpdate(new UpdateStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "d", 2), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                cdc.run();
                assertEquals(5, mutations.size());

                server_1.getManager().executeStatement(new AlterTableStatement(Arrays.asList(Column.column("e", ColumnTypes.INTEGER)), Collections.emptyList(), Collections.emptyList(), null, table.name, TableSpace.DEFAULT, null, Collections.emptyList(),
                        Collections.emptyList()), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                cdc.run();
                assertEquals(6, mutations.size());


                // transaction to be rolled back
                long tx2 = TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);
                server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 30, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx2));
                server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 31, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx2));
                TestUtils.roolbackTransaction(server_1.getManager(), TableSpace.DEFAULT, tx2);

                // nothing is to be sent to CDC
                cdc.run();
                assertEquals(7, mutations.size());

                server_1.getManager().executeUpdate(new DeleteStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                cdc.run();
                assertEquals(7, mutations.size());

                // close the server...close the ledger, now we can read the last mutation
                server_1.close();

                cdc.run();
                assertEquals(8, mutations.size());


                int i = 0;
                assertEquals(ChangeDataCapture.MutationType.CREATE_TABLE, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.UPDATE, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.ALTER_TABLE, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.DELETE, mutations.get(i++).getMutationType());


            }
        }
    }

    @Test
    public void testBasicCaptureDataChangeWithTransactions() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

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

            // create table in transaction
            long tx = TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            // work on the table in transaction
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));

            // commit
            TestUtils.commitTransaction(server_1.getManager(), TableSpace.DEFAULT, tx);

            // work on the table outside of the transaction
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // close the server and the ledger
            server_1.close();

            List<ChangeDataCapture.Mutation> mutations = new ArrayList<>();
            try (final ChangeDataCapture cdc = new ChangeDataCapture(
                    server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableSpaceUUID(),
                    client_configuration,
                    new ChangeDataCapture.MutationListener() {
                        @Override
                        public void accept(ChangeDataCapture.Mutation mutation) {
                            LOG.log(Level.INFO, "mutation " + mutation);
                            assertTrue(mutation.getTimestamp() > 0);
                            assertNotNull(mutation.getLogSequenceNumber());
                            assertNotNull(mutation.getTable());
                            mutations.add(mutation);
                        }
                    },
                    LogSequenceNumber.START_OF_TIME);) {
                cdc.start();
                cdc.run();


                assertEquals(3, mutations.size());

                int i = 0;
                ChangeDataCapture.Mutation m1 = mutations.get(i++);
                assertEquals(ChangeDataCapture.MutationType.CREATE_TABLE, m1.getMutationType());
                Table tableFromM1 = m1.getTable();
                assertNotNull(tableFromM1);
                assertEquals(table, tableFromM1);
                ChangeDataCapture.Mutation m2 = mutations.get(i++);
                assertEquals(ChangeDataCapture.MutationType.INSERT, m2.getMutationType());
                assertEquals(m2.getTable(), tableFromM1);
                assertEquals(1, m2.getRecord().get("c"));
                assertEquals(2, m2.getRecord().get("d"));
                ChangeDataCapture.Mutation m3 = mutations.get(i++);
                assertEquals(ChangeDataCapture.MutationType.INSERT, m3.getMutationType());
                assertEquals(m3.getTable(), tableFromM1);
                assertEquals(2, m3.getRecord().get("c"));
                assertEquals(2, m3.getRecord().get("d"));

            }
        }
    }

}
