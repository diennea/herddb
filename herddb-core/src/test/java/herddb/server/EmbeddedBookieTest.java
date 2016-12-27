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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.ClientSideMetadataProviderException;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ScanResultSet;
import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
import herddb.model.ColumnTypes;
import herddb.model.GetResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.utils.Bytes;
import herddb.utils.ZKTestEnv;

/**
 * Booting the Bookie inside HerdDB Server
 *
 * @author enrico.olivelli
 */
public class EmbeddedBookieTest {

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
    public void test_boot_with_bookie() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder("server1").toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_BOOKKEEPER_START, true);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder("server2").toPath().toAbsolutePath())
                .set(ServerConfiguration.PROPERTY_PORT, 7868)
                .set(ServerConfiguration.PROPERTY_BOOKKEEPER_BOOKIE_PORT, 3182);

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

            try (Server server_2 = new Server(serverconfig_2)) {
                server_2.start();

                server_1.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(server_2.getManager().waitForTablespace(TableSpace.DEFAULT, 60000, false));

                server_2.getManager().setErrorIfNotLeader(false);
                // wait for data to arrive on server_2
                for (int i = 0; i < 100; i++) {
                    GetResult found = server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                    if (found.found()) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertTrue(server_2.getManager().get(new GetStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION).found());

                server_2.getManager().setErrorIfNotLeader(true);

                ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
                client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
                client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
                client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
                client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

                try (HDBClient client = new HDBClient(client_configuration);
                        HDBConnection connection = client.openConnection()) {

                    try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1 WHERE c=1", Collections.emptyList(), 0, 0, 10);) {
                        assertEquals(1, scan.consume().size());
                    }                    
                    // switch leader to server2
                    server_2.getManager().executeStatement(new AlterTableSpaceStatement(TableSpace.DEFAULT,
                            new HashSet<>(Arrays.asList("server1", "server2")), "server2", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                    // wait that server_1 leaves leadership                    
                    for (int i = 0; i < 100; i++) {
                        TableSpaceManager tManager = server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT);
                        if (tManager != null && !tManager.isLeader()) {
                            break;
                        }
                        Thread.sleep(100);
                    }
                    TableSpaceManager tManager = server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT);
                    assertTrue(tManager != null && !tManager.isLeader());

                    // the client MUST automatically look for the new leader
                    try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1 WHERE c=1", Collections.emptyList(), 0, 0, 10);) {
                        assertEquals(1, scan.consume().size());
                    }

                }

                // assert  that server_1 is not accepting request any more
                try (HDBClient client_to_1 = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                        HDBConnection connection = client_to_1.openConnection()) {
                    client_to_1.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server_1));
                    try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1 WHERE c=1", Collections.emptyList(), 0, 0, 10);) {
                        fail("server_1 MUST not accept queries");
                    } catch (ClientSideMetadataProviderException ok) {
                    }
                }

                // assert that server_2 is accepting requests
                try (HDBClient client_to_2 = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                        HDBConnection connection = client_to_2.openConnection()) {
                    client_to_2.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server_2));
                    try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT, "SELECT * FROM t1 WHERE c=1", Collections.emptyList(), 0, 0, 10);) {
                        assertEquals(1, scan.consume().size());
                    }
                }

            }

        }
    }

}
