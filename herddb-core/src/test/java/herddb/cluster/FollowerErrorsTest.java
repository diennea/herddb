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
package herddb.cluster;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.codec.RecordSerializer;
import herddb.core.TableManager;
import herddb.core.TableSpaceManager;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.GetResult;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import herddb.utils.SystemCrashSimulator;
import herddb.utils.ZKTestEnv;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class FollowerErrorsTest {

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
        SystemCrashSimulator.clear();
    }

    @Test
    public void testFollowerBootError() throws Exception {
        
        // inject a temporary error during download
        final AtomicInteger errorCount = new AtomicInteger(1);
        SystemCrashSimulator.addListener(new SystemCrashSimulator.SimpleCrashPointListener("receiveTableDataChunk") {
            @Override
            public void crashPoint(Object... args) throws Exception {
                // force write to disk and then inject an erro
                fail("THIS IS NOT THE GOOD WAY TO FORCE A FLUSH TO DISK");
                TableSpaceManager manager = (TableSpaceManager) args[0];
                TableManager tableManager = (TableManager) manager.getTableManager("t1");
                
                tableManager.forceFlushallPagesForTests();
                if (errorCount.getAndDecrement() > 0) {
                    throw new Exception("error!");
                }
            }
        });

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
                // force downloading a snapshot from the leader
                .set(ServerConfiguration.PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT, true)
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
                        new HashSet<>(Arrays.asList("server1", "server2")), "server1", 2, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

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
            
            fail("This ISSUE  is not finished, if we crash while starting the tablespace we should unload all of the index pages and data pages held in memory and notify the Policy");
            
            fail("This ISSUE  is not finished, we should reproduce the case in which during recovery we are really writing data to disk and eraseTablespaceData really deletes something");

        }
    }
}
