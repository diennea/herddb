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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.TableSpaceDumpReceiver;
import herddb.client.ZookeeperClientSideMetadataProvider;
import herddb.codec.RecordSerializer;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.storage.DataStorageManagerException;
import herddb.utils.ZKTestEnv;

/**
 * Booting two servers, one table space
 *
 * @author enrico.olivelli
 */
public class DownloadTableSpaceTest {

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
    public void downloadTablespaceTest() throws Exception {
        ServerConfiguration serverconfig_1 = new ServerConfiguration(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_PORT, 7867);
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                .name("t1")
                .column("c", ColumnTypes.INTEGER)
                .primaryKey("c")
                .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            for (int i = 0; i < 1000; i++) {
                server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", i)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            }

            List<Map<String, Object>> logical_data = new ArrayList<>();
            AtomicBoolean start = new AtomicBoolean();

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                HDBConnection con = client.openConnection()) {
                client.setClientSideMetadataProvider(new ZookeeperClientSideMetadataProvider(testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath()));
                CountDownLatch count = new CountDownLatch(1);
                con.dumpTableSpace(TableSpace.DEFAULT, new TableSpaceDumpReceiver() {

                    Table table;

                    @Override
                    public void start(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException {
                        System.out.println("start at " + logSequenceNumber);
                        start.set(true);
                    }

                    @Override
                    public void finish(LogSequenceNumber logSequenceNumber) {
                        System.out.println("finish!");
                        count.countDown();
                    }

                    @Override
                    public void endTable() {
                        System.out.println("endTable");
                        table = null;
                    }

                    @Override
                    public void receiveTableDataChunk(List<Record> records) {
//                        System.out.println("receiveTableDataChunk " + records);
                        for (Record r : records) {
                            Map<String, Object> bean = r.toBean(table);
//                            System.out.println("received:" + bean);
                            logical_data.add(bean);
                        }
                    }

                    @Override
                    public void beginTable(Table table, Map<String, Object> stats) {
                        System.out.println("beginTable " + table);
                        this.table = table;
                    }

                }, 89, false
                );
                assertTrue(count.await(20, TimeUnit.SECONDS));
                assertEquals(1000, logical_data.size());
                assertTrue(start.get());
            }
        }
    }

}
