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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.DMLResult;
import herddb.client.GetResult;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.core.stats.TableManagerStats;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;

/**
 * Concurrent updates
 *
 * @author enrico.olivelli
 */
public class MultipleConcurrentUpdatesTest {

    private static int TABLESIZE = 10_000;
    private static int MULTIPLIER = 2;
    private static int THREADPOLSIZE = 100;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        performTest(false, 0);
    }

    @Test
    public void testWithTransactions() throws Exception {
        performTest(true, 0);
    }

    @Test
    public void testWithCheckpoints() throws Exception {
        performTest(false, 2000);
    }

    @Test
    public void testWithTransactionsWithCheckpoints() throws Exception {
        performTest(true, 2000);
    }

    private void performTest(boolean useTransactions, long checkPointPeriod) throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfiguration = new ServerConfiguration(baseDir);

        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 10 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 1024 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 1024 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, checkPointPeriod);
        serverConfiguration.set(ServerConfiguration.PROPERTY_DATADIR, folder.newFolder().getAbsolutePath());
        serverConfiguration.set(ServerConfiguration.PROPERTY_LOGDIR, folder.newFolder().getAbsolutePath());

        ConcurrentHashMap<String, Integer> expectedValue = new ConcurrentHashMap<>();
        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                    "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                long tx = connection.beginTransaction(TableSpace.DEFAULT);                
                for (int i = 0; i < TABLESIZE; i++) {
                    connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false,
                        Arrays.asList("test_" + i, 1, 2));
                    expectedValue.put("test_" + i, 1);
                }
                connection.commitTransaction(TableSpace.DEFAULT, tx);
                ExecutorService threadPool = Executors.newFixedThreadPool(THREADPOLSIZE);
                try {
                    List<Future> futures = new ArrayList<>();
                    AtomicLong updates = new AtomicLong();
                    AtomicLong gets = new AtomicLong();
                    for (int i = 0; i < TABLESIZE * MULTIPLIER; i++) {
                        futures.add(threadPool.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    boolean update = ThreadLocalRandom.current().nextBoolean();
                                    int k = ThreadLocalRandom.current().nextInt(TABLESIZE);
                                    int value = ThreadLocalRandom.current().nextInt(TABLESIZE);
                                    long transactionId;
                                    if (update) {
                                        updates.incrementAndGet();
                                        DMLResult updateResult = connection.executeUpdate(TableSpace.DEFAULT,
                                            "UPDATE mytable set n1=? WHERE id=?", useTransactions ? TransactionContext.AUTOTRANSACTION_ID : TransactionContext.NOTRANSACTION_ID, false,
                                            Arrays.asList(value, "test_" + k));
                                        long count = updateResult.updateCount;
                                        transactionId = updateResult.transactionId;
                                        if (count <= 0) {
                                            throw new RuntimeException("not updated ?");
                                        }
                                        expectedValue.put("test_" + k, value);
                                    } else {
                                        gets.incrementAndGet();
                                        GetResult res = connection.executeGet(TableSpace.DEFAULT, "SELECT * FROM mytable where id=?",
                                            useTransactions ? TransactionContext.AUTOTRANSACTION_ID : TransactionContext.NOTRANSACTION_ID, Arrays.asList("test_" + k));
                                        if (res.data == null) {
                                            throw new RuntimeException("not found?");
                                        }
                                        transactionId = res.transactionId;
                                    }
                                    if (useTransactions) {
                                        if (transactionId <= 0) {
                                            throw new RuntimeException("no transaction ?");
                                        }
                                        connection.commitTransaction(TableSpace.DEFAULT, transactionId);
                                    }
                                } catch (Exception err) {
                                    throw new RuntimeException(err);
                                }
                            }
                        }
                        ));
                    }
                    for (Future f : futures) {
                        f.get();
                    }

                    System.out.println("stats::updates:" + updates);
                    System.out.println("stats::get:" + gets);
                    assertTrue(updates.get() > 0);
                    assertTrue(gets.get() > 0);

                    List<String> erroredKeys = new ArrayList<>();
                    for (Map.Entry<String, Integer> entry : expectedValue.entrySet()) {
                        GetResult res = connection.executeGet(TableSpace.DEFAULT, "SELECT n1 FROM mytable where id=?",
                            TransactionContext.NOTRANSACTION_ID, Arrays.asList(entry.getKey()));
                        assertNotNull(res.data);

                        if (!Long.valueOf(entry.getValue()).equals(res.data.get("n1"))) {
                            System.out.println("expected value "+res.data.get("n1")+", but got "+Long.valueOf(entry.getValue())+" for key "+entry.getKey());
                            erroredKeys.add(entry.getKey());
                        }                        
                    }
                    assertTrue(erroredKeys.isEmpty());

                    TableManagerStats stats = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableManager("mytable").getStats();
                    System.out.println("stats::tablesize:" + stats.getTablesize());
                    System.out.println("stats::dirty records:" + stats.getDirtyrecords());
                    System.out.println("stats::unload count:" + stats.getUnloadedPagesCount());
                    System.out.println("stats::load count:" + stats.getLoadedPagesCount());
                    System.out.println("stats::buffers used mem:" + stats.getBuffersUsedMemory());

                    assertTrue(stats.getUnloadedPagesCount() > 0);
                    assertEquals(TABLESIZE, stats.getTablesize());
                } finally {
                    threadPool.shutdown();
                    threadPool.awaitTermination(1, TimeUnit.MINUTES);
                }
            }
        }
        
        // restart and recovery
        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                for (Map.Entry<String, Integer> entry : expectedValue.entrySet()) {
                    GetResult res = connection.executeGet(TableSpace.DEFAULT, "SELECT n1 FROM mytable where id=?",
                        TransactionContext.NOTRANSACTION_ID, Arrays.asList(entry.getKey()));
                    assertNotNull(res.data);
                    assertEquals(Long.valueOf(entry.getValue()), res.data.get("n1"));
                }
            }
        }
    }
}
