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

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.DMLResult;
import herddb.client.GetResult;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.ScanResultSet;
import herddb.core.stats.TableManagerStats;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.utils.RawString;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This tests emulates a system which tracks changes to a table using another table
 *
 * @author enrico.olivelli
 */
public class HistoryChangelogTest {

    private static final int TABLESIZE = 10000;
    private static final int MULTIPLIER = 20;
    private static final int THREADPOLSIZE = 10;

    private static class Element {

        private final int status;
        private final long hid;

        public Element(int status, long current_hid) {
            this.status = status;
            this.hid = current_hid;
        }

    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    @Ignore
    public void test() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(baseDir);

        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 10 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 1024 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 1024 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0);
        serverConfiguration.set(ServerConfiguration.PROPERTY_DATADIR, folder.newFolder().getAbsolutePath());
        serverConfiguration.set(ServerConfiguration.PROPERTY_LOGDIR, folder.newFolder().getAbsolutePath());

        ConcurrentSkipListSet<Long> doneElements = new ConcurrentSkipListSet<>();

        ConcurrentHashMap<Long, Element> elements = new ConcurrentHashMap<>();
        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForStandaloneBoot();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id long primary key, hid long, status integer)", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                long resultCreateTableHistory = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE history (id long, hid long, status integer, primary key (id,hid) )", 0, false, true, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTableHistory);

                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                for (long i = 0; i < TABLESIZE; i++) {
                    int status = 0;
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,hid,status) values(?,?,?)", tx, false, true,
                            Arrays.asList(i, 0, status));
                    elements.put(i, new Element(0, status));
                }
                connection.commitTransaction(TableSpace.DEFAULT, tx);

                ExecutorService threadPool = Executors.newFixedThreadPool(THREADPOLSIZE);
                try {
                    List<Future> futures = new ArrayList<>();
                    AtomicLong updates = new AtomicLong();
                    for (int i = 0; i < TABLESIZE * MULTIPLIER; i++) {
                        futures.add(threadPool.submit(new Runnable() {
                                                          @Override
                                                          public void run() {
                                                              try {

                                                                  long id = ThreadLocalRandom.current().nextInt(TABLESIZE);
                                                                  doneElements.add(id);
                                                                  Element element = elements.remove(id);
                                                                  if (element == null) {
                                                                      return;
                                                                  }

                                                                  int new_status = element.status + 1;
                                                                  long next_hid = element.hid + 1;

                                                                  long transactionId;
                                                                  updates.incrementAndGet();

                                                                  DMLResult updateResult = connection.executeUpdate(TableSpace.DEFAULT,
                                                                          "UPDATE mytable set hid=?,status=? WHERE id=?", TransactionContext.AUTOTRANSACTION_ID, false, true,
                                                                          Arrays.asList(next_hid, new_status, id));

                                                                  transactionId = updateResult.transactionId;
                                                                  if (updateResult.updateCount <= 0) {
                                                                      throw new RuntimeException("not updated ?");
                                                                  }
                                                                  DMLResult insertResult = connection.executeUpdate(TableSpace.DEFAULT,
                                                                          "INSERT INTO history (id,hid,status) values (?,?,?)", transactionId, false, true,
                                                                          Arrays.asList(id, next_hid, new_status));
                                                                  if (insertResult.updateCount <= 0) {
                                                                      throw new RuntimeException("not inserted ?");
                                                                  }
                                                                  connection.commitTransaction(TableSpace.DEFAULT, transactionId);

                                                                  // make the element available again
                                                                  elements.put(id, new Element(new_status, next_hid));

                                                              } catch (Exception err) {
                                                                  err.printStackTrace();
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
                    assertTrue(updates.get() > 0);

                    TableManagerStats stats = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableManager("mytable").getStats();
                    System.out.println("stats::tablesize:" + stats.getTablesize());
                    System.out.println("stats::dirty records:" + stats.getDirtyrecords());
                    System.out.println("stats::unload count:" + stats.getUnloadedPagesCount());
                    System.out.println("stats::load count:" + stats.getLoadedPagesCount());
                    System.out.println("stats::buffers used mem:" + stats.getBuffersUsedMemory());

                    assertEquals(TABLESIZE, stats.getTablesize());

                    for (Map.Entry<Long, Element> entry : elements.entrySet()) {
                        {
                            GetResult res = connection.executeGet(TableSpace.DEFAULT, "SELECT status,hid FROM mytable where id=?",
                                    TransactionContext.NOTRANSACTION_ID, true, Arrays.asList(entry.getKey()));
                            assertNotNull(res.data);
                            assertEquals(entry.getValue().status, res.data.get(RawString.of("status")));
                            assertEquals(entry.getValue().hid, res.data.get(RawString.of("hid")));
                        }
                        if (doneElements.contains(entry.getKey())) {
                            ScanResultSet res = connection.executeScan(TableSpace.DEFAULT, "SELECT id, status, hid, (SELECT MAX(hid) as mm  from history where history.id=mytable.id) as maxhid "
                                            + "FROM mytable where id=?", true, Arrays.asList(entry.getKey()),
                                    TransactionContext.NOTRANSACTION_ID, -1, 10000, true);
                            List<Map<String, Object>> consume = res.consume();
                            assertEquals(1, consume.size());
                            Map<String, Object> data = consume.get(0);
                            System.out.println("data:" + data);
                            assertEquals(entry.getValue().status, data.get("status"));
                            assertEquals(entry.getValue().hid, data.get("hid"));
                            assertEquals(entry.getValue().hid, data.get("maxhid"));
                            assertEquals(entry.getKey(), data.get("id"));
                        }
                    }
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
                for (Map.Entry<Long, Element> entry : elements.entrySet()) {
                    {
                        GetResult res = connection.executeGet(TableSpace.DEFAULT,
                                "SELECT status,hid FROM mytable where id=?",
                                TransactionContext.NOTRANSACTION_ID, true, Arrays.asList(entry.getKey()));
                        assertNotNull(res.data);
                        assertEquals(entry.getValue().status, res.data.get(RawString.of("status")));
                        assertEquals(entry.getValue().hid, res.data.get(RawString.of("hid")));
                    }
                    if (doneElements.contains(entry.getKey())) {
                        ScanResultSet res = connection.executeScan(TableSpace.DEFAULT, "SELECT id, status, hid, (SELECT MAX(hid) as mm  from history where history.id=mytable.id) as maxhid "
                                        + "FROM mytable where id=?", true, Arrays.asList(entry.getKey()),
                                TransactionContext.NOTRANSACTION_ID, -1, 10000, true);
                        List<Map<String, Object>> consume = res.consume();
                        assertEquals(1, consume.size());
                        Map<String, Object> data = consume.get(0);
                        System.out.println("data:" + data);
                        assertEquals(entry.getValue().status, data.get("status"));
                        assertEquals(entry.getValue().hid, data.get("hid"));
                        assertEquals(entry.getValue().hid, data.get("maxhid"));
                        assertEquals(entry.getKey(), data.get("id"));
                    }
                }
            }
        }
    }

}
