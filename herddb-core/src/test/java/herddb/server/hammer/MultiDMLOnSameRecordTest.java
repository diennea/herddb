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
package herddb.server.hammer;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.core.DBManager;
import herddb.core.TestUtils;
import herddb.core.stats.TableManagerStats;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.utils.RawString;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author enrico.olivelli
 */
public class MultiDMLOnSameRecordTest {

    private static final RawString N1 = RawString.of("n1");
    private static final int THREADPOLSIZE = 100;
    private static final int TESTSIZE = 10000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfiguration = new ServerConfiguration(baseDir);

        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 10 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 1024 * 1024 / 4);
        serverConfiguration.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 1024 * 1024);
        serverConfiguration.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 0);
        serverConfiguration.set(ServerConfiguration.PROPERTY_DATADIR, folder.newFolder().getAbsolutePath());
        serverConfiguration.set(ServerConfiguration.PROPERTY_LOGDIR, folder.newFolder().getAbsolutePath());

        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForStandaloneBoot();
            DBManager manager = server.getManager();
            execute(manager, "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", Collections.emptyList());

            ExecutorService threadPool = Executors.newFixedThreadPool(THREADPOLSIZE);
            try {
                List<Future> futures = new ArrayList<>();
                AtomicLong updates = new AtomicLong();
                AtomicLong deletes = new AtomicLong();
                AtomicLong inserts = new AtomicLong();
                AtomicLong duplicatePkErrors = new AtomicLong();

                for (int i = 0; i < TESTSIZE; i++) {
                    futures.add(threadPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {

                                int k = ThreadLocalRandom.current().nextInt(10);
                                long value = ThreadLocalRandom.current().nextInt(100) + 1000;
                                String key = "test_" + k;
                                boolean update = ThreadLocalRandom.current().nextBoolean();
                                boolean insert = ThreadLocalRandom.current().nextBoolean();
                                boolean delete = ThreadLocalRandom.current().nextBoolean();
                                if (update) {
                                    updates.incrementAndGet();

                                    execute(manager, "UPDATE mytable set n1=? WHERE id=?", Arrays.asList(value, key));
                                } else if (insert) {
                                    inserts.incrementAndGet();
                                    try {
                                        execute(manager, "INSERT INTO mytable(n1, id) values(?,?)",
                                                Arrays.asList(value, key));
                                    } catch (DuplicatePrimaryKeyException ok) {
                                        duplicatePkErrors.incrementAndGet();
                                    }
                                } else if (delete) {
                                    deletes.incrementAndGet();
                                    execute(manager, "DELETE FROM mytable WHERE id=?",
                                            Arrays.asList(key));
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
                System.out.println("stats::deletes:" + deletes);
                System.out.println("stats::inserts:" + inserts);
                System.out.println("stats::duplicatePkErrors:" + duplicatePkErrors);

                TableManagerStats stats = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableManager("mytable").getStats();
                System.out.println("stats::tablesize:" + stats.getTablesize());
                System.out.println("stats::dirty records:" + stats.getDirtyrecords());
                System.out.println("stats::unload count:" + stats.getUnloadedPagesCount());
                System.out.println("stats::load count:" + stats.getLoadedPagesCount());
                System.out.println("stats::buffers used mem:" + stats.getBuffersUsedMemory());

                assertTrue(stats.getUnloadedPagesCount() > 0);
            } finally {
                threadPool.shutdown();
                threadPool.awaitTermination(1, TimeUnit.MINUTES);
            }

        }

        // restart and recovery
        try (Server server = new Server(serverConfiguration)) {
            server.start();
            server.waitForTableSpaceBoot(TableSpace.DEFAULT, 300000, true);
        }
    }
}
