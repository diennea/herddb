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
import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import herddb.core.DBManager;
import herddb.core.stats.TableManagerStats;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.TableSpace;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Concurrent access to the same record, in particular DELETE vs UPDATE. We are using a set of N keys
 *
 * @author enrico.olivelli
 */
public class MultiDMLOnSameRecordTest {

    private static final int THREADPOLSIZE = 4;
    private static final int TESTSIZE = 1000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testWithPrimaryKeyIndexSeek() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(baseDir);

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
                                    System.err.println("do " + Thread.currentThread() + " update on " + Bytes.from_string(key));
                                    updates.incrementAndGet();

                                    execute(manager, "UPDATE mytable set n1=? WHERE id=?", Arrays.asList(value, key));
                                } else if (insert) {
                                    System.err.println("do " + Thread.currentThread() + " insert on " + Bytes.from_string(key));
                                    inserts.incrementAndGet();
                                    try {
                                        execute(manager, "INSERT INTO mytable(n1, id) values(?,?)",
                                                Arrays.asList(value, key));
                                    } catch (DuplicatePrimaryKeyException ok) {
                                        duplicatePkErrors.incrementAndGet();
                                    }
                                } else if (delete) {
                                    System.err.println("do " + Thread.currentThread() + " delete on " + Bytes.from_string(key));
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

    @Test
    public void testWithFullTableScan() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(baseDir);

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
            execute(manager, "CREATE TABLE mytable (id string primary key, n1 long, k2 string)", Collections.emptyList());

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
                            Thread.currentThread().setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                                @Override
                                public void uncaughtException(Thread t, Throwable e) {
                                    e.printStackTrace();
                                }
                            });
                            try {

                                int k = ThreadLocalRandom.current().nextInt(10);
                                long value = ThreadLocalRandom.current().nextInt(100) + 1000;
                                String key = "test_" + k;
                                boolean update = ThreadLocalRandom.current().nextBoolean();
                                boolean insert = ThreadLocalRandom.current().nextBoolean();
                                boolean delete = ThreadLocalRandom.current().nextBoolean();
                                if (update) {
                                    updates.incrementAndGet();

                                    System.out.println("do " + Thread.currentThread() + " update on " + Bytes.from_string(key));
                                    execute(manager, "UPDATE mytable set n1=? WHERE k2=?", Arrays.asList(value, key));
                                } else if (insert) {
                                    System.out.println("do " + Thread.currentThread() + " insert on " + Bytes.from_string(key));
                                    inserts.incrementAndGet();
                                    try {
                                        execute(manager, "INSERT INTO mytable(n1, id, k2) values(?,?,?)",
                                                Arrays.asList(value, key, key));
                                    } catch (DuplicatePrimaryKeyException ok) {
                                        duplicatePkErrors.incrementAndGet();
                                    }
                                } else if (delete) {
                                    System.out.println("do " + Thread.currentThread() + " delete on " + Bytes.from_string(key));
                                    deletes.incrementAndGet();
                                    execute(manager, "DELETE FROM mytable WHERE k2=?",
                                            Arrays.asList(key));
                                }
                            } catch (Throwable err) {
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
                System.out.println("stats::deletes:" + deletes);
                System.out.println("stats::inserts:" + inserts);
                System.out.println("stats::duplicatePkErrors:" + duplicatePkErrors);

                TableManagerStats stats = server.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableManager("mytable").getStats();
                System.out.println("stats::tablesize:" + stats.getTablesize());
                System.out.println("stats::dirty records:" + stats.getDirtyrecords());
                System.out.println("stats::unload count:" + stats.getUnloadedPagesCount());
                System.out.println("stats::load count:" + stats.getLoadedPagesCount());
                System.out.println("stats::buffers used mem:" + stats.getBuffersUsedMemory());
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

    @Test
    public void testWithIndexSeek() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfiguration = newServerConfigurationWithAutoPort(baseDir);

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
            execute(manager, "CREATE TABLE mytable (id string primary key, n1 long, k2 string)", Collections.emptyList());
            execute(manager, "CREATE INDEX tt ON mytable (k2)", Collections.emptyList());

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

                                    execute(manager, "UPDATE mytable set n1=? WHERE k2=?", Arrays.asList(value, key));
                                } else if (insert) {
                                    inserts.incrementAndGet();
                                    try {
                                        execute(manager, "INSERT INTO mytable(n1, id, k2) values(?,?,?)",
                                                Arrays.asList(value, key, key));
                                    } catch (DuplicatePrimaryKeyException ok) {
                                        duplicatePkErrors.incrementAndGet();
                                    }
                                } else if (delete) {
                                    deletes.incrementAndGet();
                                    execute(manager, "DELETE FROM mytable WHERE k2=?",
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