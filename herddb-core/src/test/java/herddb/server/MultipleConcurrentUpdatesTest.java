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
import herddb.client.GetResult;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.assertTrue;

/**
 * Basic server/client boot test
 *
 * @author enrico.olivelli
 */
public class MultipleConcurrentUpdatesTest {
//
//         @Before
//    public void setupLogger() throws Exception {
//        Level level = Level.FINEST;
//        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//
//            @Override
//            public void uncaughtException(Thread t, Throwable e) {
//                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
//                e.printStackTrace();
//            }
//        });
//        java.util.logging.LogManager.getLogManager().reset();
//        ConsoleHandler ch = new ConsoleHandler();
//        ch.setLevel(level);
//        SimpleFormatter f = new SimpleFormatter();
//        ch.setFormatter(f);
//        java.util.logging.Logger.getLogger("").setLevel(level);
//        java.util.logging.Logger.getLogger("").addHandler(ch);
//    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(new ServerConfiguration(baseDir))) {
            server.start();
            ClientConfiguration clientConfiguration = new ClientConfiguration(folder.newFolder().toPath());
            try (HDBClient client = new HDBClient(clientConfiguration);
                HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                long resultCreateTable = connection.executeUpdate(TableSpace.DEFAULT,
                    "CREATE TABLE mytable (id string primary key, n1 long, n2 integer)", 0, false, Collections.emptyList()).updateCount;
                Assert.assertEquals(1, resultCreateTable);

                long tx = connection.beginTransaction(TableSpace.DEFAULT);
                int SIZE = 10_000;
                for (int i = 0; i < SIZE; i++) {
                    connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id,n1,n2) values(?,?,?)", tx, false,
                        Arrays.asList("test_" + i, 1, 2));
                }
                connection.commitTransaction(TableSpace.DEFAULT, tx);

                ExecutorService threadPool = Executors.newFixedThreadPool(100);
                List<Future> futures = new ArrayList<>();
                AtomicLong updates = new AtomicLong();
                AtomicLong gets = new AtomicLong();
                for (int i = 0; i < SIZE * 100; i++) {
                    futures.add(threadPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                boolean update = ThreadLocalRandom.current().nextBoolean();
                                int k = ThreadLocalRandom.current().nextInt(SIZE);
                                if (update) {
                                    updates.incrementAndGet();
                                    long count = connection.executeUpdate(TableSpace.DEFAULT,
                                        "UPDATE mytable set n1=? WHERE id=?", TransactionContext.NOTRANSACTION_ID, false,
                                        Arrays.asList(k, "test_" + k)).updateCount;
                                    if (count <= 0) {
                                        throw new RuntimeException("not updated ?");
                                    }
                                } else {
                                    gets.incrementAndGet();
                                    GetResult res = connection.executeGet(TableSpace.DEFAULT, "SELECT * FROM mytable where id=?",
                                        TransactionContext.NOTRANSACTION_ID, Arrays.asList("test_" + k));
                                    if (res.data == null) {
                                        throw new RuntimeException("not found?");
                                    }

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

                System.out.println("updates:"+updates);
                System.out.println("get:"+gets);
                assertTrue(updates.get()>0);
                assertTrue(gets.get()>0);

            }
        }
    }
}
