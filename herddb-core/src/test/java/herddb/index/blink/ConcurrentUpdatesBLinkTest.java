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
package herddb.index.blink;

import herddb.core.RandomPageReplacementPolicy;
import herddb.utils.RandomString;
import herddb.utils.Sized;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Tests on concurcurrent massive updates on different keys
 */
public class ConcurrentUpdatesBLinkTest {

    @Test
    public void concurrentUpdatesTests() throws Exception {

        int DATA_SIZE = 50000;
        int THREADS = 4;
        int KEY_SIZE = 25;
        int ITERATIONS = 100000;
        BLinkTest.DummyBLinkIndexDataStorage<Sized<String>, Long> storage = new BLinkTest.DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<String>, Long> blink = new BLink<>(2048L, new BLinkTest.StringSizeEvaluator(), new RandomPageReplacementPolicy(30), storage)) {

            Random random = new Random();
            RandomString rs = new RandomString(random);
            ConcurrentHashMap<String, Long> expectedValues = new ConcurrentHashMap<>();
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < DATA_SIZE; ++i) {
                String key = rs.nextString(KEY_SIZE);
                keys.add(key);
                long value = random.nextInt(Integer.MAX_VALUE) + 1;
                assertTrue(value > 0); // zero means null
                blink.insert(Sized.valueOf(key), value);
                expectedValues.put(key, value);
            }
            int numKeys = keys.size();
            ExecutorService threadpool = Executors.newFixedThreadPool(THREADS);
            System.out.println("generated " + numKeys + " keys");
            AtomicLong updates = new AtomicLong();
            AtomicLong inserts = new AtomicLong();
            AtomicLong skipped = new AtomicLong();
            AtomicLong deletes = new AtomicLong();
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < ITERATIONS; i++) {
                String key = keys.get(random.nextInt(numKeys));
                long value = random.nextLong();
                boolean delete = random.nextInt(100) < 10; // 10 % deletes
                futures.add(threadpool.submit(new Runnable() {
                    @Override
                    public void run() {
                        Long current = expectedValues.remove(key);
                        if (current == null) {
                            skipped.incrementAndGet();
                            return;
                        }
                        if (delete) {
                            blink.delete(Sized.valueOf(key));
                            expectedValues.put(key, 0L);
                            deletes.incrementAndGet();
                        } else {
                            blink.insert(Sized.valueOf(key), value);
                            if (current == 0L) {
                                inserts.incrementAndGet();
                            } else {
                                updates.incrementAndGet();
                            }
                            expectedValues.put(key, value);
                        }
                    }
                }));

            }
            int progress = 0;
            for (Future f : futures) {
                f.get();
                if (++progress % 10000 == 0) {
                    System.out.println("done " + progress + "/" + ITERATIONS);
                }
            }
            int nulls = 0;
            for (String key : keys) {
                Long value = blink.search(Sized.valueOf(key));
                Long expected = expectedValues.get(key);
                if (expected == 0) {
                    assertNull(value);
                    nulls++;
                } else {
                    assertEquals(expected, value);
                }
            }
            System.out.println("total swapin " + storage.swapIn);
            System.out.println("inserts " + inserts);
            System.out.println("updates " + updates);
            System.out.println("skipped " + skipped);
            System.out.println("deletes " + deletes);
            System.out.println("iterations " + ITERATIONS + " (" + (inserts.intValue() + updates.intValue() + deletes.intValue()+ skipped.intValue()) + ")");
            System.out.println("nulls " + nulls);
            threadpool.shutdown();
        }

    }
}
