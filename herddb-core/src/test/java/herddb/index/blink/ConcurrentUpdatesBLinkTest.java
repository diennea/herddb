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
import herddb.utils.Bytes;
import herddb.utils.LocalLockManager;
import herddb.utils.LockHandle;
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
import org.junit.Test;

/**
 * Tests on concurcurrent massive updates on different keys
 */
public class ConcurrentUpdatesBLinkTest {

    @Test
    public void concurrentUpdatesTests() throws Exception {

        int DATA_SIZE = 50000;
        int THREADS = 100;
        int KEY_SIZE = 25;
        int ITERATIONS = 100000;
        BLinkTest.DummyBLinkIndexDataStorage<Sized<String>, Long> storage = new BLinkTest.DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<String>, Long> blink = new BLink<>(2048L, new BLinkTest.StringSizeEvaluator(), new RandomPageReplacementPolicy(3), storage)) {

            Random random = new Random();
            RandomString rs = new RandomString(random);
            ConcurrentHashMap<String, Long> expectedValues = new ConcurrentHashMap<>();
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < DATA_SIZE; ++i) {
                String key = rs.nextString(KEY_SIZE);
                keys.add(key);
                long value = random.nextLong();
                blink.insert(Sized.valueOf(key), value);
                expectedValues.put(key, value);
            }
            LocalLockManager locksManager = new LocalLockManager();
            int numKeys = keys.size();
            ExecutorService threadpool = Executors.newFixedThreadPool(THREADS);
            System.out.println("generated " + numKeys + " keys");
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < ITERATIONS; i++) {
                String key = keys.get(random.nextInt(numKeys));
                long value = random.nextLong();
                futures.add(threadpool.submit(new Runnable() {
                    @Override
                    public void run() {
                        Bytes _key = Bytes.from_string(key);
                        LockHandle lock = locksManager.acquireWriteLockForKey(_key);
                        try {
                            blink.insert(Sized.valueOf(key), value);
                        } finally {
                            locksManager.releaseWriteLockForKey(_key, lock);
                        }
                    }
                }));

            }
            for (Future f : futures) {
                f.get();
            }
            for (String key : keys) {
                Long value = blink.search(Sized.valueOf(key));
                Long expected = expectedValues.get(key);
                assertEquals(expected, value);
            }
            System.out.println("total swapin " + storage.swapIn);
            threadpool.shutdown();
        }

    }
}
