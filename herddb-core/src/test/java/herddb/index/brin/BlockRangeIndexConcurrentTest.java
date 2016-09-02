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
package herddb.index.brin;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Unit tests for BlockRangeIndex
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexConcurrentTest {

    @Test
    public void testConcurrentWrites() throws Exception {
        int testSize = 1000;
        int parallelism = 6;
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(10);
        ExecutorService threadpool = Executors.newFixedThreadPool(parallelism);
        CountDownLatch l = new CountDownLatch(testSize);
        try {
            for (int i = 0; i < testSize; i++) {
                int _i = i;
                threadpool.submit(() -> {
                    index.put(_i, "a" + _i);
                    l.countDown();
                });
            }
        } finally {
            threadpool.shutdown();
        }
        l.await(10, TimeUnit.SECONDS);
        index.dump();
        List<String> result = index.lookUpRange(0, testSize + 1);
        for (String res : result) {
            System.out.println("res " + res);
        }

        for (int i = 0; i < testSize; i++) {
            assertTrue("cannot find " + i, index.containsKey(i));
        }

    }

}
