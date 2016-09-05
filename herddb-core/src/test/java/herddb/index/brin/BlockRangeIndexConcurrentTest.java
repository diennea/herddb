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

import herddb.index.brin.BlockRangeIndex.Block;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
        verifyIndex(index);
        List<String> result = index.lookUpRange(0, testSize + 1);
        for (String res : result) {
            System.out.println("res " + res);
        }

        for (int i = 0; i < testSize; i++) {
            assertTrue("cannot find " + i, index.containsKey(i));
        }

    }

    private void verifyIndex(BlockRangeIndex<Integer, String> index) {
        Integer lastmax = null;
        for (Block b : index.getBlocks().values()) {
            System.out.println("check block " + lastmax + " -> " + b.minKey + "," + b.maxKey);
            if (lastmax == null) {
                lastmax = (Integer) b.maxKey;
            } else {
                Integer entryMin = (Integer) b.minKey;
                Integer entryMax = (Integer) b.maxKey;
                if (entryMin < lastmax) {
                    fail(entryMin + " < " + lastmax);
                }
                lastmax = entryMax;
            }
        }
    }

    @Test
    public void testConcurrentReadsWritesWithSplits() throws Exception {
        int testSize = 1000;
        int parallelism = 6;
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(10);
        ExecutorService threadpool = Executors.newFixedThreadPool(parallelism);
        CountDownLatch l = new CountDownLatch(testSize);
        ConcurrentLinkedQueue<String> results = new ConcurrentLinkedQueue<>();
        try {
            for (int i = 0; i < testSize; i++) {
                int _i = i;
                threadpool.submit(() -> {
                    index.put(_i, "a" + _i);
                    List<String> search = index.search(_i);
                    results.addAll(search);
                    l.countDown();
                });
            }
        } finally {
            threadpool.shutdown();
        }
        l.await(10, TimeUnit.SECONDS);
        index.dump();
        verifyIndex(index);
        List<String> result = index.lookUpRange(0, testSize + 1);
        for (String res : result) {
            System.out.println("res " + res);
        }

        for (int i = 0; i < testSize; i++) {
            assertTrue("cannot find " + i, index.containsKey(i));
            assertTrue("cannot find a" + i, results.contains("a" + i));
        }

    }

    @Test
    public void testConcurrentReadsWritesDeletesWithSplits() throws Exception {
        int testSize = 1000;
        int parallelism = 6;
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(10);
        ExecutorService threadpool = Executors.newFixedThreadPool(parallelism);
        CountDownLatch l = new CountDownLatch(testSize);
        ConcurrentLinkedQueue<String> results = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> results2 = new ConcurrentLinkedQueue<>();
        try {
            for (int i = 0; i < testSize; i++) {
                int _i = i;
                threadpool.submit(() -> {
                    index.put(_i, "a" + _i);
                    List<String> search = index.search(_i);
                    results.addAll(search);
                    l.countDown();
                    index.delete(_i, "a" + _i);
                    List<String> search2 = index.search(_i);
                    results2.addAll(search2);
                });
            }
        } finally {
            threadpool.shutdown();
        }
        l.await(10, TimeUnit.SECONDS);
        index.dump();
        verifyIndex(index);
        List<String> result = index.lookUpRange(0, testSize + 1);
        assertTrue(result.isEmpty());

        for (int i = 0; i < testSize; i++) {
            assertTrue("cannot find a" + i, results.contains("a" + i));
        }
        assertTrue(results2.isEmpty());
        assertTrue(index.getBlocks().isEmpty());

    }

}
