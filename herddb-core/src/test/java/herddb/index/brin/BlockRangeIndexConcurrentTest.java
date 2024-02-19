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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.ClockProPolicy;
import herddb.core.PageReplacementPolicy;
import herddb.core.RandomPageReplacementPolicy;
import herddb.index.brin.BlockRangeIndex.Block;
import herddb.utils.SizeAwareObject;
import herddb.utils.Sized;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

/**
 * Unit tests for PagedBlockRangeIndex
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexConcurrentTest {

    @Test
    public void testConcurrentWrites() throws Exception {

        int testSize = 10000;
        int parallelism = 10;
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(3));
        index.boot(BlockRangeIndexMetadata.empty());

        try {

            ExecutorService threadpool = Executors.newFixedThreadPool(parallelism);
            CountDownLatch l = new CountDownLatch(testSize);

            try {
                for (int i = 0; i < testSize; i++) {
                    int _i = i;
                    threadpool.submit(() -> {
                        try {
                            index.put(Sized.valueOf(_i), Sized.valueOf("a" + _i));
                            l.countDown();
                        } catch (RuntimeException e) {
                            e.printStackTrace();
                        }
                    });
                }
            } finally {
                threadpool.shutdown();
            }

            assertTrue(l.await(10, TimeUnit.SECONDS));

            index.checkpoint();

            dumpIndex(index);
            verifyIndex(index);

            List<Sized<String>> result = index.search(Sized.valueOf(0), Sized.valueOf(testSize + 1));
            for (Sized<String> res : result) {
                System.out.println("res " + res.dummy);
            }

            for (int i = 0; i < testSize; i++) {
                assertTrue("cannot find " + i, index.containsKey(Sized.valueOf(i)));
            }

        } catch (Exception | AssertionError e) {

            deepDumpIndex(index);
            throw e;
        }
    }

    @Test
    public void testConcurrentReadsWritesWithSplits() throws Exception {

        int testSize = 10000;
        int parallelism = 10;
        BlockRangeIndex<Sized<Integer>, Sized<String>> index = new BlockRangeIndex<>(1024,
                new RandomPageReplacementPolicy(3));
        index.boot(BlockRangeIndexMetadata.empty());
        ExecutorService threadpool = Executors.newFixedThreadPool(parallelism);

        try {

            ConcurrentLinkedQueue<Sized<String>> results = new ConcurrentLinkedQueue<>();
            List<Future<?>> jobs = new ArrayList<>(testSize);

            try {
                for (int i = 0; i < testSize; i++) {
                    int _i = i;
                    jobs.add(threadpool.submit(() -> {
                        index.put(Sized.valueOf(_i), Sized.valueOf("a" + _i));
                        List<Sized<String>> search = index.search(Sized.valueOf(_i));
                        results.addAll(search);
                        if (search.isEmpty()) {
                            search = index.search(Sized.valueOf(_i));
                            throw new IllegalStateException("Empty Search! i " + _i);
                        }
                    }));
                }
            } finally {
                threadpool.shutdown();
            }

            for (Future<?> job : jobs) {
                /* Job exception rethrow */
                try {
                    job.get(10, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    throw (Exception) e.getCause();
                }
            }

            index.checkpoint();

            dumpIndex(index);
            verifyIndex(index);
            List<Sized<String>> result = index.search(Sized.valueOf(0), Sized.valueOf(testSize + 1));
            for (Sized<String> res : result) {
                System.out.println("res " + res.dummy);
            }

            for (int i = 0; i < testSize; i++) {
                assertTrue("cannot find " + i, index.containsKey(Sized.valueOf(i)));
                assertTrue("cannot find a" + i, results.contains(Sized.valueOf("a" + i)));
            }

        } catch (Exception | AssertionError e) {

            deepDumpIndex(index);
            throw e;
        }
    }

    @Test
    public void testConcurrentReadsWritesDeletesWithSplits() throws Exception {

        int testSize = 10000;
        int parallelism = 10;
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(3));
        index.boot(BlockRangeIndexMetadata.empty());

        try {

            ExecutorService threadpool = Executors.newFixedThreadPool(parallelism);
            CountDownLatch l = new CountDownLatch(testSize);
            ConcurrentLinkedQueue<Sized<String>> afterInsertSearch = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<Sized<String>> afterDeleteSearch = new ConcurrentLinkedQueue<>();
            List<Future<?>> jobs = new ArrayList<>(testSize);

            try {
                for (int i = 0; i < testSize; i++) {
                    int _i = i;
                    jobs.add(threadpool.submit(() -> {
                        try {
                            index.put(Sized.valueOf(_i), Sized.valueOf("a" + _i));
                            List<Sized<String>> search = index.search(Sized.valueOf(_i));
                            afterInsertSearch.addAll(search);
                            if (search.isEmpty()) {
                                throw new IllegalStateException("Empty Search! i " + _i);
                            }

                            index.delete(Sized.valueOf(_i), Sized.valueOf("a" + _i));
                            List<Sized<String>> search2 = index.search(Sized.valueOf(_i));
                            afterDeleteSearch.addAll(search2);
                            l.countDown();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }));
                }
            } finally {
                threadpool.shutdown();
            }

            for (Future<?> job : jobs) {
                /* Job exception rethrow */
                try {
                    job.get(10, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    throw (Exception) e.getCause();
                }
            }

            index.checkpoint();
            dumpIndex(index);

            verifyIndex(index);
            List<Sized<String>> lookupAfterDeletion = index.search(Sized.valueOf(0), Sized.valueOf(testSize + 1));
            assertTrue(lookupAfterDeletion.isEmpty());

            for (int i = 0; i < testSize; i++) {
                assertTrue("cannot find a" + i, afterInsertSearch.contains(Sized.valueOf("a" + i)));
            }
            assertTrue(afterDeleteSearch.isEmpty());

        } catch (Exception | AssertionError e) {

            deepDumpIndex(index);
            throw e;
        }

    }

    @Test
    public void testConcurrentReadsWritesDeletesWithSplitsOnTwoIndexes() throws Exception {

        int testSize = 10000;
        int parallelism = 10;

        PageReplacementPolicy policy = new ClockProPolicy(3);
        BlockRangeIndex<Sized<Integer>, Sized<String>> checkpointIndex =
                new BlockRangeIndex<>(1024, policy);
        BlockRangeIndex<Sized<Integer>, Sized<String>> otherIndex =
                new BlockRangeIndex<>(1024, policy);
        checkpointIndex.boot(BlockRangeIndexMetadata.empty());
        otherIndex.boot(BlockRangeIndexMetadata.empty());

        int cpreload = 0;
        int opreload = 0;
        while (checkpointIndex.getNumBlocks() < 2) {
            checkpointIndex.put(Sized.valueOf(1), sizedWithPadding("c", 5, cpreload++));
        }
        final int fincpreload = cpreload;
        while (otherIndex.getNumBlocks() < 2) {
            otherIndex.put(Sized.valueOf(1), sizedWithPadding("a", 5, opreload++));
        }

        try {

            ExecutorService threadpool = Executors.newFixedThreadPool(parallelism);
            Future<Void> checkpointingJob;
            List<Future<?>> jobs = new ArrayList<>(testSize);

            AtomicBoolean stopCheckpointingThread = new AtomicBoolean(false);

            try {
                checkpointingJob = threadpool.submit((Callable<Void>) () -> {
                    int i = fincpreload;
                    while (stopCheckpointingThread.get()) {
                        ++i;
                        if (i % 10 == 0) {
                            checkpointIndex.checkpoint();
                        }

                        Sized<Integer> key = Sized.valueOf(1);
                        Sized<String> value = sizedWithPadding("c", 5, i);
                        checkpointIndex.put(key, value);
                        List<Sized<String>> search = checkpointIndex.search(key);
                        if (search.isEmpty()) {
                            throw new IllegalStateException("Empty Search! Checkpointing index i " + 1);
                        }
                        checkpointIndex.delete(key, value);
                    }

                    return null;
                });

                for (int test = opreload; test < testSize + opreload; test++) {
                    int i = test;
                    jobs.add(threadpool.submit((Callable<Void>) () -> {
                        Sized<Integer> key = Sized.valueOf(1);
                        Sized<String> value = sizedWithPadding("a", 5, i);
                        List<Sized<String>> search = otherIndex.search(key);
                        if (search.isEmpty()) {
                            throw new IllegalStateException("Empty Search! i " + 1);
                        }
                        otherIndex.delete(key, value);
                        List<Sized<String>> research = otherIndex.search(key);
                        if (!research.isEmpty()) {
                            if (research.contains(value)) {
                                throw new IllegalStateException("Value not deleted! i " + 1 + " k " + key + " v " + value);
                            }
                        }

                        if (i % 10 == 0) {
                            otherIndex.checkpoint();
                        }
                        return null;
                    }));
                }
            } finally {
                threadpool.shutdown();
            }


            System.out.println("Waiting running jobs");
            int n = 0;
            for (Future<?> job : jobs) {
                /* Job exception rethrow */
                try {
                    job.get(25, TimeUnit.SECONDS);
                } catch (Exception e) {
                    if (e instanceof TimeoutException) {
                        System.out.println("Probable deadlock condition detected");
                    }
                    System.out.println("Running job " + n + " returned in error, terminating all jobs");
                    for (Future<?> termination : jobs) {
                        termination.cancel(true);
                    }
                    stopCheckpointingThread.set(true);
                    checkpointingJob.cancel(true);
                    throw e;
                }
            }

            stopCheckpointingThread.set(true);
            /* Job exception rethrow */
            try {
                checkpointingJob.get(10, TimeUnit.SECONDS);
                System.out.println("Checkpointing job terminated");
            } catch (ExecutionException e) {
                throw (Exception) e.getCause();
            }

            deepDumpIndex(otherIndex);
            System.out.println("Checkpointing indexes");
            checkpointIndex.checkpoint();
            otherIndex.checkpoint();

            List<Sized<String>> otherContent = otherIndex.search(Sized.valueOf(1));
            assertEquals(otherContent.size(), opreload);

            System.out.println("Dumping indexes");
            System.out.println("Dumping checkpointIndex");
            deepDumpIndex(checkpointIndex);
            System.out.println("Dumping otherIndex");
            deepDumpIndex(otherIndex);

            System.out.println("Verifying indexes");
            System.out.println("Verifying checkpointIndex");
            verifyIndex(checkpointIndex);
            System.out.println("Verifying otherIndex");
            verifyIndex(otherIndex);

        } catch (Exception | AssertionError e) {
            deepDumpIndex(checkpointIndex);
            deepDumpIndex(otherIndex);
            throw e;
        }

    }

    private Sized<String> sizedWithPadding(String prefix, int places, int value) {
        StringBuilder builder = new StringBuilder(prefix);
        String string = Integer.toString(value);
        int valuelen = string.length();
        int paddings = places - valuelen;
        for (int i = 0; i < paddings; ++i) {
            builder.append('0');
        }
        builder.append(string);
        return Sized.valueOf(builder.toString());
    }

    private <X extends Comparable<X> & SizeAwareObject> void dumpIndex(BlockRangeIndex<X, ?> index) {
        System.out.println("---DUMP INDEX-----------------------------");
        for (Block<X, ?> block : index.getBlocks().values()) {
            dumpBlock(block, false);
        }
        System.out.println("------------------------------------------");
    }

    private <X extends Comparable<X> & SizeAwareObject> void deepDumpIndex(BlockRangeIndex<X, ?> index) {
        try {
            System.out.println("---DEEP DUMP INDEX------------------------");
            for (Block<X, ?> block : index.getBlocks().values()) {
                System.out.println("---------------------");
                dumpBlock(block, true);
            }
            System.out.println("------------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private <X extends Comparable<X> & SizeAwareObject> void dumpBlock(Block<X, ?> block, boolean deep) {
        System.out.println("BLOCK " + block);
        if (deep) {
            /*
             * Forcefully load the block to check internal data.
             * Do not attempt to unload to avoid locking in deadlock situations
             */
            block.ensureBlockLoadedWithoutUnload();

            block.values.forEach((k, v) -> System.out.println(k + ": " + v));
        }
    }

    private void verifyIndex(BlockRangeIndex<Sized<Integer>, Sized<String>> index) {
        System.out.println("check index");
        Integer lastmax = null;
        for (Block<Sized<Integer>, Sized<String>> b : index.getBlocks().values()) {
            if (b.key == BlockRangeIndex.BlockStartKey.HEAD_KEY) {
                System.out.println("check block " + lastmax + " -> -inf");
            } else {
                System.out.println("check block " + lastmax + " -> " + b.key.minKey.dummy);
            }

            /* Forcefully load the block to check internal data */
            b.ensureBlockLoadedWithoutUnload();

            if (b.values.isEmpty()) {
                if (index.getBlocks().size() != 1 && b.key != BlockRangeIndex.BlockStartKey.HEAD_KEY) {
                    System.out.println("non head of degenerate tree empty block: " + b);
                    dumpBlock(b, true);
                    fail("non head of degenerate tree empty block " + b);
                }
            } else {
                if (lastmax == null) {
                    lastmax = b.values.lastKey().dummy;
                } else {
                    Integer entryMin = b.values.firstKey().dummy;
                    Integer entryMax = b.values.lastKey().dummy;
                    if (entryMin < lastmax) {
                        fail(entryMin + " < " + lastmax);
                    }
                    lastmax = entryMax;
                }
            }
        }
    }

}
