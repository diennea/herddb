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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

import org.junit.Assert;
import org.junit.Test;

import herddb.core.ClockProPolicy;
import herddb.index.blink.BLink.SizeEvaluator;

/**
 * Tests on concurcurrent checkpoint
 */
public class ConcurrentCheckpointBLinkTest {

    static final class LongSizeEvaluator implements SizeEvaluator<Long, Long> {

        public static final SizeEvaluator<Long, Long> INSTANCE = new LongSizeEvaluator();

        @Override
        public long evaluateKey(Long key) {
            return Long.BYTES;
        }

        @Override
        public long evaluateValue(Long value) {
            return Long.BYTES;
        }

        @Override
        public long evaluateAll(Long key, Long value) {
            return evaluateKey(key) + evaluateValue(value);
        }

        private static final Long INFINITY = Long.valueOf(Long.MAX_VALUE);

        @Override
        public Long getPosiviveInfinityKey() {
            return INFINITY;
        }

    }

    private static final class Reporter {

        private final long intervalMS;

        private long start = -1L;
        private long nextReport = -1L;

        public Reporter(long interval, TimeUnit unit) {
            super();
            this.intervalMS = unit.toMillis(interval);
        }

        public void start() {
            start = System.currentTimeMillis();
            nextReport = start + intervalMS;
        }

        public Timings report() {

            long now = System.currentTimeMillis();

            if (now >= nextReport) {
                long elapsed = now  - (nextReport - intervalMS);
                nextReport = now + intervalMS;

                return new Timings(now - start, elapsed);
            }

            return null;

        }

    }

    public static final class Timings {

        private final long totalTime;
        private final long reportTime;

        public Timings(long totalTime, long reportTime) {
            super();
            this.totalTime = totalTime;
            this.reportTime = reportTime;
        }

        public long getTotalTime() {
            return totalTime;
        }
        public long getReportTime() {
            return reportTime;
        }

    }

    /** Insert new data booking "blocks" */
    private static final class Writer implements Callable<Long> {

        private final BLink<Long, Long> blink;
        private final boolean debug;
        private final AtomicLong nextKeyBlock;
        private final long keyBlocks;
        private final long keyBlockSize;
        private final long startKey;
        private final BlockingQueue<Long> insertedBlocks;
        private final StampedLock checkpointLock;
        private final CyclicBarrier startBarrier;

        private final Reporter reporter;

        public Writer(BLink<Long, Long> blink, boolean debug, AtomicLong nextKeyBlock, long keyBlocks, long keyBlockSize,
                      long startKey, BlockingQueue<Long> insertedBlocks, StampedLock checkpointLock,
                      CyclicBarrier startBarrier) {
            super();
            this.blink = blink;
            this.debug = debug;
            this.nextKeyBlock = nextKeyBlock;
            this.keyBlocks = keyBlocks;
            this.keyBlockSize = keyBlockSize;
            this.startKey = startKey;
            this.insertedBlocks = insertedBlocks;
            this.checkpointLock = checkpointLock;
            this.startBarrier = startBarrier;
            this.reporter = new Reporter(1, TimeUnit.SECONDS);
        }

        @Override
        public Long call() throws Exception {

            /* Await other thread startup */
            startBarrier.await(10, TimeUnit.SECONDS);

            long tid = Thread.currentThread().getId();

            long inserted = 0;
            long keyBlock;

            reporter.start();

            try {
                while ((keyBlock = nextKeyBlock.getAndIncrement()) < keyBlocks) {

                    long startBlockKey = keyBlock * keyBlockSize + startKey;
                    long maxBlockKey = startBlockKey + keyBlockSize;

                    if (debug) {
                        System.out.printf("[TID %3d] Write   from %3d to %3d block %3d\n", tid, startBlockKey, maxBlockKey, keyBlock);
                    }

                    for(long key = startBlockKey; key < maxBlockKey; ++key) {

                        long stamp = checkpointLock.readLockInterruptibly();
                        try {

                            Long lkey = Long.valueOf(key);
                            blink.insert(lkey, lkey);

                            ++inserted;

                            Timings timings = reporter.report();
                            if (timings != null) {
                                System.out.printf("[TID %3d] Inserted     %8d in %7d ms (%4d ms)\n", tid, inserted, timings.getTotalTime(), timings.getReportTime());
                            }

                            if (Thread.interrupted()) {
                                throw new InterruptedException();
                            }

                        } finally {
                            checkpointLock.unlockRead(stamp);
                        }
                    }

                    insertedBlocks.add(keyBlock);

                }

            } catch (InterruptedException e) {

                return inserted;

            } catch (Exception e) {

                /*
                 * Fast print exception (still show the exception even if the future.get won't be invoked due to
                 * some deadlock)
                 */
                e.printStackTrace();
                throw e;

            }

            return inserted;
        }

    }

    /** Delete data "blocks" */
    private static final class Deleter implements Callable<Long> {

        private final BLink<Long, Long> blink;
        private final boolean debug;
        private final AtomicLong deletingBlocks;
        private final AtomicLong deletedBlocks;
        private final long keyBlocks;
        private final long keyBlockSize;
        private final long startKey;
        private final BlockingQueue<Long> insertedBlocks;
        private final StampedLock checkpointLock;
        private final CyclicBarrier startBarrier;

        private final Reporter reporter;

        public Deleter(BLink<Long, Long> blink, boolean debug, AtomicLong deletingBlocks, AtomicLong deletedBlocks, long keyBlocks,
                       long keyBlockSize, long startKey, BlockingQueue<Long> insertedBlocks, StampedLock checkpointLock,
                       CyclicBarrier startBarrier) {
            super();
            this.blink = blink;
            this.debug = debug;
            this.deletingBlocks = deletingBlocks;
            this.deletedBlocks = deletedBlocks;
            this.keyBlocks = keyBlocks;
            this.keyBlockSize = keyBlockSize;
            this.startKey = startKey;
            this.insertedBlocks = insertedBlocks;
            this.checkpointLock = checkpointLock;
            this.startBarrier = startBarrier;
            this.reporter = new Reporter(1, TimeUnit.SECONDS);
        }

        @Override
        public Long call() throws Exception {

            /* Await other thread startup */
            startBarrier.await(10, TimeUnit.SECONDS);

            long tid = Thread.currentThread().getId();

            long deleted = 0;

            reporter.start();

            try {

                while(deletingBlocks.getAndIncrement() < keyBlocks) {

                    Long keyBlock = insertedBlocks.take();

                    long startBlockKey = keyBlock * keyBlockSize + startKey;
                    long maxBlockKey = startBlockKey + keyBlockSize;

                    if (debug) {
                        System.out.printf("[TID %3d] Delete  from %3d to %3d block %3d\n", tid, startBlockKey, maxBlockKey, keyBlock);
                    }

                    for(long key = startBlockKey; key < maxBlockKey; ++key) {

                        long stamp = checkpointLock.readLockInterruptibly();
                        try {

                            blink.delete(Long.valueOf(key));

                            ++deleted;

                            Timings timings = reporter.report();
                            if (timings != null) {
                                System.out.printf("[TID %3d] Deleted      %8d in %7d ms (%4d ms)\n", tid, deleted, timings.getTotalTime(), timings.getReportTime());
                            }

                            if (Thread.interrupted()) {
                                throw new InterruptedException();
                            }

                        } finally {
                            checkpointLock.unlockRead(stamp);
                        }
                    }

                    deletedBlocks.incrementAndGet();
                }

            } catch (InterruptedException e) {

                return deleted;

            } catch (Exception e) {

                /*
                 * Fast print exception (still show the exception even if the future.get won't be invoked due to
                 * some deadlock)
                 */
                e.printStackTrace();
                throw e;

            }


            return deleted;
        }

    }

    /** Read data */
    private static final class Reader implements Callable<Long> {

        private final BLink<Long, Long> blink;
        private final boolean debug;
        private final AtomicBoolean stop;
        private final AtomicLong insertedBlocks;
        private final AtomicLong deletedBlocks;
        private final long keyBlockSize;
        private final long startKey;
        private final CyclicBarrier startBarrier;

        private final Reporter reporter;

        public Reader(BLink<Long, Long> blink, boolean debug, AtomicBoolean stop, AtomicLong insertedBlocks, AtomicLong deletedBlocks, long keyBlockSize, long startKey, CyclicBarrier startBarrier) {
            super();
            this.blink = blink;
            this.debug = debug;
            this.stop = stop;
            this.insertedBlocks = insertedBlocks;
            this.deletedBlocks = deletedBlocks;
            this.keyBlockSize = keyBlockSize;
            this.startKey = startKey;
            this.startBarrier = startBarrier;
            this.reporter = new Reporter(1, TimeUnit.SECONDS);
        }

        @Override
        public Long call() throws Exception {

            /* Await other thread startup */
            startBarrier.await(10, TimeUnit.SECONDS);

            long tid = Thread.currentThread().getId();

            long searches = 0;

            reporter.start();

            try {

                while(!stop.get()) {

                    long maxInsertedBlock = insertedBlocks.get();
                    long countDeletedBlock = deletedBlocks.get();

                    long endSearchKey = maxInsertedBlock * keyBlockSize + startKey;
                    long startSearchKey = countDeletedBlock * keyBlockSize + startKey;


                    if (endSearchKey != startSearchKey) {

                        if (debug) {
                            System.out.printf("[TID %3d] Read    from %3d to %3d max inserted block %3d deleted blocks %3d\n", tid, startSearchKey, endSearchKey, maxInsertedBlock, countDeletedBlock);
                        }

                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }

                        for (long key = endSearchKey - 1L; key >= startSearchKey; --key) {

                            Long lkey = Long.valueOf(key);
                            Long found = blink.search(lkey);

                            if (found != null) {
                                Assert.assertEquals(lkey, found);
                            }

                            ++searches;

                            Timings timings = reporter.report();
                            if (timings != null) {
                                System.out.printf("[TID %3d] Read         %8d in %7d ms (%4d ms)\n", tid, searches, timings.getTotalTime(), timings.getReportTime());
                            }

                        }
                    }

                    if (Thread.interrupted()) {
                        return searches;
                    }

                }

            } catch (Exception e) {

                /*
                 * Fast print exception (still show the exception even if the future.get won't be invoked due to
                 * some deadlock)
                 */
                e.printStackTrace();
                throw e;

            }

            return searches;
        }

    }

    /** Checkpoint BLink */
    private static final class Checkpointer implements Callable<Long> {

        private final BLink<Long, Long> blink;
        private final boolean debug;
        private final AtomicBoolean stop;
        private final StampedLock checkpointLock;
        private final CyclicBarrier startBarrier;

        private final Reporter reporter;

        public Checkpointer(BLink<Long, Long> blink, boolean debug, AtomicBoolean stop, StampedLock checkpointLock, CyclicBarrier startBarrier) {
            super();
            this.blink = blink;
            this.debug = debug;
            this.stop = stop;
            this.checkpointLock = checkpointLock;
            this.startBarrier = startBarrier;
            this.reporter = new Reporter(1, TimeUnit.SECONDS);
        }

        @Override
        public Long call() throws Exception {

            /* Await other thread startup */
            startBarrier.await(10, TimeUnit.SECONDS);

            long tid = Thread.currentThread().getId();

            long checkpoints = 0;
            long checkStart;

            reporter.start();

            while(!stop.get()) {

                try {

                    long stamp = checkpointLock.writeLockInterruptibly();
                    try {

                        if (debug) {
                            System.out.printf("[TID %3d] Checkpoint   %3d\n", tid, checkpoints);
                        }
                        checkStart = System.currentTimeMillis();

                        blink.checkpoint();
                        ++checkpoints;

                        if (debug) {
                            System.out.printf("[TID %3d] Checkpoint   %3d in %3d ms\n", tid, checkpoints, System.currentTimeMillis() - checkStart);
                        }

                        Timings timings = reporter.report();
                        if (timings != null) {
                            System.out.printf("[TID %3d] Checkpointed %8d in %7d ms (%4d ms)\n", tid, checkpoints, timings.getTotalTime(), timings.getReportTime());
                        }

                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }

                    } finally {
                        checkpointLock.unlockWrite(stamp);
                    }

                    Thread.yield();

                } catch (InterruptedException e) {

                    return checkpoints;

                } catch (Exception e) {

                    /*
                     * Fast print exception (still show the exception even if the future.get won't be invoked due to
                     * some deadlock)
                     */
                    e.printStackTrace();
                    throw e;

                }
            }

            return checkpoints;
        }

    }

    /** Execute a checkpoint on an index with many empty nodes */
    @Test
    public void concurrentReadWithManyEmptyNodes() throws Exception {

        int concurrentReaders = 4;
        int concurrentWriters = 1;
        int concurrentDeleters = 2;

        boolean debugCheckpointers = false;
        boolean debugReaders = false;
        boolean debugWriters = false;
        boolean debugDeleters = false;

        long minKey = 0;
        long maxKey = 25000;

        long blockKeySize = 100;


        Assert.assertTrue(minKey < maxKey);
        Assert.assertTrue(maxKey < LongSizeEvaluator.INSTANCE.getPosiviveInfinityKey());

        long insertableKeys = maxKey - minKey;

        /* Check that all blocks have equal size */
        Assert.assertEquals(0, insertableKeys % blockKeySize);

        long keyBlocks = insertableKeys / blockKeySize;

        /* Check there are enough blocks  */
        Assert.assertTrue(keyBlocks >= concurrentWriters);
        Assert.assertTrue(keyBlocks >= concurrentDeleters);


        ExecutorService executor = null;
        BLinkTest.DummyBLinkIndexDataStorage<Long, Long> storage = new BLinkTest.DummyBLinkIndexDataStorage<>();
        try (BLink<Long, Long> blink = new BLink<>(2048L, LongSizeEvaluator.INSTANCE, new ClockProPolicy(30), storage)) {

            AtomicLong nextKeyBlock = new AtomicLong(0);
            AtomicLong deletingKeyBlock = new AtomicLong(0);
            AtomicLong deletedKeyBlock = new AtomicLong(0);
            AtomicBoolean stop = new AtomicBoolean(false);

            int threads = concurrentReaders + concurrentWriters + concurrentDeleters + 1 /* checkpoint */;

            executor = Executors.newFixedThreadPool(threads);

            StampedLock checkpointLock = new StampedLock();
            CyclicBarrier startBarrier = new CyclicBarrier(threads + 1);
            BlockingQueue<Long> insertedBlocks = new LinkedBlockingQueue<>();

            Future<Long> checkpoint = executor.submit(new Checkpointer(blink, debugCheckpointers, stop, checkpointLock, startBarrier));

            List<Future<Long>> writes = new ArrayList<>();
            for (int i = 0; i < concurrentWriters; ++i) {
                writes.add(executor.submit(new Writer(blink, debugWriters, nextKeyBlock, keyBlocks, blockKeySize, minKey,
                        insertedBlocks, checkpointLock, startBarrier)));
            }

            List<Future<Long>> deletes = new ArrayList<>();
            for (int i = 0; i < concurrentDeleters; ++i) {
                deletes.add(executor.submit(new Deleter(blink, debugDeleters, deletingKeyBlock, deletedKeyBlock, keyBlocks,
                        blockKeySize, minKey, insertedBlocks, checkpointLock, startBarrier)));
            }

            List<Future<Long>> reads = new ArrayList<>();
            for (int i = 0; i < concurrentReaders; ++i) {
                reads.add(executor.submit(
                        new Reader(blink, debugReaders, stop, nextKeyBlock, deletedKeyBlock, blockKeySize, minKey, startBarrier)));
            }

            int nwrites = 0;
            int ndeletes = 0;
            int nreads = 0;

            try {

                /* Await other thread startup */
                startBarrier.await(10, TimeUnit.SECONDS);

                for (Future<Long> f : writes) {
                    nwrites += f.get();
                }

                for (Future<Long> f : deletes) {
                    ndeletes += f.get();
                }

            } finally {
                stop.set(true);
            }


            for (Future<Long> f : reads) {
                nreads += f.get();
            }

            long nckeckpoints = checkpoint.get();

            System.out.println("Swapins:     " + storage.swapIn);
            System.out.println("Inserts:     " + nwrites);
            System.out.println("Reads:       " + nreads);
            System.out.println("Deletes:     " + ndeletes);
            System.out.println("Checkpoints: " + nckeckpoints);

        } finally {

            if (executor != null) {
                executor.shutdownNow();
            }

        }

    }
}
