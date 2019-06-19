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
package herddb.index;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;

/**
 * Base test suite for {@link KeyToPageIndex}
 *
 * @author diego.salvi
 */
public abstract class KeyToPageIndexTest {

    abstract KeyToPageIndex createIndex();


    @Test
    public void getUsedMemory() {

        try (KeyToPageIndex index = createIndex()) {

            final Bytes key = Bytes.from_int(1);

            index.start(LogSequenceNumber.START_OF_TIME);

            Assert.assertEquals(0, index.getUsedMemory());

            /* Test put */
            index.put(key, 1L);

            Assert.assertTrue(index.getUsedMemory() > 0);

            /* Test remove after put */
            index.remove(key);

            Assert.assertEquals(0, index.getUsedMemory());


            /* Test put if */
            index.put(key, 1L, null);

            Assert.assertTrue(index.getUsedMemory() > 0);

            /* Test remove after put if */
            index.remove(key);

            Assert.assertEquals(0, index.getUsedMemory());

        }

    }

    @Test
    public void size() {

        try (KeyToPageIndex index = createIndex()) {

            final Bytes key = Bytes.from_int(1);

            index.start(LogSequenceNumber.START_OF_TIME);

            Assert.assertEquals(0, index.size());

            /* Test put */
            index.put(key, 1L);

            Assert.assertEquals(1, index.size());

            /* Test remove after put */
            index.remove(key);

            Assert.assertEquals(0, index.size());


            /* Test put if */
            index.put(key, 1L, null);

            Assert.assertEquals(1, index.size());

            /* Test remove after put if */
            index.remove(key);

            Assert.assertEquals(0, index.size());

        }

    }

    /** This test failed with the error
     * <pre>
     * herddb.storage.DataStorageManagerException: pages are immutable
     * at herddb.mem.MemoryDataStorageManager.writeIndexPage(MemoryDataStorageManager.java:298)
     * at herddb.index.blink.BLinkKeyToPageIndex$BLinkIndexDataStorageImpl.createPage(BLinkKeyToPageIndex.java:620)
     * at herddb.index.blink.BLinkKeyToPageIndex$BLinkIndexDataStorageImpl.overwriteNodePage(BLinkKeyToPageIndex.java:605)
     * at herddb.index.blink.BLink$Node.writeNodePage(BLink.java:2661)
     * at herddb.index.blink.BLink$Node.writePage(BLink.java:2646)
     * at herddb.index.blink.BLink$Node.flush(BLink.java:2541)
     * at herddb.index.blink.BLink$Node.unload(BLink.java:2493)
     * at herddb.index.blink.BLink.unload(BLink.java:342)
     * at herddb.index.blink.BLink.normalize(BLink.java:894)
     * at herddb.index.blink.BLink.insert(BLink.java:620)</pre>
     *
     * At least BLink must handles pages overwrite
     */
    @Test
    public void massivePut() {

        int entries = 100000;

        try (KeyToPageIndex index = createIndex()) {

            index.start(LogSequenceNumber.START_OF_TIME);

            for(int i = 0; i < entries; ++i) {
                index.put(Bytes.from_int(i), 1L);
            }

            for(int i = 0; i < entries; ++i) {
                Assert.assertEquals(index.get(Bytes.from_int(i)),Long.valueOf(1L));
            }
        }

    }

    @Test
    public void putIf() {

        int entries = 100;

        try (KeyToPageIndex index = createIndex()) {

            index.start(LogSequenceNumber.START_OF_TIME);

            for(int i = 0; i < entries; ++i) {
                index.put(Bytes.from_int(i), 1L);
            }

            for(int i = 0; i < entries; ++i) {
                Assert.assertTrue(index.put(Bytes.from_int(i), 2L, 1L));
            }

            for(int i = 0; i < entries; ++i) {
                Assert.assertFalse(index.put(Bytes.from_int(i), 3L, 1L));
                index.put(Bytes.from_int(i), 3L, 1L);
            }
        }
    }

    @Test
    public void concurrentPutIf() throws InterruptedException, ExecutionException, TimeoutException {

        int jobs = 10000;
        int parallelJobs = 10;

        ExecutorService executor = Executors.newFixedThreadPool(parallelJobs * ConcurrentPutIfTask.THREADS_PER_JOB);

        try (KeyToPageIndex index = createIndex()) {

            index.start(LogSequenceNumber.START_OF_TIME);

            for (int i = 0; i < jobs; ++i) {
                index.put(Bytes.from_int(i), 0L);
            }

            CompletableFuture<?>[] futures = new CompletableFuture<?>[jobs];

            for (int i = 0; i < jobs; ++i) {
                futures[i] = ConcurrentPutIfTask.submitJob(executor, index, Bytes.from_int(i), 1L, 0L);
            }

            CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);

        } finally {

            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.DAYS);
        }

    }

    private static final class ConcurrentPutIfTask implements Callable<Long> {

        public static final CompletableFuture<?> submitJob(ExecutorService service, KeyToPageIndex index, Bytes key, Long newPage, Long expectedPage) {

            final ConcurrentPutIfTask[] tasks = createTasks(index, key, newPage, expectedPage);

            @SuppressWarnings("unchecked")
            final CompletableFuture<Long>[] futures = new CompletableFuture[tasks.length];

            for (int i = 0; i < tasks.length; ++i) {
                futures[i] = CompletableFuture.supplyAsync(tasks[i]::call, service);
            }

            return CompletableFuture.allOf(futures);

        }

        private static final ConcurrentPutIfTask[] createTasks(KeyToPageIndex index, Bytes key, Long newPage, Long expectedPage) {

            CyclicBarrier start = new CyclicBarrier(3);
            CyclicBarrier end = new CyclicBarrier(3);
            AtomicInteger counter = new AtomicInteger(0);

            return new ConcurrentPutIfTask[] {
                    new ConcurrentPutIfTask(index, key, newPage, expectedPage, start, end, counter),
                    new ConcurrentPutIfTask(index, key, newPage, expectedPage, start, end, counter),
                    new ConcurrentPutIfTask(index, key, newPage, expectedPage, start, end, counter)
                    };

        }

        public static final int THREADS_PER_JOB = 3;


        private final KeyToPageIndex index;

        private final Bytes key;
        private final Long newPage;
        private final Long expectedPage;

        private final CyclicBarrier start;
        private final CyclicBarrier end;
        private final AtomicInteger counter;

        public ConcurrentPutIfTask(KeyToPageIndex index, Bytes key, Long newPage, Long expectedPage, CyclicBarrier start, CyclicBarrier end, AtomicInteger counter) {
            super();
            this.index = index;
            this.key = key;
            this.newPage = newPage;
            this.expectedPage = expectedPage;
            this.start = start;
            this.end = end;
            this.counter = counter;
        }

        @Override
        public Long call() throws RuntimeException {

            awaitOnBarrier(start);

            boolean put = index.put(key, newPage, expectedPage);

            if (put) {
                counter.incrementAndGet();
            }

            awaitOnBarrier(end);

            Long page = index.get(key);

            Assert.assertEquals("Wrong page in PK index", newPage, page);
            Assert.assertEquals("Expected just one modification in PK index", 1, counter.get());

            return page;

        }

        private void awaitOnBarrier(CyclicBarrier barrier) {
            try {
                barrier.await(10, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                System.out.println(key.to_int() + " " + e);
                e.printStackTrace();
                Assert.fail("Too many time blocked on waiting");
            } catch (InterruptedException e) {
                System.out.println(key.to_int() + " " + e);
                e.printStackTrace();
                /* Interrupting */
                return;
            } catch (BrokenBarrierException e) {
                System.out.println(key.to_int() + " " + e);
                e.printStackTrace();
                Assert.fail("Barrier broken while waiting");
            }
        }

    }

}
