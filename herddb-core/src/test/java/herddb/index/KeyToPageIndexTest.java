package herddb.index;

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

            CyclicBarrier barrier = new CyclicBarrier(3);
            CyclicBarrier barrier2 = new CyclicBarrier(3);
            AtomicInteger counter = new AtomicInteger(0);

            return new ConcurrentPutIfTask[] {
                    new ConcurrentPutIfTask(index, key, newPage, expectedPage, barrier, barrier2, counter),
                    new ConcurrentPutIfTask(index, key, newPage, expectedPage, barrier, barrier2, counter),
                    new ConcurrentPutIfTask(index, key, newPage, expectedPage, barrier, barrier2, counter)
                    };

        }

        public static final int THREADS_PER_JOB = 3;


        private final KeyToPageIndex index;

        private final Bytes key;
        private final Long newPage;
        private final Long expectedPage;

        private final CyclicBarrier barrier;
        private final CyclicBarrier barrier2;
        private final AtomicInteger counter;

        public ConcurrentPutIfTask(KeyToPageIndex index, Bytes key, Long newPage, Long expectedPage, CyclicBarrier barrier, CyclicBarrier barrier2, AtomicInteger counter) {
            super();
            this.index = index;
            this.key = key;
            this.newPage = newPage;
            this.expectedPage = expectedPage;
            this.barrier = barrier;
            this.barrier2 = barrier2;
            this.counter = counter;
        }

        @Override
        public Long call() throws RuntimeException {

            awaitOnBarrier(barrier);

            boolean put = index.put(key, newPage, expectedPage);

            if (put) {
                counter.incrementAndGet();
            }

            awaitOnBarrier(barrier2);

            Long page = index.get(key);

            Assert.assertEquals("Wrong page in PK index", newPage, page);
            Assert.assertEquals("Expected just one modification in PK index", 1, counter.get());

            return page;

//            return 0L;

        }
//
//        private void awaitOnBarrier() {
//            try {
//                barrier.await(10, TimeUnit.SECONDS);
//            } catch (TimeoutException e) {
//                System.out.println(key.to_int() + " " + e);
//                e.printStackTrace();
//                fail("Too many time blocked on waiting");
//            } catch (InterruptedException e) {
//                System.out.println(key.to_int() + " " + e);
//                e.printStackTrace();
//                /* Interrupting */
//                return;
//            } catch (BrokenBarrierException e) {
//                System.out.println(key.to_int() + " " + e);
//                e.printStackTrace();
//                fail("Barrier broken while waiting");
//            }
//        }

        private void awaitOnBarrier(CyclicBarrier barrier) throws RuntimeException {
            try {
                barrier.await();
//                barrier.await(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
//                System.out.println(key.to_int() + " " + e);
//                e.printStackTrace();
//                fail("Too many time blocked on waiting");
//            } catch (InterruptedException e) {
//                System.out.println(key.to_int() + " " + e);
//                e.printStackTrace();
//                /* Interrupting */
//                return;
//            } catch (BrokenBarrierException e) {
//                System.out.println(key.to_int() + " " + e);
//                e.printStackTrace();
//                fail("Barrier broken while waiting");
//            }
        }

    }

}
