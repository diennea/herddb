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

import herddb.core.MemoryManager;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BlinkRandomBench {

    public static void main(String[] args) {

        ByteArrayOutputStream oos = new ByteArrayOutputStream();
        ByteArrayOutputStream eos = new ByteArrayOutputStream();

        PrintStream oout = System.out;
        PrintStream oerr = System.err;

        // int maxruns = 1000000;
        // int report = maxruns / 1000;

        // int maxruns = 100000;
        // int report = maxruns / 1000;

        // int maxruns = 1000;
        // int report = maxruns / 100;

        int maxruns = 100;
        int report = maxruns / 100;

        // int maxruns = 100;
        // int report = maxruns / 10;

        // int maxruns = 1;
        // int report = 1;

        // int threads = 5;
        // long maxID = 8;

        // int threads = 6;
        // long maxID = 10;

        // int threads = 16;
        // long maxID = 20;

        // int threads = 20;
        // long maxID = 2000;

        // int threads = 20;
        // long maxID = 20000;

        // int threads = 20;
        // long maxID = 200000;

        int threads = 20;
        long maxID = 200000;

        // int threads = 1;
        // long maxID = 50;

        // int threads = 25;
        // long maxID = 50;

        long minID = 1;

        boolean error = false;

        int count = 0;

        boolean success = false;

        long startw, endw;
        long startr, endr;

        long timew = 0;
        long timer = 0;

        long timewrep = 0;
        long timerrep = 0;

        try {
            while (!error) {

                // oos = new ByteArrayOutputStream();
                PrintStream ops = new PrintStream(oos);
                System.setOut(ops);

                // eos = new ByteArrayOutputStream();
                PrintStream eps = new PrintStream(eos);
                System.setErr(eps);

                MemoryManager mem = new MemoryManager(5 * (1L << 20), 10 * (128L << 10), (128L << 10));
                // MemoryManager mem = new MemoryManager(5 * (1L << 20), (128L << 10), (128L << 10));

                try (MemoryDataStorageManager ds = new MemoryDataStorageManager();
                     BLinkKeyToPageIndex idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds)) {

                    idx.start(LogSequenceNumber.START_OF_TIME);

                    long[] data = new long[(int) (maxID - minID)];

                    for (long i = 0; i < (maxID - minID); ++i) {
                        data[(int) i] = i + minID;
                    }

                    Collections.shuffle(Arrays.asList(data));

                    long[][] datas = new long[threads][];

                    int added = 0;
                    for (int i = 0; i < threads; ++i) {

                        int toadd = (data.length - added) / (threads - i);

                        long[] tdata = new long[toadd];
                        System.arraycopy(data, added, tdata, 0, toadd);
                        added += toadd;

                        datas[i] = tdata;
                    }

                    ExecutorService ex = Executors.newFixedThreadPool(threads);

                    CyclicBarrier barrier = new CyclicBarrier(threads);

                    startw = System.currentTimeMillis();

                    List<Future<?>> futures = new ArrayList<>();
                    for (int i = 0; i < threads; ++i) {

                        final int index = i;
                        Future<?> f = ex.submit(() -> {

                            try {
                                barrier.await();
                            } catch (InterruptedException | BrokenBarrierException e) {
                                e.printStackTrace(System.out);
                            }

                            for (int k = 0; k < datas[index].length; ++k) {
                                idx.put(Bytes.from_long(datas[index][k]), datas[index][k]);
                            }
                        });

                        futures.add(f);
                    }

                    for (Future<?> f : futures) {
                        try {
                            f.get();
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace(System.out);
                            error = true;
                        }
                    }

                    endw = System.currentTimeMillis();

//                  System.out.println(idx.tree.toStringFull());

                    startr = System.currentTimeMillis();

                    if (!error) {

                        futures.clear();

                        for (int i = 0; i < threads; ++i) {

                            final int index = i;
                            Future<?> f = ex.submit(() -> {

                                try {
                                    barrier.await();
                                } catch (InterruptedException | BrokenBarrierException e) {
                                    e.printStackTrace(System.out);
                                }

                                for (int k = 0; k < datas[index].length; ++k) {
                                    long r = idx.get(Bytes.from_long(datas[index][k]));

                                    if (r != datas[index][k]) {
                                        System.out.println(datas[index][k]);

                                        throw new RuntimeException("Search Error! " + datas[index][k]);
                                    }
                                }
                            });

                            futures.add(f);
                        }


                        for (Future<?> f : futures) {
                            try {
                                f.get();
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace(System.out);
                                error = true;
                            }
                        }

                    }

                    endr = System.currentTimeMillis();

                    ex.shutdown();

                    try {
                        ex.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        e.printStackTrace(System.out);
                    }

                    long write = endw - startw;
                    long read = endr - startr;

                    timew += write;
                    timer += read;

                    timewrep += write;
                    timerrep += read;

                    if (++count % report == 0) {

                        { /* write */

                            double operations = (count * (maxID - minID + 1));
                            double msPerOp = timew / operations;
                            double opPerMs = operations / timew;

                            double operationswi = (report * (maxID - minID + 1));
                            double msPerOpwi = timewrep / operationswi;
                            double opPerMswi = operationswi / timewrep;

                            double perc = count * 100 / (double) maxruns;
                            oout.println("W [" + String.format("%3.2f", perc) + "%] " + String.format("%7d", count)
                                    + " time: " + String.format("%7d", timew) + " (" + timewrep + ")" + " op/ms: "
                                    + String.format("%3.2f", opPerMs) + " (instant " + String.format("%3.2f", opPerMswi)
                                    + ")" + " ms/op: " + String.format("%3.2f", msPerOp) + " (instant "
                                    + String.format("%3.2f", msPerOpwi) + ")");
                        }

                        { /* read */

                            double operations = (count * (maxID - minID + 1));
                            double msPerOp = timer / operations;
                            double opPerMs = operations / timer;

                            double operationsri = (report * (maxID - minID + 1));
                            double msPerOpri = timerrep / operationsri;
                            double opPerMsri = operationsri / timerrep;

                            double perc = count * 100 / (double) maxruns;
                            oout.println("R [" + String.format("%3.2f", perc) + "%] " + String.format("%7d", count)
                                    + " time: " + String.format("%7d", timer) + " (" + timerrep + ")" + " op/ms: "
                                    + String.format("%3.2f", opPerMs) + " (instant " + String.format("%3.2f", opPerMsri)
                                    + ")" + " ms/op: " + String.format("%3.2f", msPerOp) + " (instant "
                                    + String.format("%3.2f", msPerOpri) + ")");
                        }

                        timewrep = 0;
                        timerrep = 0;

                        // oout.print(blink.toStringFull());
                    }

                    oout.flush();

                    if (count == maxruns) {
                        success = true;
                        return;
                    }

                    System.out.flush();
                    System.err.flush();

                    ops.close();
                    eps.close();

                    ops = null;
                    eps = null;

                    if (!error) {
                        oos.reset();
                        eos.reset();
                    }

                }

            }

        } finally {
            System.out.flush();
            System.setOut(oout);

            System.err.flush();
            System.setErr(oerr);

            if (!success) {
                try {
                    System.out.println("OS S " + oos.size());
                    String os = oos.toString("UTF-8");
                    System.out.println(os);

                    System.out.println("ES S " + eos.size());
                    String es = eos.toString("UTF-8");
                    System.err.println(es);
                } catch (UnsupportedEncodingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }

    }

}
