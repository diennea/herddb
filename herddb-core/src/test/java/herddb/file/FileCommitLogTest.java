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
package herddb.file;

import static herddb.file.FileCommitLog.ENTRY_START;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.log.CommitLog;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.utils.TestUtils;

/**
 * Basic Tests on FileCommitLog
 *
 * @author enrico.olivelli
 */
public class FileCommitLogTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testLog() throws Exception {
        try (FileCommitLogManager manager = new FileCommitLogManager(folder.newFolder().toPath(), 64 * 1024 * 1024);) {
            manager.start();
            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.startWriting();
                for (int i = 0; i < 10_000; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), false);
                    writeCount++;
                }
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");
        }
    }

    @Test
    public void testDiskFullLogMissingFooter() throws Exception {
        try (FileCommitLogManager manager = new FileCommitLogManager(folder.newFolder().toPath(), 64 * 1024 * 1024)) {
            manager.start();
            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.startWriting();
                for (int i = 0; i < 100; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), true).getLogSequenceNumber();
                    writeCount++;
                }
                FileCommitLog fileCommitLog = (FileCommitLog) log;

                // simulate end of disk
                byte[] dummyEntry = LogEntryFactory.beginTransaction(0).serialize();
                // header
                fileCommitLog.getWriter().out.write(ENTRY_START);
                fileCommitLog.getWriter().out.writeLong(0);
                // entry
                fileCommitLog.getWriter().out.write(dummyEntry);
                // missing entry footer
                fileCommitLog.getWriter().out.flush();

            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");

            // must be able to read twice
            AtomicInteger readCount2 = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount2.incrementAndGet();
                    }
                }, true);
            }
            assertEquals(writeCount, readCount.get());
        }
    }

    @Test
    public void testDiskFullLogBrokenEntry() throws Exception {
        try (FileCommitLogManager manager = new FileCommitLogManager(folder.newFolder().toPath(), 64 * 1024 * 1024)) {
            manager.start();
            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.startWriting();
                for (int i = 0; i < 100; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), true).getLogSequenceNumber();
                    writeCount++;
                }
                FileCommitLog fileCommitLog = (FileCommitLog) log;

                // simulate end of disk
                byte[] dummyEntry = LogEntryFactory.beginTransaction(0).serialize();
                // header
                fileCommitLog.getWriter().out.write(ENTRY_START);
                fileCommitLog.getWriter().out.writeLong(0);
                // just half entry
                fileCommitLog.getWriter().out.write(dummyEntry, 0, dummyEntry.length / 2);
                // missing entry footer
                fileCommitLog.getWriter().out.flush();

            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");

            // must be able to read twice
            AtomicInteger readCount2 = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount2.incrementAndGet();
                    }
                }, true);
            }
            assertEquals(writeCount, readCount.get());
        }
    }

    @Test
    public void testLogsynch() throws Exception {
        try (FileCommitLogManager manager = new FileCommitLogManager(folder.newFolder().toPath(), 64 * 1024 * 1024);) {
            manager.start();
            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.startWriting();
                for (int i = 0; i < 100; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), true);
                    writeCount++;
                }
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");
        }
    }

    @Test
    public void testLogMultiFiles() throws Exception {
        try (FileCommitLogManager manager = new FileCommitLogManager(folder.newFolder().toPath(), 1024);) {
            manager.start();

            int writeCount = 0;
            final long _startWrite = System.currentTimeMillis();
            try (FileCommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.startWriting();
                for (int i = 0; i < 10_000; i++) {
                    log.log(LogEntryFactory.beginTransaction(0), false);
                    writeCount++;
                }
                TestUtils.waitForCondition(() -> {
                    int qsize = log.getQueueSize();
                    return qsize == 0;
                }, TestUtils.NOOP, 100);
            }
            final long _endWrite = System.currentTimeMillis();
            AtomicInteger readCount = new AtomicInteger();
            try (CommitLog log = manager.createCommitLog("tt", "aa", "nodeid");) {
                log.recovery(LogSequenceNumber.START_OF_TIME, new BiConsumer<LogSequenceNumber, LogEntry>() {
                    @Override
                    public void accept(LogSequenceNumber t, LogEntry u) {
                        readCount.incrementAndGet();
                    }
                }, true);
            }
            final long _endRead = System.currentTimeMillis();
            assertEquals(writeCount, readCount.get());
            System.out.println("Write time: " + (_endWrite - _startWrite) + " ms");
            System.out.println("Read time: " + (_endRead - _endWrite) + " ms");
        }
    }

}
