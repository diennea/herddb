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

import herddb.log.CommitLog;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

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
        FileCommitLogManager manager = new FileCommitLogManager(folder.getRoot().toPath());
        int writeCount = 0;
        final long _startWrite = System.currentTimeMillis();
        try (CommitLog log = manager.createCommitLog("tt");) {
            log.startWriting();
            for (int i = 0; i < 1_000_000; i++) {
                log.log(LogEntryFactory.beginTransaction("tt", 0), false);
                writeCount++;
            }
        }
        final long _endWrite = System.currentTimeMillis();
        AtomicInteger readCount = new AtomicInteger();
        try (CommitLog log = manager.createCommitLog("tt");) {
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
