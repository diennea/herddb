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
package herddb.mem;

import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * In Memory CommitLogManager, for tests
 *
 * @author enrico.olivelli
 */
public class MemoryCommitLogManager extends CommitLogManager {

    @Override
    public CommitLog createCommitLog(String tableSpace) {
        return new CommitLog() {

            AtomicLong offset = new AtomicLong(-1);

            @Override
            public LogSequenceNumber log(LogEntry entry, boolean synch) throws LogNotAvailableException {
                // NOOP
                entry.serialize();
                return new LogSequenceNumber(1, offset.incrementAndGet());
            }

            @Override
            public LogSequenceNumber getLastSequenceNumber() {
                return new LogSequenceNumber(1, offset.get());
            }

            private volatile boolean closed;

            @Override
            public void close() throws LogNotAvailableException {
                closed = true;
            }

            @Override
            public boolean isClosed() {
                return closed;
            }

            @Override
            public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing) throws LogNotAvailableException {
            }
            
            @Override
            public void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException {
            }

            @Override
            public void followTheLeader(LogSequenceNumber skipPast, BiConsumer<LogSequenceNumber, LogEntry> consumer) throws LogNotAvailableException {
            }

            @Override
            public void startWriting() throws LogNotAvailableException {
            }

            @Override
            public void clear() throws LogNotAvailableException {
            }

        };
    }

}
