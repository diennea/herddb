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
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * In Memory CommitLogManager, for tests
 *
 * @author enrico.olivelli
 */
public class MemoryCommitLogManager extends CommitLogManager {

    private final boolean testSerialize;

    public MemoryCommitLogManager() {
        this(true);
    }
    
    public MemoryCommitLogManager(boolean testSerialize) {
        this.testSerialize = testSerialize;
    }

    @Override
    public CommitLog createCommitLog(String tableSpace, String tablespaceName, String localNodeId) {
        return new CommitLog() {

            AtomicLong offset = new AtomicLong(-1);

            @Override
            public CommitLogResult log(LogEntry entry, boolean synch) throws LogNotAvailableException {
                if (isHasListeners()) {
                    synch = true;
                }
                if (testSerialize) {
                     // this is useful only for internal testing
                    // NOOP, but trigger serialization subsystem
                    try {
                        entry.serialize(ExtendedDataOutputStream.NULL);
                    } catch (IOException err) {
                        throw new LogNotAvailableException(err);
                    }
                }
                LogSequenceNumber logPos = new LogSequenceNumber(1, offset.incrementAndGet());
                notifyListeners(logPos, entry);
                return new CommitLogResult(logPos, !synch, synch);
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
            public boolean isFailed() {
                return false;
            }

            @Override
            public boolean isClosed() {
                return closed;
            }

            @Override
            public void recovery(LogSequenceNumber snapshotSequenceNumber,
                                 BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing) throws LogNotAvailableException {
            }

            @Override
            public void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException {
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
