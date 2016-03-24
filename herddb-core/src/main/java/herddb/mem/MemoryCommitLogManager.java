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
import herddb.log.SequenceNumber;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In Memory CommitLogManager, for tests
 *
 * @author enrico.olivelli
 */
public class MemoryCommitLogManager extends CommitLogManager {

    @Override
    public CommitLog createCommitLog(String tableSpace) {
        return new CommitLog() {

            AtomicLong offset = new AtomicLong();

            @Override
            public void log(LogEntry entry) throws LogNotAvailableException {
                // NOOP
                offset.incrementAndGet();
            }

            @Override
            public void log(List<LogEntry> entries) throws LogNotAvailableException {
                // NOOP
                offset.addAndGet(entries.size());
            }

            @Override
            public SequenceNumber getActualSequenceNumber() {
                return new SequenceNumber(1, offset.get());
            }

        };
    }

}
