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

package herddb.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Result of the write to the commit log
 *
 * @author enrico.olivelli
 */
public final class CommitLogResult {

    public final CompletableFuture<LogSequenceNumber> logSequenceNumber;
    public final boolean deferred;
    public final boolean sync;

    public CommitLogResult(LogSequenceNumber logSequenceNumber, boolean deferred, boolean sync) {
        this.logSequenceNumber = CompletableFuture.completedFuture(logSequenceNumber);
        this.deferred = deferred;
        this.sync = sync;
    }

    public CommitLogResult(CompletableFuture<LogSequenceNumber> logSequenceNumber, boolean deferred, boolean sync) {
        this.logSequenceNumber = logSequenceNumber;
        this.deferred = deferred;
        this.sync = sync;
    }

    public LogSequenceNumber getLogSequenceNumber() throws LogNotAvailableException {
        try {
            return logSequenceNumber.get();
        } catch (ExecutionException err) {
            throw new LogNotAvailableException(err.getCause());
        } catch (InterruptedException err) {
            throw new LogNotAvailableException(err);
        }
    }

}
