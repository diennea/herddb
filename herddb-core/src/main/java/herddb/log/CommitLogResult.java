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
import java.util.concurrent.Future;

/**
 * Result of the write to the commit log
 *
 * @author enrico.olivelli
 */
public final class CommitLogResult {

    public final Future<LogSequenceNumber> logSequenceNumber;
    public final boolean deferred;

    public CommitLogResult(LogSequenceNumber logSequenceNumber, boolean deferred) {
        this.logSequenceNumber = CompletableFuture.completedFuture(logSequenceNumber);
        this.deferred = deferred;
    }

    public CommitLogResult(Future<LogSequenceNumber> logSequenceNumber, boolean deferred) {
        this.logSequenceNumber = logSequenceNumber;
        this.deferred = deferred;
    }

    public LogSequenceNumber getLogSequenceNumber() throws LogNotAvailableException {
        try {
            return logSequenceNumber.get();
        } catch (ExecutionException | InterruptedException err) {
            throw new LogNotAvailableException(err);
        }
    }

    public void synch() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
