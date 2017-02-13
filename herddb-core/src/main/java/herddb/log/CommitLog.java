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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * This is the core write-ahead-log of the system. Every change to data is logged to the CommitLog before beeing applied
 * to memory/storage.
 *
 * @author enrico.olivelli
 */
public abstract class CommitLog implements AutoCloseable {

    /**
     * Log a single entry and returns only when the entry has been safely written to the log
     *
     * @param entry
     * @param synch
     * @return
     * @throws LogNotAvailableException
     */
    public abstract LogSequenceNumber log(LogEntry entry, boolean synch) throws LogNotAvailableException;

    /**
     * Log a batch of entries and returns only when the batch has been safely written to the log. In case of
     * LogNotAvailableException it is not possible to know which entries have been written.
     *
     * @param entries
     * @param synch
     * @return
     * @throws LogNotAvailableException
     */
    public List<LogSequenceNumber> log(List<LogEntry> entries, boolean synch) throws LogNotAvailableException {
        List<LogSequenceNumber> res = new ArrayList<>();
        for (LogEntry entry : entries) {
            res.add(log(entry, false));
        }
        return res;
    }

    public abstract void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing) throws LogNotAvailableException;

    public abstract void followTheLeader(LogSequenceNumber skipPast, BiConsumer<LogSequenceNumber, LogEntry> consumer) throws LogNotAvailableException;

    public abstract LogSequenceNumber getLastSequenceNumber();

    public abstract void startWriting() throws LogNotAvailableException;

    public abstract void clear() throws LogNotAvailableException;

    @Override
    public abstract void close() throws LogNotAvailableException;

    public abstract boolean isClosed();

    public abstract void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException;

    protected CommitLogListener[] listeners = null;

    public synchronized void attachCommitLogListener(CommitLogListener l) {
        if (listeners == null) {
            CommitLogListener[] _listeners = new CommitLogListener[1];
            _listeners[0] = l;
            listeners = _listeners;
        } else {
            CommitLogListener[] _listeners = new CommitLogListener[listeners.length + 1];
            if (listeners.length > 0) {
                System.arraycopy(listeners, 0, _listeners, 0, listeners.length);
            }
            _listeners[_listeners.length - 1] = l;
            listeners = _listeners;
        }
    }

    public synchronized void removeCommitLogListener(CommitLogListener l) {
        CommitLogListener[] _listeners = new CommitLogListener[listeners.length - 1];
        int pos = 0;
        boolean found = false;
        for (int i = 0; i < listeners.length; i++) {
            if (listeners[i] != l) {
                _listeners[pos++] = listeners[i];
            } else {
                found = true;
            }
        }
        if (found) {
            if (_listeners.length == 0) {
                this.listeners = null;
            } else {
                this.listeners = _listeners;
            }
        }
    }

}
