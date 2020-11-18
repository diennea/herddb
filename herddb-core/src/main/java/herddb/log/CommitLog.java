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

import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the core write-ahead-log of the system. Every change to data is
 * logged to the CommitLog before beeing applied to memory/storage.
 *
 * @author enrico.olivelli
 */
public abstract class CommitLog implements AutoCloseable {

    /**
     * Log a single entry and returns only when the entry has been safely
     * written to the log
     *
     * @param entry
     * @param synch
     * @return
     * @throws LogNotAvailableException
     */
    public abstract CommitLogResult log(LogEntry entry, boolean synch) throws LogNotAvailableException;

    public abstract void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing) throws LogNotAvailableException;

    /**
     * Checks if log contains enough data in order to recovery from a given snapshot sequence number.
     *
     * @param snapshotSequenceNumber
     * @return true in case recovery is possible.
     */
    public boolean isRecoveryAvailable(LogSequenceNumber snapshotSequenceNumber) {
        return true;
    }

    public void metadataUpdated(int expectedReplicaCount) {
    }

    public interface FollowerContext extends AutoCloseable {

        @Override
        default void close() {
        }
    }

    public <T extends FollowerContext> T startFollowing(LogSequenceNumber lastPosition) {
        return null;
    }

    public interface EntryAcceptor {
        boolean accept(LogSequenceNumber lsn, LogEntry entry) throws Exception;
    }

    public void followTheLeader(
            LogSequenceNumber skipPast, EntryAcceptor consumer,
            FollowerContext context
    ) throws LogNotAvailableException {
        // useful only on cluster
    }

    public abstract LogSequenceNumber getLastSequenceNumber();

    public abstract void startWriting(int expectedReplicaCount) throws LogNotAvailableException;

    public abstract void clear() throws LogNotAvailableException;

    @Override
    public abstract void close() throws LogNotAvailableException;

    public abstract boolean isFailed();

    public abstract boolean isClosed();

    public abstract void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException;

    protected CommitLogListener[] listeners = null;

    protected synchronized boolean isHasListeners() {
        return listeners != null;
    }

    protected synchronized void notifyListeners(LogSequenceNumber logPos, LogEntry edit) {
        if (listeners != null) {
            for (CommitLogListener l : listeners) {
                LOG.info("notifyListeners {}, {}", new Object[]{logPos, edit});
                l.logEntry(logPos, edit);
            }
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(CommitLog.class.getName());

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
