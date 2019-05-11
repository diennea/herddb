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
package herddb.cluster;

import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import herddb.log.CommitLog;
import herddb.log.CommitLogResult;
import herddb.log.FullRecoveryNeededException;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.utils.EnsureLongIncrementAccumulator;
import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.client.BKException.BKClientClosedException;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;

/**
 * Commit log replicated on Apache Bookkeeper
 *
 * @author enrico.olivelli
 */
public class BookkeeperCommitLog extends CommitLog {

    private static final Logger LOGGER = Logger.getLogger(BookkeeperCommitLog.class.getName());
    // Max number for entry to read while tailing
    private static final int MAX_ENTRY_TO_TAIL = 5000;
    // Max time to wait for an entry to arrive
    private static final int LONG_POLL_TIMEOUT = 1000;
    private final String sharedSecret = "herddb";
    private final BookKeeper bookKeeper;
    private final BookkeeperCommitLogManager parent;
    private final ZookeeperMetadataStorageManager metadataManager;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final String tableSpaceUUID;
    private final String tableSpaceName; // only for logging
    private final String localNodeId; // only for logging
    private volatile long lastApplicationWriteTs = 0;
    private volatile CommitFileWriter writer;
    private volatile long currentLedgerId = 0;
    private volatile long lastLedgerId = -1;
    private final AtomicLong lastSequenceNumber = new AtomicLong(-1);
    private LedgersInfo actualLedgersList;
    private int ensemble = 1;
    private int writeQuorumSize = 1;
    private int ackQuorumSize = 1;
    private long ledgersRetentionPeriod = 1000 * 60 * 60 * 24;
    private long maxIdleTime = 0;

    private volatile boolean closed = false;
    private volatile boolean failed = false;

    public LedgersInfo getActualLedgersList() {
        return actualLedgersList;
    }

    private void signalLogFailed() {
        failed = true;
    }

    public void rollNewLedger() {
        openNewLedger();
    }

    void forceLastAddConfirmed() {
        if (maxIdleTime <= 0 || closed) {
            return;
        }
        long _lastWriteTs = lastApplicationWriteTs;
        long idleTime = System.currentTimeMillis() - _lastWriteTs;
        if (_lastWriteTs > 0 && idleTime > maxIdleTime) {
            CommitFileWriter _writer = writer;
            if (_writer != null) {
                _writer.writeNoop();
            }
        }
    }

    private class CommitFileWriter implements AutoCloseable {

        private volatile LedgerHandle out;
        private final long ledgerId;
        private volatile boolean errorOccurredDuringWrite;

        private CommitFileWriter() throws LogNotAvailableException {
            try {
                Map<String, byte[]> metadata = new HashMap<>();
                metadata.put("tablespaceuuid", tableSpaceUUID.getBytes(StandardCharsets.UTF_8));
                metadata.put("tablespacename", tableSpaceName.getBytes(StandardCharsets.UTF_8));
                metadata.put("leader", localNodeId.getBytes(StandardCharsets.UTF_8));
                this.out = bookKeeper.createLedger(ensemble, writeQuorumSize, ackQuorumSize,
                        BookKeeper.DigestType.CRC32C, sharedSecret.getBytes(StandardCharsets.UTF_8), metadata);
                this.ledgerId = this.out.getId();
                lastLedgerId = ledgerId;
                lastSequenceNumber.set(-1);
            } catch (InterruptedException | BKException err) {
                throw new LogNotAvailableException(err);
            }
        }

        public long getLedgerId() {
            return ledgerId;
        }

        public CompletableFuture<LogSequenceNumber> writeEntry(LogEntry edit) {
            // BK will release the buffer after handling the entry
            ByteBuf serialize = edit.serializeAsByteBuf();
            CompletableFuture<LogSequenceNumber> res = new CompletableFuture<>();
            this.out.asyncAddEntry(serialize, (int rc, LedgerHandle lh, long offset, Object o) -> {
                if (rc == BKException.Code.OK) {
                    if (edit.type != LogEntryType.NOOP) { // do not take into account NOOPs
                        lastApplicationWriteTs = System.currentTimeMillis();
                    }
                    res.complete(new LogSequenceNumber(lh.getId(), offset));
                } else {
                    errorOccurredDuringWrite = true;
                    res.completeExceptionally(new LogNotAvailableException(BKException.create(rc)));
                }
            }, null);
            return res;
        }

        @Override
        public void close() throws LogNotAvailableException {

            LedgerHandle _out = out;
            if (_out != null) {
                try {
                    LOGGER.log(Level.INFO, "Closing ledger " + _out.getId()
                            + ", with LastAddConfirmed=" + _out.getLastAddConfirmed()
                            + ", LastAddPushed=" + _out.getLastAddPushed()
                            + " length=" + _out.getLength()
                            + ", errorOccurred:" + errorOccurredDuringWrite);

                    _out.close();
                } catch (InterruptedException | BKException err) {
                    throw new LogNotAvailableException(err);
                } finally {
                    out = null;
                }
            }
        }

        private void writeNoop() {
            // write a dummy entry, this will force LastAddConfirmed to be piggybacked
            try {
                log(LogEntryFactory.noop(), false);
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "error", t);
            }
        }

    }

    public BookkeeperCommitLog(String tableSpaceUUID, String tableSpaceName, String localNodeId,
            ZookeeperMetadataStorageManager metadataStorageManager, BookKeeper bookkeeper, BookkeeperCommitLogManager parent) throws LogNotAvailableException {
        this.metadataManager = metadataStorageManager;
        this.tableSpaceUUID = tableSpaceUUID;
        this.tableSpaceName = tableSpaceName;
        this.localNodeId = localNodeId;
        this.bookKeeper = bookkeeper;
        this.parent = parent;
    }

    public long getLastLedgerId() {
        return lastLedgerId;
    }

    public int getEnsemble() {
        return ensemble;
    }

    public void setEnsemble(int ensemble) {
        this.ensemble = ensemble;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public void setWriteQuorumSize(int writeQuorumSize) {
        this.writeQuorumSize = writeQuorumSize;
    }

    public int getAckQuorumSize() {
        return ackQuorumSize;
    }

    public void setAckQuorumSize(int ackQuorumSize) {
        this.ackQuorumSize = ackQuorumSize;
    }

    public long getLedgersRetentionPeriod() {
        return ledgersRetentionPeriod;
    }

    public void setLedgersRetentionPeriod(long ledgersRetentionPeriod) {
        this.ledgersRetentionPeriod = ledgersRetentionPeriod;
    }

    public long getMaxIdleTime() {
        return maxIdleTime;
    }

    public void setMaxIdleTime(long maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    @Override
    public CommitLogResult log(LogEntry edit, boolean sync) throws LogNotAvailableException {
        CommitFileWriter _writer = writer;
        CompletableFuture<LogSequenceNumber> res;
        if (closed || _writer == null) {
            res = new CompletableFuture();
            res.completeExceptionally(
                    new LogNotAvailableException(new Exception("this commitlog has been closed for tablespace " + tableSpaceDescription()))
                            .fillInStackTrace());
        } else {
            res = _writer.writeEntry(edit);
            res.whenCompleteAsync((pos, error) -> {
                if (error != null) {
                    handleBookKeeperAsyncFailure(error, edit);
                }
            }
            );
            res.thenAccept((pos) -> {
                if (lastLedgerId == pos.ledgerId) {
                    lastSequenceNumber.accumulateAndGet(pos.offset,
                            EnsureLongIncrementAccumulator.INSTANCE);
                }
                notifyListeners(pos, edit);
            }
            );
        }
        if (!sync) {
            // client is not really interested to the result of the write
            // sending a fake completed result
            return new CommitLogResult(
                    CompletableFuture.<LogSequenceNumber>completedFuture(null), true /* deferred */, false);
        } else {
            return new CommitLogResult(res, false /* deferred */, true);
        }
    }

    private String tableSpaceDescription() {
        return this.tableSpaceName + " (" + this.tableSpaceUUID + ")";
    }

    private void handleBookKeeperAsyncFailure(Throwable cause, LogEntry edit) {
        if (cause.getCause() != null && cause instanceof LogNotAvailableException) {
            cause = cause.getCause();
        }
        LOGGER.log(Level.SEVERE, "bookkeeper async failure on tablespace " + tableSpaceDescription() + " while writing entry " + edit, cause);
        if (cause instanceof BKException.BKLedgerClosedException) {
            LOGGER.log(Level.SEVERE, "ledger has been closed, need to open a new ledger for tablespace " + tableSpaceDescription(), closed);
        } else if (cause instanceof BKException.BKLedgerFencedException) {
            LOGGER.log(Level.SEVERE, "this server was fenced for tablespace " + tableSpaceDescription() + " !", cause);
            close();
            signalLogFailed();
        } else if (cause instanceof BKException.BKNotEnoughBookiesException) {
            LOGGER.log(Level.SEVERE, "bookkeeper failure for tablespace " + tableSpaceDescription(), cause);
            close();
            signalLogFailed();
        }
    }

    private void openNewLedger() throws LogNotAvailableException {
        lock.writeLock().lock();
        try {
            closeCurrentWriter();
            writer = new CommitFileWriter();
            currentLedgerId = writer.getLedgerId();
            LOGGER.log(Level.INFO, "Tablespace {1}, opened new ledger:{0}", new Object[]{currentLedgerId, tableSpaceDescription()});
            if (actualLedgersList.getFirstLedger() < 0) {
                actualLedgersList.setFirstLedger(currentLedgerId);
            }
            actualLedgersList.addLedger(currentLedgerId);
            metadataManager.saveActualLedgersList(tableSpaceUUID, actualLedgersList);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing) throws LogNotAvailableException {
        String tableSpaceDescription = tableSpaceDescription();
        this.actualLedgersList = metadataManager.getActualLedgersList(tableSpaceUUID);
        LOGGER.log(Level.INFO, "Actual ledgers list:{0} tableSpace {1}", new Object[]{actualLedgersList, tableSpaceDescription});
        this.lastLedgerId = snapshotSequenceNumber.ledgerId;
        this.currentLedgerId = snapshotSequenceNumber.ledgerId;
        this.lastSequenceNumber.set(snapshotSequenceNumber.offset);
        LOGGER.log(Level.INFO, "recovery from latest snapshotSequenceNumber:{0} tableSpace {1}, node {2}, fencing {3}", new Object[]{snapshotSequenceNumber, tableSpaceDescription, localNodeId, fencing});
        if (currentLedgerId > 0 && !this.actualLedgersList.getActiveLedgers().contains(currentLedgerId) && !this.actualLedgersList.getActiveLedgers().isEmpty()) {
            // TODO: download snapshot from another remote broker
            throw new FullRecoveryNeededException(new Exception("Actual ledgers list does not include latest snapshot ledgerid:" + currentLedgerId + " tablespace " + tableSpaceDescription));
        }
        if (snapshotSequenceNumber.isStartOfTime() && !this.actualLedgersList.getActiveLedgers().isEmpty() && !this.actualLedgersList.getActiveLedgers().contains(this.actualLedgersList.getFirstLedger())) {
            throw new FullRecoveryNeededException(new Exception("Tablespace " + tableSpaceDescription + ": Local data is absent, and actual ledger list " + this.actualLedgersList.getActiveLedgers() + " does not contain first ledger of ever: " + this.actualLedgersList.getFirstLedger()));
        }
        try {
            for (long ledgerId : actualLedgersList.getActiveLedgers()) {

                if (ledgerId < snapshotSequenceNumber.ledgerId) {
                    LOGGER.log(Level.FINER, "Skipping ledger {0}", ledgerId);
                    continue;
                }
                LedgerHandle handle;
                if (fencing) {
                    handle = bookKeeper.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, sharedSecret.getBytes(StandardCharsets.UTF_8));
                } else {
                    handle = bookKeeper.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32C, sharedSecret.getBytes(StandardCharsets.UTF_8));
                }
                try {
                    long first;
                    if (ledgerId == snapshotSequenceNumber.ledgerId) {
                        first = snapshotSequenceNumber.offset;
                        if (first == -1) {
                            // this can happen if checkpoint  happened while starting to follow a new ledger but actually no entry was ever read
                            LOGGER.log(Level.INFO, "Tablespace " + tableSpaceDescription + ", recovering from latest snapshot ledger " + ledgerId + ", first entry " + first + " is not valid. Adjusting to 0");
                            first = 0;
                        }
                        LOGGER.log(Level.FINE, "Tablespace " + tableSpaceDescription + ", recovering from latest snapshot ledger " + ledgerId + ", starting from entry " + first);
                    } else {
                        first = 0;
                        LOGGER.log(Level.FINE, "Tablespace " + tableSpaceDescription + ", recovering from ledger " + ledgerId + ", starting from entry " + first);
                    }
                    long lastAddConfirmed = handle.getLastAddConfirmed();
                    LOGGER.log(Level.INFO, "Tablespace " + tableSpaceDescription + ", Recovering from ledger " + ledgerId + ", first=" + first + " lastAddConfirmed=" + lastAddConfirmed);

                    final int BATCH_SIZE = 10000;
                    if (lastAddConfirmed >= 0) {

                        for (long b = first; b <= lastAddConfirmed;) {
                            long start = b;
                            long end = b + BATCH_SIZE;
                            if (end > lastAddConfirmed) {
                                end = lastAddConfirmed;
                            }
                            b = end + 1;
                            double percent = ((start - first) * 100.0 / (lastAddConfirmed + 1));
                            int entriesToRead = (int) (1 + end - start);
                            LOGGER.log(Level.FINE, "From entry {0}, to entry {1} ({2} %)", new Object[]{start, end, percent, tableSpaceDescription});
                            long _start = System.currentTimeMillis();

                            Enumeration<LedgerEntry> entries = handle.readEntries(start, end);
                            int localEntryCount = 0;
                            while (entries.hasMoreElements()) {

                                LedgerEntry entry = entries.nextElement();
                                long entryId = entry.getEntryId();
                                LogSequenceNumber number = new LogSequenceNumber(ledgerId, entryId);
                                LogEntry statusEdit = LogEntry.deserialize(entry.getEntry());
                                lastLedgerId = ledgerId;
                                currentLedgerId = ledgerId;
                                lastSequenceNumber.set(entryId);
                                if (number.after(snapshotSequenceNumber)) {
                                    LOGGER.log(Level.FINEST, "RECOVER ENTRY #" + localEntryCount + " {0}, {1}", new Object[]{number, statusEdit});
                                    consumer.accept(number, statusEdit);
                                } else {
                                    LOGGER.log(Level.FINEST, "SKIP ENTRY #" + localEntryCount + " {0}<{1}, {2}", new Object[]{number, snapshotSequenceNumber, statusEdit});
                                }
                                localEntryCount++;
                            }
                            LOGGER.log(Level.FINER, "read " + localEntryCount + " entries from ledger " + ledgerId + ", expected " + entriesToRead);

                            LOGGER.log(Level.FINER, "finished waiting for " + entriesToRead + " entries to be read from ledger " + ledgerId);
                            if (localEntryCount != entriesToRead) {
                                throw new LogNotAvailableException("Read " + localEntryCount + " entries, expected " + entriesToRead);
                            }
                            lastLedgerId = ledgerId;
                            lastSequenceNumber.set(end);
                            long _stop = System.currentTimeMillis();
                            LOGGER.log(Level.INFO, "{4} From entry {0}, to entry {1} ({2} %) read time {3}", new Object[]{start, end, percent, (_stop - _start) + " ms", tableSpaceDescription});
                        }
                    }
                } finally {
                    handle.close();
                }
            }
            LOGGER.log(Level.INFO, "After recovery of {0} lastSequenceNumber {1}", new Object[]{tableSpaceDescription, getLastSequenceNumber()});
        } catch (InterruptedException | EOFException | RuntimeException | BKException err) {
            LOGGER.log(Level.SEVERE, "Fatal error during recovery of " + tableSpaceDescription, err);
            signalLogFailed();
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void startWriting() throws LogNotAvailableException {
        actualLedgersList = metadataManager.getActualLedgersList(tableSpaceUUID);
        openNewLedger();
    }

    @Override
    public void clear() throws LogNotAvailableException {
        this.currentLedgerId = 0;
        metadataManager.saveActualLedgersList(tableSpaceUUID, new LedgersInfo());
    }

    @Override
    public void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException {
        if (ledgersRetentionPeriod <= 0) {
            return;
        }
        LOGGER.log(Level.INFO, "dropOldLedgers lastCheckPointSequenceNumber: {0}, ledgersRetentionPeriod: {1} ,lastLedgerId: {2}, currentLedgerId: {3}, tablespace {4}, actualLedgersList {5}",
                new Object[]{lastCheckPointSequenceNumber, ledgersRetentionPeriod, lastLedgerId, currentLedgerId, tableSpaceDescription(), actualLedgersList});
        long min_timestamp = System.currentTimeMillis() - ledgersRetentionPeriod;
        List<Long> oldLedgers = actualLedgersList.getOldLedgers(min_timestamp);
        LOGGER.log(Level.INFO, "dropOldLedgers currentLedgerId: {0}, lastLedgerId: {1}, dropping ledgers before {2}: {3} tablespace {4}",
                new Object[]{currentLedgerId, lastLedgerId, new java.sql.Timestamp(min_timestamp), oldLedgers, tableSpaceDescription()});
        oldLedgers.remove(this.currentLedgerId);
        oldLedgers.remove(this.lastLedgerId);
        if (oldLedgers.isEmpty()) {
            LOGGER.log(Level.INFO, "dropOldLedgers no ledger to drop now, tablespace {0}",
                    new Object[]{tableSpaceDescription()});
            return;
        }
        for (long ledgerId : oldLedgers) {
            try {
                LOGGER.log(Level.INFO, "dropping ledger {0}, tablespace {1}", new Object[]{ledgerId, tableSpaceDescription()});
                actualLedgersList.removeLedger(ledgerId);
                try {
                    bookKeeper.deleteLedger(ledgerId);
                } catch (BKException.BKNoSuchLedgerExistsException error) {
                    LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId + " for tablespace " + tableSpaceDescription(), error);
                }
                metadataManager.saveActualLedgersList(tableSpaceUUID, actualLedgersList);
                LOGGER.log(Level.INFO, "dropping ledger {0}, finished, tablespace {1}", new Object[]{ledgerId, tableSpaceDescription()});
            } catch (BKException | InterruptedException error) {
                LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId + " for tablespace " + tableSpaceDescription(), error);
                throw new LogNotAvailableException(error);
            }
        }
    }

    @Override
    public final void close() {
        parent.releaseLog(this.tableSpaceUUID);
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closeCurrentWriter();
            closed = true;
            LOGGER.severe("closed");
        } finally {
            writer = null;
            lock.writeLock().unlock();
        }

    }

    private void closeCurrentWriter() {
        if (writer != null) {
            try {
                writer.close();
            } catch (LogNotAvailableException err) {
                LOGGER.log(Level.SEVERE, "error while closing ledger during write", err);
            } finally {
                writer = null;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isFailed() {
        return failed;
    }

    private final class BKFollowerContext implements FollowerContext {

        volatile ReadHandle currentLedger;
        volatile long nextEntryToRead;
        volatile long ledgerToTail;

        BKFollowerContext(LogSequenceNumber lastPosition) {
            ledgerToTail = lastPosition.ledgerId;
            nextEntryToRead = lastPosition.offset + 1;
            LOGGER.info(tableSpaceDescription() + " start following, fist position is " + lastPosition);
        }

        void ensureOpenReader(LogSequenceNumber currentPosition) throws org.apache.bookkeeper.client.api.BKException,
                InterruptedException, LogNotAvailableException {
            if (LOGGER.isLoggable(Level.FINER)) {
                LOGGER.finer(tableSpaceDescription() + " seekToNextLedger currentPosition=" + currentPosition + " ledgerToTail " + ledgerToTail);
            }
            // we are alreading tailing the good ledger, but it may not be the same of "currentPosition"
            if (currentLedger != null && currentLedger.getId() == ledgerToTail) {
                if (currentPosition.ledgerId == ledgerToTail) {
                    nextEntryToRead = currentPosition.offset + 1;
                } else {
                    // not the same ledger of currentPosition, need to read from 0
                    nextEntryToRead = 0;
                }
                if (currentLedger.getLastAddConfirmed() < nextEntryToRead) {
                    // let's see if there is new data on the ledger
                    // tryReadLastAddConfirmed will perform and RPC to bookie
                    // to read the current LAC
                    currentLedger.tryReadLastAddConfirmed();
                }

                if (currentLedger.isClosed() && currentLedger.getLastAddConfirmed()
                        == nextEntryToRead - 1) {
                    if (LOGGER.isLoggable(Level.FINER)) {
                        LOGGER.finer(tableSpaceDescription() + " ledger " + currentLedger.getId() + " is closed and we have fully read it");
                    }
                } else {
                    if (LOGGER.isLoggable(Level.FINER)) {
                        LOGGER.finer(tableSpaceDescription() + " seekToNextLedger keep current handle " + currentLedger.getId() + " nextEntry " + nextEntryToRead);
                    }
                    return;
                }
            }
            // current ledgers list
            List<Long> actualList = metadataManager.getActualLedgersList(tableSpaceUUID).getActiveLedgers();
            Collections.sort(actualList);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine(tableSpaceDescription() + " ledgers list " + actualList);
            }
            if (actualList.isEmpty()) {
                LOGGER.severe(tableSpaceDescription() + " no ledger in list?");
                return;
            }
            if (ledgerToTail == -1) {
                // we are not booting from a snapshot, start from the start of time
                ledgerToTail = actualList.get(0);
            }
            if (!actualList.contains(ledgerToTail)) {
                throw new LogNotAvailableException(tableSpaceDescription() + " First Ledger to open " + ledgerToTail + ", is not in the activer ledgers list " + actualList);
            }
            // no ledger opened, we are booting
            if (currentLedger == null) {
                currentLedger = bookKeeper.openLedgerNoRecovery(ledgerToTail,
                        BookKeeper.DigestType.CRC32C, sharedSecret.getBytes(StandardCharsets.UTF_8));
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine(tableSpaceDescription() + " opened direct ledger " + ledgerToTail);
                }
                nextEntryToRead = currentPosition.offset + 1;
                return;
            }

            // find the next ledger in the list
            long nextLedger = -1;
            for (long lId : actualList) {
                if (lId > ledgerToTail) {
                    ledgerToTail = lId;
                    break;
                }
            }
            if (nextLedger == -1) {
                // no more ledger to tail by now
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.finer(tableSpaceDescription() + " no more ledgers after " + ledgerToTail);
                }
                currentLedger = null;
                return;
            }

            ledgerToTail = nextLedger;
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine(tableSpaceDescription() + " start tailing " + ledgerToTail);
            }
            if (currentLedger != null) {
                currentLedger.close();
            }
            currentLedger = bookKeeper.openLedgerNoRecovery(ledgerToTail,
                    BookKeeper.DigestType.CRC32C, sharedSecret.getBytes(StandardCharsets.UTF_8));
            nextEntryToRead = 0;
        }

        @Override
        public void close() {
            if (currentLedger != null) {
                try {
                    currentLedger.close();
                } catch (org.apache.bookkeeper.client.api.BKException ex) {
                    // not really a problem
                    LOGGER.log(Level.FINE, null, ex);
                } catch (InterruptedException ex) {
                    // not really a problem
                    Thread.currentThread().interrupt();
                } finally {
                    currentLedger = null;
                }
            }
        }

    }

    @Override
    public FollowerContext startFollowing(LogSequenceNumber lastPosition) throws LogNotAvailableException {
        return new BKFollowerContext(lastPosition);
    }

    @Override
    public void followTheLeader(LogSequenceNumber lastPosition, BiConsumer<LogSequenceNumber, LogEntry> consumer,
            FollowerContext context) throws LogNotAvailableException {
        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.finer(tableSpaceDescription() + " followTheLeader lastPosition:" + lastPosition);
        }
        BKFollowerContext fContext = (BKFollowerContext) context;
        try {
            fContext.ensureOpenReader(lastPosition);

            if (fContext.currentLedger == null) {
                // no data to read
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.finer(tableSpaceDescription() + " no more data to read for now");
                }
                return;
            }
            long nextEntry = fContext.nextEntryToRead;
            long lastAddConfirmed = fContext.currentLedger.getLastAddConfirmed();
            if (LOGGER.isLoggable(Level.FINER)) {
                LOGGER.finer(tableSpaceDescription() + " next entry to read " + nextEntry + " from ledger " + fContext.currentLedger.getId() + " lastAddConfiremd " + lastAddConfirmed);
            }
            if (lastAddConfirmed < nextEntry) {
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.finer(tableSpaceDescription() + " ledger not closed but there is nothing to read by now");
                }
                return;
            }

            ReadHandle lh = fContext.currentLedger;
            try (LastConfirmedAndEntry entryAndLac = lh.readLastAddConfirmedAndEntry(nextEntry, LONG_POLL_TIMEOUT, false);) {
                if (entryAndLac.hasEntry()) {
                    org.apache.bookkeeper.client.api.LedgerEntry e = entryAndLac.getEntry();
                    acceptEntryForFollower(e, consumer);
                    long startEntry = nextEntry + 1;
                    long endEntry = entryAndLac.getLastAddConfirmed();
                    if (startEntry > endEntry) {
                        return;
                    }
                    // if we know there are already entries ready
                    // to be read then read them
                    if (endEntry - startEntry > MAX_ENTRY_TO_TAIL) {
                        // put a bound on the max entries to read per round
                        endEntry = startEntry + MAX_ENTRY_TO_TAIL;
                    }
                    try (LedgerEntries entries = lh.read(startEntry, endEntry)) {
                        for (org.apache.bookkeeper.client.api.LedgerEntry ee : entries) {
                            acceptEntryForFollower(ee, consumer);
                        }
                    }
                }

            }

        } catch (BKClientClosedException err) {
            LOGGER.log(Level.FINE, "stop following " + tableSpaceDescription(), err);
        } catch (EOFException | org.apache.bookkeeper.client.api.BKException err) {
            LOGGER.log(Level.SEVERE, tableSpaceDescription() + " internal error", err);
            throw new LogNotAvailableException(err);
        } catch (InterruptedException err) {
            LOGGER.log(Level.SEVERE, tableSpaceDescription() + " internal error", err);
            Thread.currentThread().interrupt();
            throw new LogNotAvailableException(err);
        }
    }

    private void acceptEntryForFollower(org.apache.bookkeeper.client.api.LedgerEntry e, BiConsumer<LogSequenceNumber, LogEntry> consumer) throws EOFException {
        long entryId = e.getEntryId();
        byte[] entryData = e.getEntryBytes();
        LogEntry statusEdit = LogEntry.deserialize(entryData);
        LogSequenceNumber number = new LogSequenceNumber(e.getLedgerId(), entryId);
        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.finer(tableSpaceDescription() + " follow entry " + number);
        }
        if (lastLedgerId == number.ledgerId) {
            lastSequenceNumber.accumulateAndGet(number.offset, EnsureLongIncrementAccumulator.INSTANCE);
        } else {
            lastSequenceNumber.set(number.offset);
        }
        lastLedgerId = number.ledgerId;
        currentLedgerId = number.ledgerId;
        consumer.accept(number, statusEdit);
    }

    @Override
    public LogSequenceNumber getLastSequenceNumber() {
        return new LogSequenceNumber(lastLedgerId, lastSequenceNumber.get());
    }

}
