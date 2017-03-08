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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.log.CommitLog;
import herddb.log.CommitLogListener;
import herddb.log.FullRecoveryNeededException;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.xml.ws.Holder;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * Commit log replicated on Apache Bookkeeper
 *
 * @author enrico.olivelli
 */
public class BookkeeperCommitLog extends CommitLog {

    private static final Logger LOGGER = Logger.getLogger(BookkeeperCommitLog.class.getName());

    private String sharedSecret = "dodo";
    private final BookKeeper bookKeeper;
    private final ZookeeperMetadataStorageManager metadataManager;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final String tableSpaceUUID;
    private volatile CommitFileWriter writer;
    private long currentLedgerId = 0;
    private long lastLedgerId = -1;
    private long lastSequenceNumber = -1;
    private LedgersInfo actualLedgersList;
    private int ensemble = 1;
    private int writeQuorumSize = 1;
    private int ackQuorumSize = 1;
    private long ledgersRetentionPeriod = 1000 * 60 * 60 * 24;

    public LedgersInfo getActualLedgersList() {
        return actualLedgersList;
    }

    private void signalBrokerFailed() {

    }

    private class CommitFileWriter implements AutoCloseable {

        private LedgerHandle out;
        private long ledgerId;

        @SuppressFBWarnings("REC_CATCH_EXCEPTION")
        private CommitFileWriter() throws LogNotAvailableException {
            try {
                this.out = bookKeeper.createLedger(ensemble, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.CRC32, sharedSecret.getBytes(StandardCharsets.UTF_8));
                this.ledgerId = this.out.getId();
            } catch (Exception err) {
                throw new LogNotAvailableException(err);
            }
        }

        public long getLedgerId() {
            return ledgerId;
        }

        public long writeEntry(LogEntry edit) throws LogNotAvailableException, BKException.BKLedgerClosedException, BKException.BKLedgerFencedException, BKNotEnoughBookiesException {
            long _start = System.currentTimeMillis();
            try {
                byte[] serialize = edit.serialize();
                long res = this.out.addEntry(serialize);
                lastLedgerId = ledgerId;
                return res;
            } catch (BKException.BKLedgerClosedException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (BKException.BKLedgerFencedException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (BKException.BKNotEnoughBookiesException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (InterruptedException | BKException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw new LogNotAvailableException(err);
            } finally {
                long _end = System.currentTimeMillis();
                LOGGER.log(Level.FINEST, "writeEntry {0} time " + (_end - _start) + " ms", new Object[]{edit});
            }
        }

        @Override
        public void close() throws LogNotAvailableException {
            if (out == null) {
                return;
            }
            try {
                out.close();
            } catch (Exception err) {
                throw new LogNotAvailableException(err);
            } finally {
                out = null;
            }
        }

        private List<Long> writeEntries(List<LogEntry> edits) throws LogNotAvailableException, BKException.BKLedgerClosedException, BKException.BKLedgerFencedException, BKNotEnoughBookiesException {
            int size = edits.size();
            if (size == 0) {
                return Collections.emptyList();
            } else if (size == 1) {
                return Arrays.asList(writeEntry(edits.get(0)));
            }
            long _start = System.currentTimeMillis();
            try {
                Holder<Exception> exception = new Holder<>();
                CountDownLatch latch = new CountDownLatch(edits.size());
                List<Long> res = new ArrayList<>(edits.size());
                for (int i = 0; i < size; i++) {
                    res.add(null);
                }
                for (int i = 0; i < size; i++) {
                    LogEntry edit = edits.get(i);
                    byte[] serialize = edit.serialize();
                    this.out.asyncAddEntry(serialize, new AsyncCallback.AddCallback() {
                        @Override
                        public void addComplete(int rc, LedgerHandle lh, long entryId, Object i) {
                            int index = (Integer) i;
                            if (rc != BKException.Code.OK) {
                                BKException error = BKException.create(rc);
                                exception.value = error;
                                res.set(index, null);
                                for (int j = 0; j < edits.size(); j++) {
                                    // early exit
                                    latch.countDown();
                                }
                            } else {
                                res.set(index, entryId);
                                latch.countDown();
                            }

                        }
                    }, i);
                }
                latch.await();
                if (exception.value != null) {
                    throw exception.value;
                }
                for (Long l : res) {
                    if (l == null) {
                        throw new RuntimeException("bug ! " + res);
                    }
                }
                return res;
            } catch (BKException.BKLedgerClosedException err) {
                // corner case, if some entry has been written ?? it will be duplicated on retry
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (BKException.BKLedgerFencedException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (BKException.BKNotEnoughBookiesException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw new LogNotAvailableException(err);
            } finally {
                long _end = System.currentTimeMillis();
                LOGGER.log(Level.FINEST, "writeEntries " + edits.size() + " time " + (_end - _start) + " ms");
            }
        }
    }

    public BookkeeperCommitLog(String tableSpace, ZookeeperMetadataStorageManager metadataStorageManager, BookKeeper bookkeeper) throws LogNotAvailableException {
        this.metadataManager = metadataStorageManager;
        this.tableSpaceUUID = tableSpace;
        this.bookKeeper = bookkeeper;
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

    @Override
    public List<LogSequenceNumber> log(List<LogEntry> edits, boolean synch) throws LogNotAvailableException {
        if (edits.isEmpty()) {
            return Collections.emptyList();
        }
        while (true) {
            if (closed) {
                throw new LogNotAvailableException(new Exception("closed"));
            }
            boolean close = false;
            boolean openNew = false;
            lock.readLock().lock();
            try {
                if (writer == null) {
                    throw new LogNotAvailableException(new Exception("no ledger opened for writing"));
                }
                try {
                    List<Long> newSequenceNumbers = writer.writeEntries(edits);
                    lastSequenceNumber = newSequenceNumbers.stream().max(Comparator.naturalOrder()).get();
                    List<LogSequenceNumber> res = new ArrayList<>();
                    if (listeners != null) {
                        int i = 0;
                        for (Long newSequenceNumber : newSequenceNumbers) {
                            LogSequenceNumber logPos = new LogSequenceNumber(currentLedgerId, newSequenceNumber);
                            for (CommitLogListener l : listeners) {
                                l.logEntry(logPos, edits.get(i++));
                            }
                            res.add(logPos);
                        }
                    } else {
                        for (Long newSequenceNumber : newSequenceNumbers) {
                            res.add(new LogSequenceNumber(currentLedgerId, newSequenceNumber));
                        }
                    }
                    return res;
                } catch (BKException.BKLedgerClosedException closed) {
                    LOGGER.log(Level.SEVERE, "ledger has been closed, need to open a new ledger", closed);
                    openNew = true;
                } catch (BKException.BKLedgerFencedException fenced) {
                    LOGGER.log(Level.SEVERE, "this broker was fenced!", fenced);
                    close = true;
                    throw new LogNotAvailableException(fenced);
                } catch (BKException.BKNotEnoughBookiesException missingBk) {
                    LOGGER.log(Level.SEVERE, "bookkeeper failure", missingBk);
                    close = true;
                    throw new LogNotAvailableException(missingBk);
                }
            } finally {
                lock.readLock().unlock();
                if (close) {
                    close();
                    signalBrokerFailed();
                }
                if (openNew) {
                    try {
                        Thread.sleep(1000);
                        openNewLedger();
                    } catch (InterruptedException err) {
                        throw new LogNotAvailableException(err);
                    }
                }
            }
        }
    }

    @Override
    public LogSequenceNumber log(LogEntry edit, boolean synch) throws LogNotAvailableException {
        return log(Collections.singletonList(edit), synch).get(0);

    }

    private void openNewLedger() throws LogNotAvailableException {
        lock.writeLock().lock();
        try {
            closeCurrentWriter();
            writer = new CommitFileWriter();
            currentLedgerId = writer.getLedgerId();
            LOGGER.log(Level.SEVERE, "Opened new ledger:" + currentLedgerId);
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
        this.actualLedgersList = metadataManager.getActualLedgersList(tableSpaceUUID);
        LOGGER.log(Level.SEVERE, "Actual ledgers list:" + actualLedgersList + " tableSpace " + tableSpaceUUID);
        this.lastLedgerId = snapshotSequenceNumber.ledgerId;
        this.currentLedgerId = snapshotSequenceNumber.ledgerId;
        this.lastSequenceNumber = snapshotSequenceNumber.offset;
        LOGGER.log(Level.SEVERE, "recovery from latest snapshotSequenceNumber:" + snapshotSequenceNumber);
        if (currentLedgerId > 0 && !this.actualLedgersList.getActiveLedgers().contains(currentLedgerId) && !this.actualLedgersList.getActiveLedgers().isEmpty()) {
            // TODO: download snapshot from another remote broker
            throw new FullRecoveryNeededException(new Exception("Actual ledgers list does not include latest snapshot ledgerid:" + currentLedgerId));
        }
        if (snapshotSequenceNumber.isStartOfTime() && !this.actualLedgersList.getActiveLedgers().isEmpty() && !this.actualLedgersList.getActiveLedgers().contains(this.actualLedgersList.getFirstLedger())) {
            throw new FullRecoveryNeededException(new Exception("Local data is absent, and actual ledger list " + this.actualLedgersList.getActiveLedgers() + " does not contain first ledger of ever: " + this.actualLedgersList.getFirstLedger()));
        }
        try {
            for (long ledgerId : actualLedgersList.getActiveLedgers()) {

                if (ledgerId < snapshotSequenceNumber.ledgerId) {
                    LOGGER.log(Level.SEVERE, "Skipping ledger " + ledgerId);
                    continue;
                }
                LedgerHandle handle;
                if (fencing) {
                    handle = bookKeeper.openLedger(ledgerId, BookKeeper.DigestType.CRC32, sharedSecret.getBytes(StandardCharsets.UTF_8));
                } else {
                    handle = bookKeeper.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32, sharedSecret.getBytes(StandardCharsets.UTF_8));
                }
                try {
                    long first;
                    if (ledgerId == snapshotSequenceNumber.ledgerId) {
                        first = snapshotSequenceNumber.offset;
                        LOGGER.log(Level.SEVERE, "Recovering from latest snapshot ledger " + ledgerId + ", starting from entry " + first);
                    } else {
                        first = 0;
                        LOGGER.log(Level.SEVERE, "Recovering from ledger " + ledgerId + ", starting from entry " + first);
                    }
                    long lastAddConfirmed = handle.getLastAddConfirmed();
                    LOGGER.log(Level.SEVERE, "Recovering from ledger " + ledgerId + ", first=" + first + " lastAddConfirmed=" + lastAddConfirmed);

                    final int BATCH_SIZE = 10000;
                    if (lastAddConfirmed > 0) {

                        for (long b = first; b <= lastAddConfirmed;) {
                            long start = b;
                            long end = b + BATCH_SIZE;
                            if (end > lastAddConfirmed) {
                                end = lastAddConfirmed;
                            }
                            b = end + 1;
                            double percent = ((start - first) * 100.0 / (lastAddConfirmed + 1));
                            int entriesToRead = (int) (end - start);
                            LOGGER.log(Level.SEVERE, "From entry {0}, to entry {1} ({2} %)", new Object[]{start, end, percent});
                            long _start = System.currentTimeMillis();
                            CountDownLatch latch = new CountDownLatch(entriesToRead);
                            Holder<Throwable> error = new Holder<>();
                            long size = end - start;
                            handle.asyncReadEntries(start, end, new AsyncCallback.ReadCallback() {
                                @Override
                                public void readComplete(int code, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object o) {
                                    try {
                                        LOGGER.log(Level.SEVERE, "readComplete " + code + " " + lh);
                                        if (code != Code.OK) {
                                            error.value = BKException.create(code);
                                            LOGGER.log(Level.SEVERE, "readComplete error:" + error);
                                            for (long k = 0; k < size; k++) {
                                                latch.countDown();
                                            }
                                            return;
                                        }
                                        int localEntryCount = 0;
                                        while (seq.hasMoreElements()) {
                                            LedgerEntry entry = seq.nextElement();
                                            long entryId = entry.getEntryId();
                                            LogSequenceNumber number = new LogSequenceNumber(ledgerId, entryId);
                                            LogEntry statusEdit = LogEntry.deserialize(entry.getEntry());
                                            lastLedgerId = ledgerId;
                                            currentLedgerId = ledgerId;
                                            lastSequenceNumber = entryId;
                                            if (number.after(snapshotSequenceNumber)) {
                                                LOGGER.log(Level.FINEST, "RECOVER ENTRY #" + localEntryCount + " {0}, {1}", new Object[]{number, statusEdit});
                                                consumer.accept(number, statusEdit);
                                            } else {
                                                LOGGER.log(Level.FINEST, "SKIP ENTRY #" + localEntryCount + " {0}<{1}, {2}", new Object[]{number, snapshotSequenceNumber, statusEdit});
                                            }
                                            latch.countDown();
                                            localEntryCount++;
                                        }
                                        LOGGER.log(Level.SEVERE, "read " + localEntryCount + " entries from ledger " + ledgerId + ", expected " + entriesToRead);
                                    } catch (Throwable t) {
                                        LOGGER.log(Level.SEVERE, "error while processing data: " + t, t);
                                        error.value = t;
                                    }
                                }
                            }, null);
                            LOGGER.log(Level.SEVERE, "waiting for " + entriesToRead + " entries to be read from ledger " + ledgerId);
                            latch.await();
                            LOGGER.log(Level.SEVERE, "finished waiting for " + entriesToRead + " entries to be read from ledger " + ledgerId);
                            if (error.value != null) {
                                throw new RuntimeException(error.value);
                            }
                            lastLedgerId = ledgerId;
                            lastSequenceNumber = end;
                            long _stop = System.currentTimeMillis();
                            LOGGER.log(Level.SEVERE, "From entry {0}, to entry {1} ({2} %) read time {3}", new Object[]{start, end, percent, (_stop - _start) + " ms"});
                        }
                    }
                } finally {
                    handle.close();
                }
            }
            LOGGER.severe("After recovery of " + tableSpaceUUID + " lastSequenceNumber " + getLastSequenceNumber());
        } catch (InterruptedException | RuntimeException | BKException err) {
            LOGGER.log(Level.SEVERE, "Fatal error during recovery", err);
            signalBrokerFailed();
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
        if (ledgersRetentionPeriod > 0) {
            LOGGER.log(Level.SEVERE, "dropOldLedgers lastCheckPointSequenceNumber: {0}, ledgersRetentionPeriod: {1} ,lastLedgerId: {2}, currentLedgerId: {3}",
                new Object[]{lastCheckPointSequenceNumber, ledgersRetentionPeriod, lastLedgerId, currentLedgerId});
            long min_timestamp = System.currentTimeMillis() - ledgersRetentionPeriod;
            List<Long> oldLedgers;
            lock.readLock().lock();
            try {
                oldLedgers = actualLedgersList.getOldLedgers(min_timestamp);
            } finally {
                lock.readLock().unlock();
            }

            LOGGER.log(Level.SEVERE, "dropOldLedgers currentLedgerId: {0}, lastLedgerId: {1}, dropping ledgers before {2}: {3}",
                new Object[]{currentLedgerId, lastLedgerId, new java.sql.Timestamp(min_timestamp), oldLedgers});
            oldLedgers.remove(this.currentLedgerId);
            oldLedgers.remove(this.lastLedgerId);
            if (oldLedgers.isEmpty()) {
                return;
            }
            for (long ledgerId : oldLedgers) {
                lock.writeLock().lock();
                try {
                    LOGGER.log(Level.SEVERE, "dropping ledger {0}", ledgerId);
                    actualLedgersList.removeLedger(ledgerId);
                    try {
                        bookKeeper.deleteLedger(ledgerId);
                    } catch (BKException.BKNoSuchLedgerExistsException error) {
                        LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId, error);
                    }
                    metadataManager.saveActualLedgersList(tableSpaceUUID, actualLedgersList);
                    LOGGER.log(Level.SEVERE, "dropping ledger {0}, finished", ledgerId);
                } catch (BKException | InterruptedException error) {
                    LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId, error);
                    throw new LogNotAvailableException(error);
                } finally {
                    lock.writeLock().unlock();
                }
            }

        }
    }

    private volatile boolean closed = false;

    @Override
    public final void close() {
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
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while closing ledger", err);
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
    public void followTheLeader(LogSequenceNumber skipPast, BiConsumer<LogSequenceNumber, LogEntry> consumer) throws LogNotAvailableException {

        List<Long> actualList = metadataManager.getActualLedgersList(tableSpaceUUID).getActiveLedgers();

        List<Long> toRead = actualList;
        if (skipPast.ledgerId != -1) {
            toRead = toRead.stream().filter(l -> l >= skipPast.ledgerId).collect(Collectors.toList());
        }
        try {
            long nextEntry = skipPast.offset + 1;
//            LOGGER.log(Level.SEVERE, "followTheLeader "+tableSpace+" skipPast:{0} toRead: {1} actualList:{2}, nextEntry:{3}", new Object[]{skipPast, toRead, actualList, nextEntry});
            for (Long previous : toRead) {
                //LOGGER.log(Level.SEVERE, "followTheLeader openLedger " + previous + " nextEntry:" + nextEntry);
                LedgerHandle lh;
                try {
                    lh = bookKeeper.openLedgerNoRecovery(previous,
                        BookKeeper.DigestType.CRC32, sharedSecret.getBytes(StandardCharsets.UTF_8));
                } catch (BKException.BKLedgerRecoveryException e) {
                    LOGGER.log(Level.SEVERE, "error", e);
                    return;
                }
                try {
                    long lastAddConfirmed = lh.getLastAddConfirmed();
                    LOGGER.log(Level.FINE, "followTheLeader " + tableSpaceUUID + " openLedger {0} -> lastAddConfirmed:{1}, nextEntry:{2}", new Object[]{previous, lastAddConfirmed, nextEntry});
                    if (nextEntry > lastAddConfirmed) {
                        nextEntry = 0;
                        continue;
                    }
                    Enumeration<LedgerEntry> entries
                        = lh.readEntries(nextEntry, lh.getLastAddConfirmed());

                    while (entries.hasMoreElements()) {
                        LedgerEntry e = entries.nextElement();
                        long entryId = e.getEntryId();

                        byte[] entryData = e.getEntry();
                        LogEntry statusEdit = LogEntry.deserialize(entryData);
//                        LOGGER.log(Level.SEVERE, "" + tableSpaceUUID + " followentry {0},{1} -> {2}", new Object[]{previous, entryId, statusEdit});
                        LogSequenceNumber number = new LogSequenceNumber(previous, entryId);
                        lastSequenceNumber = number.offset;
                        lastLedgerId = number.ledgerId;
                        currentLedgerId = number.ledgerId;
                        consumer.accept(number, statusEdit);
                    }
                } finally {
                    try {
                        lh.close();
                    } catch (BKException err) {
                        LOGGER.log(Level.SEVERE, "error while closing ledger", err);
                    } catch (InterruptedException err) {
                        LOGGER.log(Level.SEVERE, "error while closing ledger", err);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } catch (InterruptedException | BKException err) {
            LOGGER.log(Level.SEVERE, "internal error", err);
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public LogSequenceNumber getLastSequenceNumber() {
        return new LogSequenceNumber(lastLedgerId, lastSequenceNumber);
    }

}
