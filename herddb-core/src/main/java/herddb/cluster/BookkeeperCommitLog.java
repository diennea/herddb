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

import herddb.log.CommitLog;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.xml.ws.Holder;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.KeeperException;

/**
 * Commit log replicated on Apache Bookkeeper
 *
 * @author enrico.olivelli
 */
public class BookkeeperCommitLog extends CommitLog {

    private static final Logger LOGGER = Logger.getLogger(BookkeeperCommitLog.class.getName());

    private String sharedSecret = "dodo";
    private BookKeeper bookKeeper;
    private ZookeeperMetadataStorageManager metadataManager;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final String tableSpace;
    private volatile CommitFileWriter writer;
    private long currentLedgerId = 0;
    private long lastSequenceNumber = -1;
    private LedgersInfo actualLedgersList;
    private int ensemble = 1;
    private int writeQuorumSize = 1;
    private int ackQuorumSize = 1;
    private long ledgersRetentionPeriod = 1000 * 60 * 60 * 24;
    private long maxLogicalLogFileSize = 1024 * 1024 * 256;
    private long writtenBytes = 0;

    public long getMaxLogicalLogFileSize() {
        return maxLogicalLogFileSize;
    }

    public void setMaxLogicalLogFileSize(long maxLogicalLogFileSize) {
        this.maxLogicalLogFileSize = maxLogicalLogFileSize;
    }

    public LedgersInfo getActualLedgersList() {
        return actualLedgersList;
    }

    private void signalBrokerFailed() {

    }

    private class CommitFileWriter implements AutoCloseable {

        private LedgerHandle out;

        private CommitFileWriter() throws LogNotAvailableException {
            try {
                this.out = bookKeeper.createLedger(ensemble, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.CRC32, sharedSecret.getBytes(StandardCharsets.UTF_8));
                writtenBytes = 0;
            } catch (Exception err) {
                throw new LogNotAvailableException(err);
            }
        }

        public long getLedgerId() {
            return this.out.getId();
        }

        public long writeEntry(LogEntry edit) throws LogNotAvailableException, BKException.BKLedgerClosedException, BKException.BKLedgerFencedException, BKNotEnoughBookiesException {
            long _start = System.currentTimeMillis();
            try {
                byte[] serialize = edit.serialize();
                writtenBytes += serialize.length;
                long res = this.out.addEntry(serialize);
                if (writtenBytes > maxLogicalLogFileSize) {
                    LOGGER.log(Level.SEVERE, "{0} bytes written to ledger. need to open a new one", writtenBytes);
                    openNewLedger();
                }
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
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw new LogNotAvailableException(err);
            } finally {
                long _end = System.currentTimeMillis();
                LOGGER.log(Level.FINEST, "writeEntry {0} time " + (_end - _start) + " ms", new Object[]{edit});
            }
        }

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
                    writtenBytes += serialize.length;
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
                if (writtenBytes > maxLogicalLogFileSize) {
                    LOGGER.log(Level.SEVERE, "{0} bytes written to ledger. need to open a new one", writtenBytes);
                    openNewLedger();
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

    public BookkeeperCommitLog(String tableSpace, ZookeeperMetadataStorageManager metadataStorageManager) throws LogNotAvailableException {
        this.metadataManager = metadataStorageManager;
        this.tableSpace = tableSpace;
        ClientConfiguration config = new ClientConfiguration();
        try {
            this.bookKeeper = new BookKeeper(config, metadataManager.getZooKeeper());
        } catch (IOException | InterruptedException | KeeperException t) {
            close();
            throw new LogNotAvailableException(t);
        }
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
    public List<LogSequenceNumber> log(List<LogEntry> edits) throws LogNotAvailableException {
        if (edits.isEmpty()) {
            return Collections.emptyList();
        }
        while (true) {
            if (closed) {
                throw new LogNotAvailableException(new Exception("closed"));
            }
            writeLock.lock();
            try {
                if (writer == null) {
                    throw new LogNotAvailableException(new Exception("no ledger opened for writing"));
                }
                try {
                    List<Long> newSequenceNumbers = writer.writeEntries(edits);
                    lastSequenceNumber = newSequenceNumbers.stream().max(Comparator.naturalOrder()).get();
                    List<LogSequenceNumber> res = new ArrayList<>();
                    for (Long newSequenceNumber : newSequenceNumbers) {
                        res.add(new LogSequenceNumber(currentLedgerId, newSequenceNumber));
                    }
                    return res;
                } catch (BKException.BKLedgerClosedException closed) {
                    LOGGER.log(Level.SEVERE, "ledger has been closed, need to open a new ledger", closed);
                    Thread.sleep(1000);
                    openNewLedger();
                } catch (BKException.BKLedgerFencedException fenced) {
                    LOGGER.log(Level.SEVERE, "this broker was fenced!", fenced);
                    close();
                    signalBrokerFailed();
                    throw new LogNotAvailableException(fenced);
                } catch (BKException.BKNotEnoughBookiesException missingBk) {
                    LOGGER.log(Level.SEVERE, "bookkeeper failure", missingBk);
                    close();
                    signalBrokerFailed();
                    throw new LogNotAvailableException(missingBk);
                }
            } catch (InterruptedException err) {
                throw new LogNotAvailableException(err);
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public LogSequenceNumber log(LogEntry edit) throws LogNotAvailableException {
        while (true) {
            if (closed) {
                throw new LogNotAvailableException(new Exception("closed"));
            }
            writeLock.lock();
            try {
                if (writer == null) {
                    throw new LogNotAvailableException(new Exception("no ledger opened for writing"));
                }
                try {
                    long newSequenceNumber = writer.writeEntry(edit);
                    lastSequenceNumber = newSequenceNumber;
                    return new LogSequenceNumber(currentLedgerId, newSequenceNumber);
                } catch (BKException.BKLedgerClosedException closed) {
                    LOGGER.log(Level.SEVERE, "ledger has been closed, need to open a new ledger", closed);
                    Thread.sleep(1000);
                    openNewLedger();
                } catch (BKException.BKLedgerFencedException fenced) {
                    LOGGER.log(Level.SEVERE, "this broker was fenced!", fenced);
                    close();
                    signalBrokerFailed();
                    throw new LogNotAvailableException(fenced);
                } catch (BKException.BKNotEnoughBookiesException missingBk) {
                    LOGGER.log(Level.SEVERE, "bookkeeper failure", missingBk);
                    close();
                    signalBrokerFailed();
                    throw new LogNotAvailableException(missingBk);
                }
            } catch (InterruptedException err) {
                throw new LogNotAvailableException(err);
            } finally {
                writeLock.unlock();
            }
        }

    }

    private void openNewLedger() throws LogNotAvailableException {
        writeLock.lock();
        try {
            closeCurrentWriter();
            writer = new CommitFileWriter();
            currentLedgerId = writer.getLedgerId();
            LOGGER.log(Level.SEVERE, "Opened new ledger:" + currentLedgerId);
            if (actualLedgersList.getFirstLedger() < 0) {
                actualLedgersList.setFirstLedger(currentLedgerId);
            }
            actualLedgersList.addLedger(currentLedgerId);
            metadataManager.saveActualLedgersList(tableSpace, actualLedgersList);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing) throws LogNotAvailableException {
        this.actualLedgersList = metadataManager.getActualLedgersList(tableSpace);
        LOGGER.log(Level.SEVERE, "Actual ledgers list:" + actualLedgersList);
        this.currentLedgerId = snapshotSequenceNumber.ledgerId;
        LOGGER.log(Level.SEVERE, "Latest snapshotSequenceNumber:" + snapshotSequenceNumber);
        if (currentLedgerId > 0 && !this.actualLedgersList.getActiveLedgers().contains(currentLedgerId)) {
            // TODO: download snapshot from another remote broker
            throw new LogNotAvailableException(new Exception("Actual ledgers list does not include latest snapshot ledgerid:" + currentLedgerId + ". manual recoveryis needed (pickup a recent snapshot from a live broker please)"));
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
                    LOGGER.log(Level.SEVERE, "Recovering from ledger " + ledgerId + ", first=" + first, " lastAddConfirmed=" + lastAddConfirmed);
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
                            LOGGER.log(Level.SEVERE, "From entry {0}, to entry {1} ({2} %)", new Object[]{start, end, percent});
                            Enumeration<LedgerEntry> seq = handle.readEntries(start, end);
                            while (seq.hasMoreElements()) {
                                LedgerEntry entry = seq.nextElement();
                                LogSequenceNumber number = new LogSequenceNumber(ledgerId, entry.getEntryId());
                                LogEntry statusEdit = LogEntry.deserialize(entry.getEntry());
                                if (number.after(snapshotSequenceNumber)) {
                                    LOGGER.log(Level.FINEST, "RECOVER ENTRY {0}, {1}", new Object[]{number, statusEdit});
                                    consumer.accept(number, statusEdit);
                                } else {
                                    LOGGER.log(Level.FINEST, "SKIP ENTRY {0}<{1}, {2}", new Object[]{number, snapshotSequenceNumber, statusEdit});
                                }
                            }
                        }
                    }
                } finally {
                    handle.close();
                }
            }
        } catch (Exception err) {
            LOGGER.log(Level.SEVERE, "Fatal error during recovery", err);
            signalBrokerFailed();
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void startWriting() throws LogNotAvailableException {
        actualLedgersList = metadataManager.getActualLedgersList(tableSpace);
        openNewLedger();
    }

    @Override
    public void clear() throws LogNotAvailableException {
        this.currentLedgerId = 0;
        metadataManager.saveActualLedgersList(tableSpace, new LedgersInfo());
    }

    @Override
    public void checkpoint() throws LogNotAvailableException {
        dropOldLedgers();
    }

    private void dropOldLedgers() throws LogNotAvailableException {
        if (ledgersRetentionPeriod > 0) {
            long min_timestamp = System.currentTimeMillis() - ledgersRetentionPeriod;
            List<Long> oldLedgers;
            writeLock.lock();
            try {
                oldLedgers = actualLedgersList.getOldLedgers(min_timestamp);
                oldLedgers.remove(this.currentLedgerId);
            } finally {
                writeLock.unlock();
            }
            if (oldLedgers.isEmpty()) {
                return;
            }
            LOGGER.log(Level.SEVERE, "dropping ledgers before ", new java.sql.Timestamp(min_timestamp) + ": " + oldLedgers);
            for (long ledgerId : oldLedgers) {
                writeLock.lock();
                try {
                    LOGGER.log(Level.SEVERE, "dropping ledger {0}", ledgerId);
                    actualLedgersList.removeLedger(ledgerId);
                    bookKeeper.deleteLedger(ledgerId);
                    metadataManager.saveActualLedgersList(tableSpace, actualLedgersList);
                    LOGGER.log(Level.SEVERE, "dropping ledger {0}, finished", ledgerId);
                } catch (BKException | InterruptedException error) {
                    LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId, error);
                    throw new LogNotAvailableException(error);
                } finally {
                    writeLock.unlock();
                }
            }

        }
    }

    private volatile boolean closed = false;

    @Override
    public final void close() {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closeCurrentWriter();
            closed = true;
            LOGGER.severe("closed");
        } finally {
            writer = null;
            writeLock.unlock();
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

        List<Long> actualList = metadataManager.getActualLedgersList(tableSpace).getActiveLedgers();

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
                    LOGGER.log(Level.FINE, "followTheLeader "+tableSpace+" openLedger {0} -> lastAddConfirmed:{1}, nextEntry:{2}", new Object[]{previous, lastAddConfirmed, nextEntry});
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
                        LOGGER.log(Level.SEVERE, ""+tableSpace+" followentry {0},{1} -> {2}", new Object[]{previous, entryId, statusEdit});
                        LogSequenceNumber number = new LogSequenceNumber(previous, entryId);
                        consumer.accept(number, statusEdit);
                        lastSequenceNumber = number.offset;
                        currentLedgerId = number.ledgerId;
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
    public LogSequenceNumber getActualSequenceNumber() {
        return new LogSequenceNumber(currentLedgerId, lastSequenceNumber);
    }

}
