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

import herddb.log.CommitLogManager;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.server.ServerConfiguration;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * CommitLog on Apache BookKeeper
 *
 * @author enrico.olivelli
 */
public class BookkeeperCommitLogManager extends CommitLogManager {

    private final ZookeeperMetadataStorageManager metadataStorageManager;
    private int ensemble = 1;
    private int writeQuorumSize = 1;
    private int ackQuorumSize = 1;
    private BookKeeper bookKeeper;
    private ScheduledExecutorService forceLastAddConfirmedTimer;
    private final StatsLogger statsLogger;
    private final ClientConfiguration config;
    private long ledgersRetentionPeriod = 1000 * 60 * 60 * 24;
    private long maxLedgerSizeBytes = 100 * 1024 * 1024 * 1024;
    private long maxIdleTime = 0;
    private long bookkeeperClusterReadyWaitTime = 60_000;
    private volatile boolean closed;

    private ConcurrentHashMap<String, BookkeeperCommitLog> activeLogs = new ConcurrentHashMap<>();

    public BookkeeperCommitLogManager(ZookeeperMetadataStorageManager metadataStorageManager, ServerConfiguration serverConfiguration, StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        this.bookkeeperClusterReadyWaitTime = serverConfiguration.getLong(ServerConfiguration.PROPERTY_BOOKKEEPER_WAIT_CLUSTER_READY_TIMEOUT, ServerConfiguration.PROPERTY_BOOKKEEPER_WAIT_CLUSTER_READY_TIMEOUT_DEFAULT);
        config = new ClientConfiguration();

        config.setThrottleValue(0);
        config.setZkServers(metadataStorageManager.getZkAddress());
        config.setZkTimeout(metadataStorageManager.getZkSessionTimeout());
        config.setZkLedgersRootPath(serverConfiguration.getString(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT));
        config.setEnableParallelRecoveryRead(true);
        config.setEnableDigestTypeAutodetection(true);

        /* Setups values from configuration */
        for (String key : serverConfiguration.keys()) {
            if (key.startsWith("bookkeeper.")) {
                String _key = key.substring("bookkeeper.".length());
                String value = serverConfiguration.getString(key, null);
                LOG.log(Level.CONFIG, "Setting BookKeeper client configuration: {0}={1}", new Object[]{_key, value});
                config.setProperty(_key, value);
            }
        }

        LOG.log(Level.CONFIG, "Processing server config {0}", serverConfiguration);
        if (serverConfiguration.getBoolean("bookie.preferlocalbookie", false)) {
            config.setEnsemblePlacementPolicy(PreferLocalBookiePlacementPolicy.class);
        }

        LOG.config("BookKeeper client configuration:");
        for (Iterator e = config.getKeys(); e.hasNext(); ) {
            Object key = e.next();
            LOG.log(Level.CONFIG, "{0}={1}", new Object[]{key, config.getProperty(key + "")});
        }
        this.metadataStorageManager = metadataStorageManager;
        this.forceLastAddConfirmedTimer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "force-lac-thread");
                t.setDaemon(true);
                return t;
            }
        });
    }

    public BookKeeper getBookKeeper() {
        return bookKeeper;
    }

    private void forceLastAddConfirmed() {
        activeLogs.values().forEach(l -> {
            l.forceLastAddConfirmed();
        });
    }

    @Override
    public void start() throws LogNotAvailableException {
        try {
            this.bookKeeper =
                    BookKeeper
                            .forConfig(config)
                            .statsLogger(statsLogger)
                            .build();
            if (maxIdleTime > 0) {
                this.forceLastAddConfirmedTimer.scheduleWithFixedDelay(() -> {
                    this.forceLastAddConfirmed();
                }, maxIdleTime, maxIdleTime, TimeUnit.MILLISECONDS);
            }
        } catch (IOException | InterruptedException | BKException t) {
            close();
            throw new LogNotAvailableException(t);
        }
    }

    @Override
    public void close() {
        closed = true;
        
        if (forceLastAddConfirmedTimer != null) {
            try {
                forceLastAddConfirmedTimer.shutdown();
            } finally {
                forceLastAddConfirmedTimer = null;
            }
        }
        if (bookKeeper != null) {
            try {
                bookKeeper.close();
            } catch (InterruptedException | BKException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        }
    }

    private static final Logger LOG = Logger.getLogger(BookkeeperCommitLogManager.class.getName());

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

    public long getMaxLedgerSizeBytes() {
        return maxLedgerSizeBytes;
    }

    public void setMaxLedgerSizeBytes(long maxLedgerSizeBytes) {
        this.maxLedgerSizeBytes = maxLedgerSizeBytes;
    }

    public long getMaxIdleTime() {
        return maxIdleTime;
    }

    public void setMaxIdleTime(long maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    public long getBookkeeperClusterReadyWaitTime() {
        return bookkeeperClusterReadyWaitTime;
    }

    public void setBookkeeperClusterReadyWaitTime(long bookkeeperClusterReadyWaitTime) {
        this.bookkeeperClusterReadyWaitTime = bookkeeperClusterReadyWaitTime;
    }

    @Override
    public BookkeeperCommitLog createCommitLog(String tableSpaceUUID, String tableSpaceName, String localNodeId) throws LogNotAvailableException {
        BookkeeperCommitLog res = new BookkeeperCommitLog(tableSpaceUUID, tableSpaceName, localNodeId, metadataStorageManager, bookKeeper, this);
        res.setAckQuorumSize(ackQuorumSize);
        res.setEnsemble(ensemble);
        res.setMaxLedgerSizeBytes(maxLedgerSizeBytes);
        res.setLedgersRetentionPeriod(ledgersRetentionPeriod);
        res.setMaxIdleTime(maxIdleTime);
        res.setWriteQuorumSize(writeQuorumSize);
        activeLogs.put(tableSpaceUUID, res);
        return res;
    }

    void releaseLog(String tableSpaceUUID) {
        activeLogs.remove(tableSpaceUUID);
    }

    public static void scanRawLedger(long ledgerId, long fromId, long toId, herddb.client.ClientConfiguration clientConfiguration,
            ZookeeperMetadataStorageManager metadataStorageManager, Consumer<LogEntryWithSequenceNumber> consumer) throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        config.setZkServers(metadataStorageManager.getZkAddress());
        config.setZkTimeout(metadataStorageManager.getZkSessionTimeout());
        config.setZkLedgersRootPath(clientConfiguration.getString(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT));
        config.setEnableParallelRecoveryRead(true);
        config.setEnableDigestTypeAutodetection(true);

        try (org.apache.bookkeeper.client.api.BookKeeper bookKeeper = org.apache.bookkeeper.client.api.BookKeeper.newBuilder(config).build();) {
            try (ReadHandle lh = bookKeeper
                    .newOpenLedgerOp()
                    .withRecovery(false)
                    .withLedgerId(ledgerId)
                    .withPassword(BookkeeperCommitLog.SHARED_SECRET.getBytes(StandardCharsets.UTF_8))
                    .execute()
                    .get()) {
                long lastAddConfirmed = lh.readLastAddConfirmed();
                if (toId < 0) {
                    toId = lastAddConfirmed;
                }
                LOG.log(Level.INFO, "Scanning Ledger {0} from {1} to {2} LAC {3}", new Object[]{ledgerId, fromId, toId, lastAddConfirmed});
                for (long id = fromId; id <= toId; id++) {
                    try (LedgerEntries entries = lh.readUnconfirmed(id, id);) {
                        LedgerEntry entry = entries.getEntry(id);
                        LogEntry lEntry = LogEntry.deserialize(entry.getEntryBytes());
                        LogEntryWithSequenceNumber e = new LogEntryWithSequenceNumber(
                                new LogSequenceNumber(ledgerId, id),
                                lEntry);
                        consumer.accept(e);
                    }
                }
            }
        }

    }

    boolean isClosed() {
        return closed;
    }

    public static final class LogEntryWithSequenceNumber {

        public final LogSequenceNumber logSequenceNumber;
        public final LogEntry entry;

        public LogEntryWithSequenceNumber(LogSequenceNumber logSequenceNumber, LogEntry entry) {
            this.logSequenceNumber = logSequenceNumber;
            this.entry = entry;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("LogEntryWithSequenceNumber [logSequenceNumber=").append(logSequenceNumber)
                    .append(", entry=").append(entry).append("]");
            return builder.toString();
        }

    }
}
