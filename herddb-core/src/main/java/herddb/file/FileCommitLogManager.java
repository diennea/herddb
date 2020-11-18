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

package herddb.file;

import herddb.log.CommitLogManager;
import herddb.log.LogNotAvailableException;
import herddb.server.ServerConfiguration;
import herddb.utils.OpenFileUtils;
import herddb.utils.SystemProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commit logs on local files
 *
 * @author enrico.olivelli
 */
public class FileCommitLogManager extends CommitLogManager {

    private static final int MAXCONCURRENTFSYNCS = SystemProperties.getIntSystemProperty("herddb.file.maxconcurrentfsyncs", 1);

    /**
     * Force an fsync on the disk with txlogs. This period is in seconds. It is
     * an alternative to herddb.file.requirefsync.
     */
    private final int deferredSyncPeriod;

    private static final Logger LOG = LoggerFactory.getLogger(FileCommitLogManager.class.getName());

    private final Path baseDirectory;
    private final long maxLogFileSize;
    private final int maxUnsynchedBatchSize;
    private final int maxUnsynchedBatchBytes;
    private final int maxSyncTime;
    private final boolean requireSync;
    // CHECKSTYLE.OFF: MemberName
    private final boolean enableO_DIRECT;
    // CHECKSTYLE.ON: MemberName
    private final StatsLogger statsLogger;
    private ScheduledExecutorService fsyncThreadPool;
    private final List<FileCommitLog> activeLogs = new CopyOnWriteArrayList<>();

    public FileCommitLogManager(Path baseDirectory) {
        this(baseDirectory, ServerConfiguration.PROPERTY_MAX_LOG_FILE_SIZE_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT,
                ServerConfiguration.PROPERTY_MAX_SYNC_TIME_DEFAULT,
                ServerConfiguration.PROPERTY_REQUIRE_FSYNC_DEFAULT,
                ServerConfiguration.PROPERTY_TXLOG_USE_ODIRECT_DEFAULT,
                ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT,
                NullStatsLogger.INSTANCE);
    }

    public FileCommitLogManager(
            Path baseDirectory, long maxLogFileSize, int maxUnsynchedBatchSize,
            int maxUnsynchedBatchBytes,
            int maxSyncTime,
            boolean requireSync,
            boolean enableO_DIRECT,
            int deferredSyncPeriod,
            StatsLogger statsLogger
    ) {
        this.baseDirectory = baseDirectory;
        this.maxLogFileSize = maxLogFileSize;
        this.statsLogger = statsLogger;
        this.deferredSyncPeriod = deferredSyncPeriod;
        this.maxUnsynchedBatchSize = maxUnsynchedBatchSize;
        this.maxUnsynchedBatchBytes = maxUnsynchedBatchBytes;
        this.maxSyncTime = maxSyncTime;
        this.requireSync = requireSync;
        this.enableO_DIRECT = enableO_DIRECT && OpenFileUtils.isO_DIRECT_Supported();
        LOG.info("Txlog settings: fsync: " + requireSync + ", O_DIRECT: " + enableO_DIRECT + ", deferredSyncPeriod:" + deferredSyncPeriod);
    }

    @Override
    public FileCommitLog createCommitLog(String tableSpace, String tablespaceName, String localNodeId) {
        try {
            if (fsyncThreadPool == null) {
                throw new IllegalStateException("FileCommitLogManager not started");
            }
            Path folder = baseDirectory.resolve(tableSpace + ".txlog");
            Files.createDirectories(folder);
            FileCommitLog res = new FileCommitLog(folder, tablespaceName,
                    maxLogFileSize, fsyncThreadPool, statsLogger.scope(tablespaceName),
                    activeLogs::remove,
                    maxUnsynchedBatchSize,
                    maxUnsynchedBatchBytes,
                    maxSyncTime,
                    requireSync,
                    enableO_DIRECT
            );
            activeLogs.add(res);
            return res;
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    @Override
    public void close() {
        ExecutorService _fsyncThreadPool = fsyncThreadPool;
        fsyncThreadPool = null;
        if (_fsyncThreadPool != null) {
            try {
                _fsyncThreadPool.shutdown();
                _fsyncThreadPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                LOG.info("Interrupted while waiting for fsync threadpool to exit");
            }
        }
    }

    @Override
    public void start() throws LogNotAvailableException {
        this.fsyncThreadPool = Executors.newScheduledThreadPool(MAXCONCURRENTFSYNCS);
        if (deferredSyncPeriod > 0) {
            LOG.info("Starting background fsync thread, every {} s", deferredSyncPeriod);
            this.fsyncThreadPool.scheduleWithFixedDelay(new DummyFsync(), deferredSyncPeriod,
                    deferredSyncPeriod, TimeUnit.SECONDS);
        }
    }

    private class DummyFsync implements Runnable {

        @Override
        public void run() {
            for (FileCommitLog log : activeLogs) {
                if (!log.isClosed() && !log.isFailed()) {
                    log.backgroundSync();
                }
            }

        }
    }

}
