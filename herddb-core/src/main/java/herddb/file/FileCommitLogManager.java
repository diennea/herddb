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
import herddb.utils.SystemProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Commit logs on local files
 *
 * @author enrico.olivelli
 */
public class FileCommitLogManager extends CommitLogManager {

    private final static int MAXCONCURRENTFSYNCS = SystemProperties.getIntSystemProperty(
            "herddb.file.maxconcurrentfsyncs", 1);

    private static final Logger LOG = Logger.getLogger(FileCommitLogManager.class.getName());

    private final Path baseDirectory;
    private final long maxLogFileSize;
    private final StatsLogger statsLogger;
    private ExecutorService fsyncThreadPool;

    public FileCommitLogManager(Path baseDirectory, long maxLogFileSize) {
        this(baseDirectory, maxLogFileSize, new NullStatsLogger());
    }

    public FileCommitLogManager(Path baseDirectory, long maxLogFileSize, StatsLogger statsLogger) {
        this.baseDirectory = baseDirectory;
        this.maxLogFileSize = maxLogFileSize;
        this.statsLogger = statsLogger;

    }

    @Override
    public FileCommitLog createCommitLog(String tableSpace, String tablespaceName, String localNodeId) {
        try {
            if (fsyncThreadPool == null) {
                throw new IllegalStateException("FileCommitLogManager not started");
            }
            Path folder = baseDirectory.resolve(tableSpace + ".txlog");
            Files.createDirectories(folder);
            return new FileCommitLog(folder, tablespaceName,
                    maxLogFileSize, fsyncThreadPool, statsLogger.scope(tablespaceName));
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
                LOG.log(Level.INFO, "Interrupted while waiting for fsync threadpool to exit");
            }
        }
    }

    @Override
    public void start() throws LogNotAvailableException {
        this.fsyncThreadPool = Executors.newFixedThreadPool(MAXCONCURRENTFSYNCS);
    }

}
