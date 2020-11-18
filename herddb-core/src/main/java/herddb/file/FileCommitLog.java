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

import herddb.log.CommitLog;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import herddb.utils.ODirectFileOutputStream;
import herddb.utils.OpenFileUtils;
import herddb.utils.SimpleBufferedOutputStream;
import herddb.utils.SystemProperties;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commit log on file
 *
 * @author enrico.olivelli
 */
public class FileCommitLog extends CommitLog {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitLog.class.getName());

    private Path logDirectory;
    private final String tableSpaceName;

    private long currentLedgerId = 0;

    private LogSequenceNumber recoveredLogSequence;

    private long maxLogFileSize = 1024 * 1024;
    private long writtenBytes = 0;

    private volatile CommitFileWriter writer;
    private Thread spool;
    private final OpStatsLogger statsFsyncTime;
    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicInteger pendingEntries = new AtomicInteger();
    private final OpStatsLogger statsEntryLatency;
    private final OpStatsLogger statsEntrySyncLatency;
    private final OpStatsLogger syncSize;
    private final OpStatsLogger syncBytes;
    private final Counter deferredSyncs;
    private final Counter newfiles;
    private final ExecutorService fsyncThreadPool;
    private final Consumer<FileCommitLog> onClose;

    private static final int WRITE_QUEUE_SIZE = SystemProperties.getIntSystemProperty(
            "herddb.file.writequeuesize", 10_000_000);

    private final BlockingQueue<LogEntryHolderFuture> writeQueue = new LinkedBlockingQueue<>(WRITE_QUEUE_SIZE);

    private final int maxUnsyncedBatchSize;
    private final int maxUnsyncedBatchBytes;
    private final long maxSyncTime;
    private final boolean requireSync;
    // CHECKSTYLE.OFF: MemberName
    private final boolean enableO_DIRECT;
    // CHECKSTYLE.ON: MemberName

    public static final String LOGFILEEXTENSION = ".txlog";

    private volatile boolean closed = false;
    private volatile boolean failed = false;
    private volatile boolean needsSync = false;

    static final byte ZERO_PADDING = 0;
    static final byte ENTRY_START = 13;
    static final byte ENTRY_END = 25;

    void backgroundSync() {
        if (needsSync) {
            try {
                getWriter().sync(true);
                deferredSyncs.inc();
                needsSync = false;
            } catch (Throwable t) {
                LOGGER.error("error background fsync on " + this.logDirectory, t);
            }
        }
    }

    class CommitFileWriter implements AutoCloseable {

        final long ledgerId;
        long sequenceNumber;

        final Path filename;
        final FileChannel channel;
        final ExtendedDataOutputStream out;
        volatile boolean writerClosed;

        private CommitFileWriter(long ledgerId, long sequenceNumber) throws IOException {
            this.ledgerId = ledgerId;
            this.sequenceNumber = sequenceNumber;

            filename = logDirectory.resolve(String.format("%016x", ledgerId) + LOGFILEEXTENSION).toAbsolutePath();
            // in case of IOException the stream is not opened, not need to close it
            if (enableO_DIRECT) {
                Files.createFile(filename);
                LOGGER.debug("opening (O_DIRECT) new file {} for tablespace {}", new Object[]{filename, tableSpaceName});
                // in O_DIRECT mode we have to call flush() and this will
                // eventually write all data to disks, adding some padding of zeroes
                // because in O_DIRECT mode you can only write fixed length blocks
                // of data
                // fsync is quite redudant, but having separated code paths
                // will make code tricker to understand.
                // so in O_DIRECT mode flush() ~ fsync()
                ODirectFileOutputStream oo = new ODirectFileOutputStream(filename);
                this.channel = oo.getFc();
                this.out = new ExtendedDataOutputStream(oo);
            } else {
                LOGGER.debug("opening (no O_DIRECT) new file {} for tablespace {}", new Object[]{filename, tableSpaceName});
                this.channel = FileChannel.open(filename,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

                this.out = new ExtendedDataOutputStream(new SimpleBufferedOutputStream(Channels.newOutputStream(this.channel)));
            }
            writtenBytes = 0;
        }

        private int writeEntry(long seqnumber, LogEntry entry) throws IOException {
            this.out.writeByte(ENTRY_START);
            this.out.writeLong(seqnumber);
            int written = entry.serialize(out);
            this.out.writeByte(ENTRY_END);
            int entrySize = (1 + 8 + written + 1);
            writtenBytes += entrySize;

            if (!requireSync) {
                needsSync = true;
            }

            return entrySize;
        }

        public void flush() throws IOException {
            this.out.flush();
        }

        public void sync() throws IOException {
            sync(false);
        }

        public void sync(boolean force) throws IOException {

            if (!force && !requireSync) {
                return;
            }
            long now = System.nanoTime();
            /* We don't need to flush file metadatas, flush just data! */
            try {
                this.channel.force(false);
            } catch (ClosedChannelException closed) {
                if (!writerClosed) {
                    throw closed;
                }
            }
            final long time = System.nanoTime() - now;
            statsFsyncTime.registerSuccessfulEvent(time, TimeUnit.NANOSECONDS);
        }

        @Override
        public void close() throws LogNotAvailableException {
            try {
                try {
                    out.flush();
                    sync();
                } catch (IOException err) {
                    throw new LogNotAvailableException(err);
                }
            } finally {
                try {
                    // we are closing this writer, every write has been fsync'd so
                    // new fsyncs in another thread may ignore errors
                    writerClosed = true;

                    out.close();
                    channel.close();
                } catch (IOException err) {
                    throw new LogNotAvailableException(err);
                }
            }
        }
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

    public static class CommitFileReader implements AutoCloseable {

        final ExtendedDataInputStream in;
        final long ledgerId;

        private CommitFileReader(ExtendedDataInputStream in, long ledgerId) {
            this.in = in;
            this.ledgerId = ledgerId;
        }

        public static CommitFileReader openForDescribeRawfile(Path filename) throws IOException {
            ExtendedDataInputStream in = new ExtendedDataInputStream(
                    new BufferedInputStream(Files.newInputStream(filename, StandardOpenOption.READ), 64 * 1024));
            String lastPath = (filename.getFileName() + "").replace(LOGFILEEXTENSION, "");
            long ledgerId;
            try {
                ledgerId = Long.valueOf(lastPath, 16);
            } catch (NumberFormatException err) {
                ledgerId = 0;
            }
            return new CommitFileReader(in, ledgerId);
        }

        private CommitFileReader(Path logDirectory, long ledgerId) throws IOException {
            this.ledgerId = ledgerId;
            Path filename = logDirectory.resolve(String.format("%016x", ledgerId) + LOGFILEEXTENSION);
            // in case of IOException the stream is not opened, not need to close it
            this.in = new ExtendedDataInputStream(new BufferedInputStream(Files.newInputStream(filename, StandardOpenOption.READ), 64 * 1024));
        }

        public LogEntryWithSequenceNumber nextEntry() throws IOException {
            byte entryStart;
            try {
                try {
                    entryStart = in.readByte();
                } catch (EOFException completeFileFinished) {
                    return null;
                }
                // skip zeros due to padding if using pre-allocation or O_DIRECT
                while (entryStart == ZERO_PADDING) {
                    try {
                        entryStart = in.readByte();
                    } catch (EOFException completeFileFinished) {
                        return null;
                    }
                }
                if (entryStart != ENTRY_START) {
                    throw new IOException("corrupted txlog file");
                }
                long seqNumber = this.in.readLong();
                LogEntry edit = LogEntry.deserialize(this.in);
                int entryEnd = this.in.readByte();
                if (entryEnd != ENTRY_END) {
                    throw new IOException("corrupted txlog file, found a " + entryEnd + " instead of magic '" + ENTRY_END + "'");
                }
                return new LogEntryWithSequenceNumber(new LogSequenceNumber(ledgerId, seqNumber), edit);
            } catch (EOFException truncatedLog) {
                // if we hit EOF the entry has not been written, and so not acked, we can ignore it and say that the file is finished
                // it is important that this is the last file in the set
                LOGGER.error("found unfinished entry in file " + this.ledgerId + ". entry was not acked. ignoring " + truncatedLog);
                return null;
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    private void openNewLedger() throws LogNotAvailableException {

        try {
            if (writer != null) {
                LOGGER.trace("closing actual file {}", writer.filename);
                writer.close();
            }

            writer = new CommitFileWriter(++currentLedgerId, -1);
            newfiles.inc();

        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    public FileCommitLog(
            Path logDirectory, String tableSpaceName,
            long maxLogFileSize, ExecutorService fsyncThreadPool, StatsLogger statslogger,
            Consumer<FileCommitLog> onClose,
            int maxUnsynchedBatchSize,
            int maxUnsynchedBatchBytes,
            int maxSyncTime,
            boolean requireSync,
            boolean enableO_DIRECT
    ) {
        this.maxUnsyncedBatchSize = maxUnsynchedBatchSize;
        this.maxUnsyncedBatchBytes = maxUnsynchedBatchBytes;
        this.maxSyncTime = TimeUnit.MILLISECONDS.toNanos(maxSyncTime);
        this.requireSync = requireSync;
        this.enableO_DIRECT = enableO_DIRECT && OpenFileUtils.isO_DIRECT_Supported();
        this.onClose = onClose;
        this.maxLogFileSize = maxLogFileSize;
        this.tableSpaceName = tableSpaceName;
        this.logDirectory = logDirectory.toAbsolutePath();
        this.spool = new Thread(new SpoolTask(), "commitlog-" + tableSpaceName);
        this.spool.setDaemon(true);
        this.statsFsyncTime = statslogger.getOpStatsLogger("fsync");
        this.statsEntryLatency = statslogger.getOpStatsLogger("entryLatency");
        this.statsEntrySyncLatency = statslogger.getOpStatsLogger("entrySyncLatency");
        this.syncSize = statslogger.getOpStatsLogger("syncBatchSize");
        this.syncBytes = statslogger.getOpStatsLogger("syncBatchBytes");
        this.deferredSyncs = statslogger.getCounter("deferredSyncs");
        this.newfiles = statslogger.getCounter("newfiles");
        statslogger.registerGauge("queuesize", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return queueSize.get();
            }

        });
        statslogger.registerGauge("pendingentries", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return pendingEntries.get();
            }

        });

        this.fsyncThreadPool = fsyncThreadPool;
        LOGGER.debug("tablespace {}, logdirectory: {}, maxLogFileSize {} bytes", new Object[]{logDirectory, maxLogFileSize, tableSpaceName});
    }

    private class SyncTask implements Runnable {

        private List<LogEntryHolderFuture> syncNeeded;

        private final int unsyncedCount;
        private final long unsyncedBytes;

        public SyncTask(List<LogEntryHolderFuture> syncNeeded, int unsyncedCount, long unsyncedBytes) {
            super();
            this.syncNeeded = syncNeeded;
            this.unsyncedBytes = unsyncedBytes;
            this.unsyncedCount = unsyncedCount;
        }

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            try {
                synch();

                syncSize.registerSuccessfulValue(unsyncedCount);
                syncBytes.registerSuccessfulValue(unsyncedBytes);

                for (LogEntryHolderFuture e : syncNeeded) {
                    statsEntrySyncLatency.registerSuccessfulEvent(now - e.timestamp, TimeUnit.MILLISECONDS);
                    e.syncDone();
                }
                syncNeeded = null;
            } catch (Throwable t) {
                failed = true;
                LOGGER.error("general commit log failure on " + FileCommitLog.this.logDirectory, t);

                for (LogEntryHolderFuture e : syncNeeded) {
                    statsEntrySyncLatency.registerFailedEvent(now - e.timestamp, TimeUnit.MILLISECONDS);
                }
            }
        }

    }

    private class SpoolTask implements Runnable {

        @Override
        public void run() {
            try {
                openNewLedger();
                List<LogEntryHolderFuture> syncNeeded = new ArrayList<>();
                long unsyncedBytes = 0;
                int unsyncedCount = 0;
                while (!closed || !writeQueue.isEmpty()) {
                    LogEntryHolderFuture entry = writeQueue.poll(maxSyncTime, TimeUnit.NANOSECONDS);
                    boolean timedOut = false;
                    if (entry != null) {
                        if (entry.entry == null) {
                            // force close placeholder
                            break;
                        }

                        queueSize.decrementAndGet();
                        int size = writeEntry(entry);

                        ++unsyncedCount;
                        unsyncedBytes += size;

                        if (entry.sync) {
                            syncNeeded.add(entry);
                        }

                    } else {
                        timedOut = true;
                    }
                    if (timedOut || unsyncedCount >= maxUnsyncedBatchSize || unsyncedBytes >= maxUnsyncedBatchBytes) {

                        /* Don't flush if there is nothing */
                        if (unsyncedCount > 0) {

                            flush();

                            if (!syncNeeded.isEmpty()) {
                                SyncTask syncTask = new SyncTask(syncNeeded, unsyncedCount, unsyncedBytes);
                                syncNeeded = new ArrayList<>();
                                fsyncThreadPool.submit(syncTask);
                            }

                            unsyncedCount = 0;
                            unsyncedBytes = 0L;
                        }

                    }
                }

                /* Don't flush if there is nothing */
                if (unsyncedCount > 0) {
                    LOGGER.info("flushing last {} entries", unsyncedCount);

                    flush();

                    /* Don't synch if there is nothing */
                    if (!syncNeeded.isEmpty()) {
                        LOGGER.info("synching last {} entries", unsyncedCount);
                        SyncTask syncTask = new SyncTask(syncNeeded, unsyncedCount, unsyncedBytes);
                        syncTask.run();
                    }

                }

            } catch (LogNotAvailableException | IOException | InterruptedException t) {
                failed = true;
                LOGGER.error("general commit log failure on " + FileCommitLog.this.logDirectory, t);
            }
        }
    }

    private class LogEntryHolderFuture {

        final CompletableFuture<LogSequenceNumber> ack = new CompletableFuture<>();
        final LogEntry entry;
        final long timestamp;
        LogSequenceNumber sequenceNumber;
        Throwable error;
        final boolean sync;

        public LogEntryHolderFuture(LogEntry entry, boolean synch) {
            if (entry == null) {
                // handle force close
                this.entry = null;
                this.timestamp = System.currentTimeMillis();
            } else {
                this.entry = entry;
                this.timestamp = entry.timestamp;
            }
            this.sync = synch;

        }

        public void error(Throwable error) {
            this.error = error;
            if (!sync) {
                syncDone();
            }
        }

        public void done(LogSequenceNumber sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            if (!sync) {
                syncDone();
            }
        }

        private void syncDone() {
            if (sequenceNumber == null && error == null) {
                throw new IllegalStateException();
            }
            pendingEntries.decrementAndGet();
            if (error != null) {
                ack.completeExceptionally(error);
            } else {
                ack.complete(sequenceNumber);
            }
        }

    }

    private int writeEntry(LogEntryHolderFuture entry) {
        try {
            CommitFileWriter writer = this.writer;

            if (writer == null) {
                throw new IOException("not yet writable");
            }

            long newSequenceNumber = ++writer.sequenceNumber;
            int written = writer.writeEntry(newSequenceNumber, entry.entry);

            if (writtenBytes > maxLogFileSize) {
                openNewLedger();
            }

            entry.done(new LogSequenceNumber(writer.ledgerId, newSequenceNumber));
            statsEntryLatency.registerSuccessfulEvent(System.currentTimeMillis() - entry.timestamp, TimeUnit.MILLISECONDS);

            return written;
        } catch (IOException | LogNotAvailableException err) {
            entry.error(err);

            statsEntryLatency.registerFailedEvent(System.currentTimeMillis() - entry.timestamp, TimeUnit.MILLISECONDS);
            return 0;
        }
    }

    private void flush() throws IOException {
        CommitFileWriter _writer = writer;
        if (_writer == null) {
            return;
        }
        _writer.flush();
    }

    private void synch() throws IOException {
        CommitFileWriter _writer = writer;
        if (_writer == null) {
            return;
        }
        _writer.sync();
    }

    public int getQueueSize() {
        return queueSize.get();
    }

    @Override
    public CommitLogResult log(LogEntry edit, boolean sync) throws LogNotAvailableException {
        if (failed) {
            throw new LogNotAvailableException("file commit log is failed");
        }
        boolean hasListeners = isHasListeners();
        if (hasListeners) {
            sync = true;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("log {}", edit);
        }
        LogEntryHolderFuture future = new LogEntryHolderFuture(edit, sync);
        try {
            queueSize.incrementAndGet();
            pendingEntries.incrementAndGet();
            writeQueue.put(future);
            if (!sync) {
                return new CommitLogResult(future.ack, false /* deferred */, false);
            } else {
                if (hasListeners) {
                    future.ack.thenAccept((pos) -> {
                        notifyListeners(pos, edit);
                    });
                }
                return new CommitLogResult(future.ack, false /* deferred */, true);
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new LogNotAvailableException(err);
        }

    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing) throws LogNotAvailableException {
        LOGGER.info("recovery {}, snapshotSequenceNumber: {}", new Object[]{snapshotSequenceNumber, tableSpaceName});
        // no lock is needed, we are at boot time
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDirectory)) {
            List<Path> names = new ArrayList<>();
            for (Path path : stream) {
                if (Files.isRegularFile(path)
                        && (path.getFileName() + "").endsWith(LOGFILEEXTENSION)) {
                    names.add(path);
                }
            }
            names.sort(Comparator.comparing(Path::toString));

            long offset = -1;
            for (Path p : names) {
                LOGGER.info("tablespace {}, logfile is {}", new Object[]{p.toAbsolutePath(), tableSpaceName});

                String name = (p.getFileName() + "").replace(LOGFILEEXTENSION, "");
                long ledgerId = Long.parseLong(name, 16);

                currentLedgerId = Math.max(currentLedgerId, ledgerId);
                offset = -1;

                try (CommitFileReader reader = new CommitFileReader(logDirectory, ledgerId)) {
                    LogEntryWithSequenceNumber n = reader.nextEntry();
                    while (n != null) {
                        offset = n.logSequenceNumber.offset;
                        if (n.logSequenceNumber.after(snapshotSequenceNumber)) {
                            LOGGER.debug("RECOVER ENTRY {}, {}", new Object[]{n.logSequenceNumber, n.entry});
                            consumer.accept(n.logSequenceNumber, n.entry);
                        } else {
                            LOGGER.debug("SKIP ENTRY {}, {}", new Object[]{n.logSequenceNumber, n.entry});
                        }
                        n = reader.nextEntry();
                    }
                }
            }

            recoveredLogSequence = new LogSequenceNumber(currentLedgerId, offset);

            LOGGER.info("Tablespace {}, max ledgerId is {}", new Object[]{currentLedgerId, tableSpaceName});
        } catch (IOException | RuntimeException err) {
            failed = true;
            throw new LogNotAvailableException(err);
        }

    }

    @Override
    public void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException {
        LOGGER.error("dropOldLedgers {} lastCheckPointSequenceNumber: {}, currentLedgerId: {}",
                new Object[]{lastCheckPointSequenceNumber, currentLedgerId, tableSpaceName});

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDirectory)) {
            List<Path> names = new ArrayList<>();
            for (Path path : stream) {
                if (Files.isRegularFile(path)
                        && (path.getFileName() + "").endsWith(LOGFILEEXTENSION)) {
                    names.add(path);
                }
            }

            names.sort(Comparator.comparing(Path::toString));

            final Path last = names.isEmpty() ? null : names.get(names.size() - 1);

            int count = 0;

            long ledgerLimit = Math.min(lastCheckPointSequenceNumber.ledgerId, currentLedgerId);

            for (Path path : names) {
                boolean lastFile = path.equals(last);

                String name = (path.getFileName() + "").replace(LOGFILEEXTENSION, "");
                try {
                    long ledgerId = Long.parseLong(name, 16);

                    if (!lastFile && ledgerId < ledgerLimit) {
                        LOGGER.error("deleting logfile {} for ledger {}", new Object[]{path.toAbsolutePath(), ledgerId});
                        try {
                            Files.delete(path);
                        } catch (IOException errorDelete) {
                            LOGGER.error("fatal error while deleting file " + path, errorDelete);
                            throw new LogNotAvailableException(errorDelete);
                        }
                        ++count;
                    }
                } catch (NumberFormatException notValid) {
                }
            }

            LOGGER.error("Deleted logfiles: {}", count);
        } catch (IOException err) {
            failed = true;
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void startWriting(int expectedReplicaCount) throws LogNotAvailableException {
        ensureDirectories();
        this.spool.start();
    }

    private void ensureDirectories() throws LogNotAvailableException {
        try {
            if (!Files.isDirectory(logDirectory)) {
                LOGGER.info("directory {} does not exist. creating", logDirectory);
                Files.createDirectories(logDirectory);
            }
        } catch (IOException err) {
            failed = true;
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void close() throws LogNotAvailableException {
        closed = true;
        onClose.accept(this);

        try {
            if (writeQueue.isEmpty()) {
                writeQueue.add(new LogEntryHolderFuture(null, false));
            }
            spool.join();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new LogNotAvailableException(err);
        }
        if (writer != null) {
            writer.close();
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

    @Override
    public void clear() throws LogNotAvailableException {
        try {
            FileUtils.cleanDirectory(logDirectory);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    CommitFileWriter getWriter() {
        return writer;
    }

    @Override
    public LogSequenceNumber getLastSequenceNumber() {
        final CommitFileWriter _writer = this.writer;
        if (_writer == null) {
            return (recoveredLogSequence == null) ? new LogSequenceNumber(currentLedgerId, -1) : recoveredLogSequence;
        } else {
            return new LogSequenceNumber(_writer.ledgerId, _writer.sequenceNumber);
        }
    }

}
