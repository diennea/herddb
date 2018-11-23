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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.Channels;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import herddb.log.CommitLog;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.Fallocate;
import herddb.utils.FileUtils;
import herddb.utils.SimpleBufferedOutputStream;
import herddb.utils.SystemProperties;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Commit log on file
 *
 * @author enrico.olivelli
 */
public class FileCommitLog extends CommitLog {

    private static final Logger LOGGER = Logger.getLogger(FileCommitLog.class.getName());

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
    private final OpStatsLogger batchWriteSize;
    private final OpStatsLogger batchWriteBytes;
    private final Counter deferredSyncs;
    private final Counter newfiles;
    private final ExecutorService fsyncThreadPool;
    private final Consumer<FileCommitLog> onClose;

    private final static int WRITE_QUEUE_SIZE = SystemProperties.getIntSystemProperty(
            "herddb.file.writequeuesize", 10_000_000);

    private final static boolean USE_FALLOCATE = SystemProperties.getBooleanSystemProperty(
            "herddb.file.usefallocate", true);

    private final BlockingQueue<LogEntryHolderFuture> writeQueue = new LinkedBlockingQueue<>(WRITE_QUEUE_SIZE);

    private final int maxUnflushedBatchSize;
    private final int maxUnflushedBatchBytes;
    private final int maxSyncTime;
    private final boolean requireSync;

    public static final String LOGFILEEXTENSION = ".txlog";

    private volatile boolean closed = false;
    private volatile boolean failed = false;
    private volatile boolean needsSync = false;

    final static byte ENTRY_START = 13;
    final static byte ENTRY_END = 25;
    
    // assuming posix_fallocate will write zeros?
    final static byte ZERO_PADDING = 0;

    void backgroundSync() {
        if (needsSync) {
            try {
                getWriter().sync(true);
                deferredSyncs.inc();
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "error background fsync on " + this.logDirectory, t);
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
            LOGGER.log(Level.FINE, "starting new file {0} for tablespace {1}", new Object[]{filename, tableSpaceName});
            this.channel = FileChannel.open(filename,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            if (USE_FALLOCATE) {
                try {
                    long now = System.currentTimeMillis();
                    Fallocate
                            .forChannel(channel, maxLogFileSize)
                            .execute();
                    long end = System.currentTimeMillis();
                    LOGGER.log(Level.INFO, "fallocate " + filename + " success in " + (end - now) + " ms for " + maxLogFileSize + " bytes");
                } catch (Throwable t) {
                    LOGGER.log(Level.SEVERE, "Could not fallocate " + filename + ": " + t);
                }
            }
            this.out = new ExtendedDataOutputStream(new SimpleBufferedOutputStream(Channels.newOutputStream(this.channel)));
            writtenBytes = 0;
        }

        public int writeEntry(long seqnumber, byte[] entry) throws IOException {
            this.out.writeByte(ENTRY_START);
            this.out.writeLong(seqnumber);
            int written = entry.length;
            this.out.write(entry);
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
            statsFsyncTime.registerSuccessfulEvent(System.nanoTime() - now, TimeUnit.NANOSECONDS);
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

                // handle preallocated files
                if (entryStart == ZERO_PADDING) {
                    try {
                        byte next = in.readByte();
                        if (next == ZERO_PADDING) {
                            LOGGER.log(Level.INFO, "Detected end of pre-allocated file");
                            return null;
                        } else {
                            throw new IOException("corrupted txlog file, read " + entryStart + "," + next + " instead of a " + ENTRY_START);
                        }
                    } catch (EOFException ok) {
                        return null;
                    }
                }
                if (entryStart != ENTRY_START) {
                    throw new IOException("corrupted txlog file, read " + entryStart + " instead of a " + ENTRY_START);
                }
                long seqNumber = this.in.readLong();
                LogEntry edit = LogEntry.deserialize(this.in);
                int entryEnd = this.in.readByte();
                if (entryEnd != ENTRY_END) {
                    throw new IOException("corrupted txlog file");
                }
                return new LogEntryWithSequenceNumber(new LogSequenceNumber(ledgerId, seqNumber), edit);
            } catch (EOFException truncatedLog) {
                // if we hit EOF the entry has not been written, and so not acked, we can ignore it and say that the file is finished
                // it is important that this is the last file in the set                
                LOGGER.log(Level.SEVERE, "found unfinished entry in file " + this.ledgerId + ". entry was not acked. ignoring " + truncatedLog);
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
                LOGGER.log(Level.FINEST, "closing actual file {0}", writer.filename);
                writer.close();
            }

            writer = new CommitFileWriter(++currentLedgerId, -1);
            newfiles.inc();

        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    public FileCommitLog(Path logDirectory, String tableSpaceName,
            long maxLogFileSize, ExecutorService fsyncThreadPool, StatsLogger statslogger,
            Consumer<FileCommitLog> onClose,
            int maxUnflushedBatchSize,
            int maxUnflushedBatchBytes,
            int maxSyncTime,
            boolean requireSync
    ) {
        this.maxUnflushedBatchSize = maxUnflushedBatchSize;
        this.maxUnflushedBatchBytes = maxUnflushedBatchBytes;
        this.maxSyncTime = maxSyncTime;
        this.requireSync = requireSync;
        this.onClose = onClose;
        this.maxLogFileSize = maxLogFileSize;
        this.tableSpaceName = tableSpaceName;
        this.logDirectory = logDirectory.toAbsolutePath();
        this.spool = new Thread(new SpoolTask(), "commitlog-" + tableSpaceName);
        this.spool.setDaemon(true);
        this.statsFsyncTime = statslogger.getOpStatsLogger("fsync");
        this.statsEntryLatency = statslogger.getOpStatsLogger("entrylatency");
        this.batchWriteSize = statslogger.getOpStatsLogger("batchWriteSize");
        this.batchWriteBytes = statslogger.getOpStatsLogger("batchWriteBytes");
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
        LOGGER.log(Level.FINE, "tablespace {2}, logdirectory: {0}, maxLogFileSize {1} bytes", new Object[]{logDirectory, maxLogFileSize, tableSpaceName});
    }

    private class SyncTask implements Runnable {

        private List<LogEntryHolderFuture> doneEntries;

        private SyncTask(List<LogEntryHolderFuture> doneEntries) {
            this.doneEntries = doneEntries;
        }

        @Override
        public void run() {
            try {
                long now = System.currentTimeMillis();
                synch();
                for (LogEntryHolderFuture e : doneEntries) {
                    if (e.sync) {
                        statsEntryLatency.registerSuccessfulEvent(now - e.timestamp, TimeUnit.MILLISECONDS);
                        e.syncDone();
                    }
                }
                doneEntries = null;
            } catch (Throwable t) {
                failed = true;
                LOGGER.log(Level.SEVERE, "general commit log failure on " + FileCommitLog.this.logDirectory, t);
            }
        }

    };

    private class SpoolTask implements Runnable {

        @Override
        public void run() {
            try {
                openNewLedger();
                int count = 0;
                long unwrittenBytes = 0;
                List<LogEntryHolderFuture> batch = new ArrayList<>();
                while (!closed || !writeQueue.isEmpty()) {
                    LogEntryHolderFuture entry = writeQueue.poll(maxSyncTime, TimeUnit.MILLISECONDS);
                    boolean timedOut = false;
                    if (entry != null) {
                        if (entry.entry == null) {
                            // force close placeholder
                            break;
                        }

                        batch.add(entry);
                        count++;
                        unwrittenBytes += entry.entry.length;
                    } else {
                        timedOut = true;
                    }
                    if (timedOut || count >= maxUnflushedBatchSize || unwrittenBytes >= maxUnflushedBatchBytes) {
                        List<LogEntryHolderFuture> entriesToNotify = flushBatch(batch);
                        batch = new ArrayList<>();
                        count = 0;
                        unwrittenBytes = 0;
                        SyncTask syncTask = new SyncTask(entriesToNotify);
                        fsyncThreadPool.submit(syncTask);
                    }
                }
                List<LogEntryHolderFuture> entriesToNotify = flushBatch(batch);
                if (entriesToNotify.isEmpty()) {
                    LOGGER.log(Level.INFO, "flushing last {0} entries", entriesToNotify.size());
                    SyncTask syncTask = new SyncTask(entriesToNotify);
                    syncTask.run();
                }

            } catch (LogNotAvailableException | IOException | InterruptedException t) {
                failed = true;
                LOGGER.log(Level.SEVERE, "general commit log failure on " + FileCommitLog.this.logDirectory, t);
            }
        }

        private List<LogEntryHolderFuture> flushBatch(List<LogEntryHolderFuture> batch) throws LogNotAvailableException, IOException, InterruptedException {
            if (!batch.isEmpty()) {
                int size = 0;
                List<LogEntryHolderFuture> entriesToWrite = batch;
                for (LogEntryHolderFuture _entry : entriesToWrite) {
                    queueSize.decrementAndGet();
                    size += writeEntry(_entry);
                }
                batchWriteSize.registerSuccessfulValue(entriesToWrite.size());
                batchWriteBytes.registerSuccessfulValue(size);
                flush();
                return entriesToWrite;
            } else {
                return Collections.emptyList();
            }
        }
    }

    private class LogEntryHolderFuture {

        final CompletableFuture<LogSequenceNumber> ack = new CompletableFuture<>();
        final byte[] entry;
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
                this.entry = entry.serialize();
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

    private long writeEntry(LogEntryHolderFuture entry) {
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
            if (!entry.sync) {
                statsEntryLatency.registerSuccessfulEvent(System.currentTimeMillis() - entry.timestamp, TimeUnit.MILLISECONDS);
            }
            return written;
        } catch (IOException | LogNotAvailableException err) {
            entry.error(err);
            if (!entry.sync) {
                statsEntryLatency.registerFailedEvent(System.currentTimeMillis() - entry.timestamp, TimeUnit.MILLISECONDS);
            }
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
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "log {0}", edit);
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
    public void followTheLeader(LogSequenceNumber skipPast, BiConsumer<LogSequenceNumber, LogEntry> consumer) throws LogNotAvailableException {
        // we are always the leader!
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing) throws LogNotAvailableException {
        LOGGER.log(Level.INFO, "recovery {1}, snapshotSequenceNumber: {0}", new Object[]{snapshotSequenceNumber, tableSpaceName});
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
                LOGGER.log(Level.INFO, "tablespace {1}, logfile is {0}", new Object[]{p.toAbsolutePath(), tableSpaceName});

                String name = (p.getFileName() + "").replace(LOGFILEEXTENSION, "");
                long ledgerId = Long.parseLong(name, 16);

                currentLedgerId = Math.max(currentLedgerId, ledgerId);
                offset = -1;

                try (CommitFileReader reader = new CommitFileReader(logDirectory, ledgerId)) {
                    LogEntryWithSequenceNumber n = reader.nextEntry();
                    while (n != null) {
                        offset = n.logSequenceNumber.offset;
                        if (n.logSequenceNumber.after(snapshotSequenceNumber)) {
                            LOGGER.log(Level.FINE, "RECOVER ENTRY {0}, {1}", new Object[]{n.logSequenceNumber, n.entry});
                            consumer.accept(n.logSequenceNumber, n.entry);
                        } else {
                            LOGGER.log(Level.FINE, "SKIP ENTRY {0}, {1}", new Object[]{n.logSequenceNumber, n.entry});
                        }
                        n = reader.nextEntry();
                    }
                }
            }

            recoveredLogSequence = new LogSequenceNumber(currentLedgerId, offset);

            LOGGER.log(Level.INFO, "Tablespace {1}, max ledgerId is {0}", new Object[]{currentLedgerId, tableSpaceName});
        } catch (IOException | RuntimeException err) {
            failed = true;
            throw new LogNotAvailableException(err);
        }

    }

    @Override
    public void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException {
        LOGGER.log(Level.SEVERE, "dropOldLedgers {2} lastCheckPointSequenceNumber: {0}, currentLedgerId: {1}",
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
                        LOGGER.log(Level.SEVERE, "deleting logfile {0} for ledger {1}", new Object[]{path.toAbsolutePath(), ledgerId});
                        try {
                            Files.delete(path);
                        } catch (IOException errorDelete) {
                            LOGGER.log(Level.SEVERE, "fatal error while deleting file " + path, errorDelete);
                            throw new LogNotAvailableException(errorDelete);
                        }
                        ++count;
                    }
                } catch (NumberFormatException notValid) {
                }
            }

            LOGGER.log(Level.SEVERE, "Deleted logfiles: {0}", count);
        } catch (IOException err) {
            failed = true;
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void startWriting() throws LogNotAvailableException {
        ensureDirectories();
        this.spool.start();
    }

    private void ensureDirectories() throws LogNotAvailableException {
        try {
            if (!Files.isDirectory(logDirectory)) {
                LOGGER.log(Level.INFO, "directory {0} does not exist. creating", logDirectory);
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
