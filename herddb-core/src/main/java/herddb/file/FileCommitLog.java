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
import java.util.concurrent.ExecutionException;
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
import herddb.utils.EnsureLongIncrementAccumulator;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import herddb.utils.SimpleBufferedOutputStream;
import herddb.utils.SystemProperties;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.stats.Counter;
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
    private final OpStatsLogger statsEntryLatency;
    private final Counter queueSize;
    private final ExecutorService fsyncThreadPool;

    private final static int WRITE_QUEUE_SIZE = SystemProperties.getIntSystemProperty(
            "herddb.file.writequeuesize", 10_000_000);
    private final BlockingQueue<LogEntryHolderFuture> writeQueue = new LinkedBlockingQueue<>(WRITE_QUEUE_SIZE);

    private final static int MAX_UNSYNCHED_BATCH = SystemProperties.getIntSystemProperty(
            "herddb.file.maxsyncbatchsize", 10_000);

    private final static int MAX_SYNC_TIME = SystemProperties.getIntSystemProperty(
            "herddb.file.maxsynctime", 1);

    private final static boolean REQUIRE_FSYNC = SystemProperties.getBooleanSystemProperty(
            "herddb.file.requirefsync", true);

    public static final String LOGFILEEXTENSION = ".txlog";

    private volatile boolean closed = false;
    private volatile boolean failed = false;

    final static byte ENTRY_START = 13;
    final static byte ENTRY_END = 25;

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
            LOGGER.log(Level.SEVERE, "starting new file {0} for tablespace {1}", new Object[]{filename, tableSpaceName});

            this.channel = FileChannel.open(filename,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

            this.out = new ExtendedDataOutputStream(new SimpleBufferedOutputStream(Channels.newOutputStream(this.channel)));
            writtenBytes = 0;
        }

        public void writeEntry(long seqnumber, LogEntry edit) throws IOException {

            this.out.writeByte(ENTRY_START);
            this.out.writeLong(seqnumber);
            int written = edit.serialize(this.out);
            this.out.writeByte(ENTRY_END);
            writtenBytes += (1 + 8 + written + 1);
        }

        public void flush() throws IOException {
            this.out.flush();
        }

        public void sync() throws IOException {
            if (!REQUIRE_FSYNC) {
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
                if (entryStart != ENTRY_START) {
                    throw new IOException("corrupted txlog file");
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

        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    public FileCommitLog(Path logDirectory, String tableSpaceName, long maxLogFileSize, ExecutorService fsyncThreadPool, StatsLogger statslogger) {
        this.maxLogFileSize = maxLogFileSize;
        this.tableSpaceName = tableSpaceName;
        this.logDirectory = logDirectory.toAbsolutePath();
        this.spool = new Thread(new SpoolTask(), "commitlog-" + tableSpaceName);
        this.spool.setDaemon(true);
        this.statsFsyncTime = statslogger.getOpStatsLogger("fsync");
        this.statsEntryLatency = statslogger.getOpStatsLogger("entrylatency");
        this.queueSize = statslogger.getCounter("queuesize");
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
                        statsEntryLatency.registerSuccessfulEvent(now - e.entry.timestamp, TimeUnit.MILLISECONDS);
                        e.syncDone();
                    }
                }
                doneEntries = null;
            } catch (IOException t) {
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
                List<LogEntryHolderFuture> doneEntries = new ArrayList<>();
                while (!closed || !writeQueue.isEmpty()) {
                    LogEntryHolderFuture entry = writeQueue.poll(MAX_SYNC_TIME, TimeUnit.MILLISECONDS);
                    boolean timedOut = false;
                    if (entry != null) {
                        queueSize.dec();
                        writeEntry(entry);
                        doneEntries.add(entry);
                        count++;
                    } else {
                        timedOut = true;
                    }
                    if (timedOut || count >= MAX_UNSYNCHED_BATCH) {
                        if (!doneEntries.isEmpty()) {
                            flush();
                            List<LogEntryHolderFuture> entriesToNotify = doneEntries;
                            doneEntries = new ArrayList<>();
                            count = 0;
                            SyncTask syncTask = new SyncTask(entriesToNotify);
                            fsyncThreadPool.submit(syncTask);
                        }

                    }
                }
            } catch (LogNotAvailableException | IOException | InterruptedException t) {
                failed = true;
                LOGGER.log(Level.SEVERE, "general commit log failure on " + FileCommitLog.this.logDirectory, t);
            }
        }

    }

    private static class LogEntryHolderFuture {

        final CompletableFuture<LogSequenceNumber> ack = new CompletableFuture<>();
        final LogEntry entry;
        LogSequenceNumber sequenceNumber;
        Throwable error;
        final boolean sync;

        public LogEntryHolderFuture(LogEntry entry, boolean synch) {
            this.entry = entry;
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
            if (error != null) {
                ack.completeExceptionally(error);
            } else {
                ack.complete(sequenceNumber);
            }
        }

    }

    private void writeEntry(LogEntryHolderFuture entry) {
        try {
            CommitFileWriter writer = this.writer;

            if (writer == null) {
                throw new IOException("not yet writable");
            }

            long newSequenceNumber = ++writer.sequenceNumber;
            writer.writeEntry(newSequenceNumber, entry.entry);

            if (writtenBytes > maxLogFileSize) {
                openNewLedger();
            }

            entry.done(new LogSequenceNumber(writer.ledgerId, newSequenceNumber));
            if (!entry.sync) {
                statsEntryLatency.registerSuccessfulEvent(System.currentTimeMillis() - entry.entry.timestamp, TimeUnit.MILLISECONDS);
            }

        } catch (IOException | LogNotAvailableException err) {
            entry.error(err);
            if (!entry.sync) {
                statsEntryLatency.registerFailedEvent(System.currentTimeMillis() - entry.entry.timestamp, TimeUnit.MILLISECONDS);
            }
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

    public Counter getQueueSize() {
        return queueSize;
    }

    @Override
    public CommitLogResult log(LogEntry edit, boolean sync) throws LogNotAvailableException {
        if (failed) {
            throw new LogNotAvailableException("file commit log is failed");
        }
        if (isHasListeners()) {
            sync = true;
        }
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "log {0}", edit);
        }
        LogEntryHolderFuture future = new LogEntryHolderFuture(edit, sync);
        try {
            queueSize.inc();
            writeQueue.put(future);
            if (!sync) {
                // client is not really interested to the result of the write
                // sending a fake completed result
                return new CommitLogResult(
                        CompletableFuture.<LogSequenceNumber>completedFuture(null), true, false);
            } else {
                future.ack.thenAccept((pos) -> {
                    notifyListeners(pos, edit);
                });
                return new CommitLogResult(future.ack, false, true);
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
        try {
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
