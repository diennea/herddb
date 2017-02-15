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
import java.io.FileOutputStream;
import java.io.IOException;
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
import herddb.log.CommitLogListener;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import herddb.utils.SimpleBufferedOutputStream;
import herddb.utils.SystemProperties;

/**
 * Commit log on file
 *
 * @author enrico.olivelli
 */
public class FileCommitLog extends CommitLog {

    private static final Logger LOGGER = Logger.getLogger(FileCommitLog.class.getName());

    private Path logDirectory;

    private long currentLedgerId = 0;

    private LogSequenceNumber recoveredLogSequence;

    private long maxLogFileSize = 1024 * 1024;
    private long writtenBytes = 0;

    private volatile CommitFileWriter writer;
    private Thread spool;

    private final int WRITE_QUEUE_SIZE = SystemProperties.getIntSystemProperty(
        FileCommitLog.class.getName() + ".writequeuesize", 100000);
    private final BlockingQueue<LogEntryHolderFuture> writeQueue = new LinkedBlockingQueue<>(WRITE_QUEUE_SIZE);

    private final int MAX_UNSYNCHED_BATCH = SystemProperties.getIntSystemProperty(
        FileCommitLog.class.getName() + ".maxsynchbatchsize", 1000);

    private final int MAX_SYNCH_TIME = SystemProperties.getIntSystemProperty(
        FileCommitLog.class.getName() + ".maxsynchtime", 1);

    private final boolean REQUIRE_FSYNCH = SystemProperties.getBooleanSystemProperty(
        FileCommitLog.class.getName() + ".requirefsynch", true);

    private final static byte ENTRY_START = 13;
    private final static byte ENTRY_END = 25;

    private class CommitFileWriter implements AutoCloseable {

        final long ledgerId;
        long sequenceNumber;

        final ExtendedDataOutputStream out;
        final Path filename;
        final FileOutputStream fOut;

        private CommitFileWriter(long ledgerId, long sequenceNumber) throws IOException {
            this.ledgerId = ledgerId;
            this.sequenceNumber = sequenceNumber;

            filename = logDirectory.resolve(String.format("%016x", ledgerId) + LOGFILEEXTENSION).toAbsolutePath();
            // in case of IOException the stream is not opened, not need to close it
            LOGGER.log(Level.SEVERE, "starting new file {0} ", filename);
            this.fOut = new FileOutputStream(filename.toFile(), false);
            this.out = new ExtendedDataOutputStream(new SimpleBufferedOutputStream(this.fOut));
            writtenBytes = 0;
        }

        public void writeEntry(long seqnumber, LogEntry edit) throws IOException {

            this.out.writeByte(ENTRY_START);
            this.out.writeLong(seqnumber);
            int written = edit.serialize(this.out);
            this.out.writeByte(ENTRY_END);
            writtenBytes += (1 + 8 + written + 1);
        }

        public void synch() throws IOException {
            if (!REQUIRE_FSYNCH) {
                return;
            }
            this.out.flush();
            this.fOut.getFD().sync();
        }

        @Override
        public void close() throws LogNotAvailableException {
            try {
                out.close();
                fOut.close();
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }
        }
    }

    private static final class LogEntryWithSequenceNumber {

        LogSequenceNumber logSequenceNumber;
        LogEntry entry;

        public LogEntryWithSequenceNumber(LogSequenceNumber logSequenceNumber, LogEntry entry) {
            this.logSequenceNumber = logSequenceNumber;
            this.entry = entry;
        }

    }

    private class CommitFileReader implements AutoCloseable {

        ExtendedDataInputStream in;
        long ledgerId;
        boolean lastFile;

        private CommitFileReader(long ledgerId, boolean lastFile) throws IOException {
            this.ledgerId = ledgerId;
            this.lastFile = lastFile;
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
                if (lastFile) {
                    LOGGER.log(Level.SEVERE, "found unfinished entry in file " + this.ledgerId + ". entry was not acked. ignoring " + truncatedLog);
                    return null;
                } else {
                    throw truncatedLog;
                }
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
            ensureDirectories();

            writer = new CommitFileWriter(++currentLedgerId, -1);

        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    public FileCommitLog(Path logDirectory, long maxLogFileSize) {
        this.maxLogFileSize = maxLogFileSize;
        this.logDirectory = logDirectory.toAbsolutePath();
        this.spool = new Thread(new SpoolTask(), "commitlog-" + logDirectory);
        this.spool.setDaemon(true);
        LOGGER.log(Level.SEVERE, "logdirectory: {0}, maxLogFileSize {1} bytes", new Object[]{logDirectory, maxLogFileSize});
    }

    private class SpoolTask implements Runnable {

        @Override
        public void run() {
            try {
                openNewLedger();
                int count = 0;
                List<LogEntryHolderFuture> doneEntries = new ArrayList<>();
                while (!closed || !writeQueue.isEmpty()) {
                    LogEntryHolderFuture entry = writeQueue.poll(MAX_SYNCH_TIME, TimeUnit.MILLISECONDS);
                    boolean timedOut = false;
                    if (entry != null) {
                        writeEntry(entry);
                        doneEntries.add(entry);
                        count++;
                    } else {
                        timedOut = true;
                    }
                    if (timedOut || count >= MAX_UNSYNCHED_BATCH) {
                        count = 0;
                        if (!doneEntries.isEmpty()) {
                            synch();
                            for (LogEntryHolderFuture e : doneEntries) {
                                if (e.synch) {
                                    e.synchDone();
                                }
                            }
                            doneEntries.clear();
                        }

                    }
                }
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "general commit log failure on " + FileCommitLog.this.logDirectory);
            }
        }

    }

    private static class LogEntryHolderFuture {

        final CompletableFuture<LogSequenceNumber> ack = new CompletableFuture<>();
        final LogEntry entry;
        LogSequenceNumber sequenceNumber;
        Throwable error;
        final boolean synch;

        public LogEntryHolderFuture(LogEntry entry, boolean synch) {
            this.entry = entry;
            this.synch = synch;
        }

        public void error(Throwable error) {
            this.error = error;
            if (!synch) {
                synchDone();
            }
        }

        public void done(LogSequenceNumber sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            if (!synch) {
                synchDone();
            }
        }

        private void synchDone() {
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
        } catch (IOException | LogNotAvailableException err) {
            entry.error(err);
        }
    }

    private void synch() throws IOException {
        if (writer == null) {
            return;
        }
        writer.synch();
    }

    @Override
    public LogSequenceNumber log(LogEntry edit, boolean synch) throws LogNotAvailableException {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "log {0}", edit);
        }
        LogEntryHolderFuture future = new LogEntryHolderFuture(edit, synch);
        try {
            writeQueue.put(future);
            LogSequenceNumber logPos = future.ack.get();
            if (listeners != null) {
                for (CommitLogListener l : listeners) {
                    l.logEntry(logPos, edit);
                }
            }
            return logPos;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new LogNotAvailableException(err);
        } catch (ExecutionException err) {
            throw new LogNotAvailableException(err.getCause());
        }

    }

    @Override
    public List<LogSequenceNumber> log(List<LogEntry> entries, boolean synch) throws LogNotAvailableException {
        List<LogSequenceNumber> res = new ArrayList<>();
        int size = entries.size();
        for (int i = 0; i < size; i++) {
            boolean synchLast = i == size - 1;
            LogEntry entry = entries.get(i);
            LogSequenceNumber logpos = log(entry, synchLast);
            if (listeners != null) {
                for (CommitLogListener l : listeners) {
                    l.logEntry(logpos, entry);
                }
            }
            res.add(logpos);
        }
        return res;
    }

    @Override
    public void followTheLeader(LogSequenceNumber skipPast, BiConsumer<LogSequenceNumber, LogEntry> consumer) throws LogNotAvailableException {
        // we are always the leader!
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing) throws LogNotAvailableException {
        LOGGER.log(Level.SEVERE, "recovery, snapshotSequenceNumber: {0}", snapshotSequenceNumber);
        // no lock is needed, we are at boot time
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDirectory)) {
            List<Path> names = new ArrayList<>();
            for (Path path : stream) {
                if (Files.isRegularFile(path) && path.getFileName().toString().endsWith(LOGFILEEXTENSION)) {
                    names.add(path);
                }
            }
            names.sort(Comparator.comparing(Path::toString));

            final Path last = names.isEmpty() ? null : names.get(names.size() - 1);

            long offset = -1;
            for (Path p : names) {

                boolean lastFile = p.equals(last);

                LOGGER.log(Level.SEVERE, "logfile is {0}, lastFile {1}", new Object[]{p.toAbsolutePath(), lastFile});

                String name = p.getFileName().toString().replace(LOGFILEEXTENSION, "");
                long ledgerId = Long.parseLong(name, 16);

                currentLedgerId = Math.max(currentLedgerId, ledgerId);
                offset = -1;

                try (CommitFileReader reader = new CommitFileReader(ledgerId, lastFile)) {
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

            LOGGER.log(Level.SEVERE, "Max ledgerId is {0}", new Object[]{currentLedgerId});
        } catch (Exception err) {
            throw new LogNotAvailableException(err);
        }

    }

    @Override
    public void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException {
        LOGGER.log(Level.SEVERE, "dropOldLedgers lastCheckPointSequenceNumber: {0}, currentLedgerId: {2}",
            new Object[]{lastCheckPointSequenceNumber, currentLedgerId});

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDirectory)) {
            List<Path> names = new ArrayList<>();
            for (Path path : stream) {
                if (Files.isRegularFile(path) && path.getFileName().toString().endsWith(LOGFILEEXTENSION)) {
                    names.add(path);
                }
            }

            names.sort(Comparator.comparing(Path::toString));

            final Path last = names.isEmpty() ? null : names.get(names.size() - 1);

            int count = 0;

            long ledgerLimit = Math.min(lastCheckPointSequenceNumber.ledgerId, currentLedgerId);

            for (Path path : names) {
                boolean lastFile = path.equals(last);

                String name = path.getFileName().toString().replace(LOGFILEEXTENSION, "");
                long ledgerId = Long.parseLong(name, 16);

                if (!lastFile && ledgerId < ledgerLimit) {
                    LOGGER.log(Level.SEVERE, "deleting logfile {0} for ledger {1}", new Object[]{path.toAbsolutePath(), ledgerId});
                    Files.delete(path);
                    ++count;
                }
            }

            LOGGER.log(Level.SEVERE, "Deleted logfiles: {}", count);
        } catch (Exception err) {
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void startWriting() throws LogNotAvailableException {
        this.spool.start();
    }

    private void ensureDirectories() throws LogNotAvailableException {
        try {
            if (!Files.isDirectory(logDirectory)) {
                LOGGER.log(Level.SEVERE, "directory " + logDirectory + " does not exist. creating");
                Files.createDirectories(logDirectory);
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    private static final String LOGFILEEXTENSION = ".txlog";

    private volatile boolean closed = false;

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
    public void clear() throws LogNotAvailableException {
        try {
            FileUtils.cleanDirectory(logDirectory);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public LogSequenceNumber getLastSequenceNumber() {
        final CommitFileWriter writer = this.writer;
        if (writer == null) {
            return (recoveredLogSequence == null) ? new LogSequenceNumber(currentLedgerId, -1) : recoveredLogSequence;
        } else {
            return new LogSequenceNumber(writer.ledgerId, writer.sequenceNumber);
        }
    }

}
