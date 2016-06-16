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
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.FileUtils;
import herddb.utils.SimpleBufferedOutputStream;
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Commit log on file
 *
 * @author enrico.olivelli
 */
public class FileCommitLog extends CommitLog {

    private static final Logger LOGGER = Logger.getLogger(FileCommitLog.class.getName());

    private Path logDirectory;

    private long currentLedgerId = 0;
    private long lastSequenceNumber = -1;
    private long maxLogFileSize = 1024 * 1024;
    private long writtenBytes = 0;

    private volatile CommitFileWriter writer;

    private final ReentrantLock writeLock = new ReentrantLock();

    private final static byte ENTRY_START = 13;
    private final static byte ENTRY_END = 25;

    private class CommitFileWriter implements AutoCloseable {

        ExtendedDataOutputStream out;
        Path filename;
        FileOutputStream fOut;

        private CommitFileWriter(long ledgerId) throws IOException {
            filename = logDirectory.resolve(String.format("%016x", ledgerId) + LOGFILEEXTENSION).toAbsolutePath();
            // in case of IOException the stream is not opened, not need to close it
            LOGGER.log(Level.SEVERE, "starting new file {0} ", filename);
            this.fOut = new FileOutputStream(filename.toFile(), false);
            this.out = new ExtendedDataOutputStream(new SimpleBufferedOutputStream(this.fOut));
            writtenBytes = 0;
        }

        public void writeEntry(long seqnumber, LogEntry edit, boolean synch) throws IOException {

            this.out.writeByte(ENTRY_START);
            this.out.writeLong(seqnumber);
            int written = edit.serialize(this.out);
            this.out.writeByte(ENTRY_END);
            if (synch) {
                this.out.flush();
                this.fOut.getFD().sync();
            }
            writtenBytes += (1 + 8 + written + 1);
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

        private CommitFileReader(long ledgerId) throws IOException {
            this.ledgerId = ledgerId;
            Path filename = logDirectory.resolve(String.format("%016x", ledgerId) + LOGFILEEXTENSION);
            // in case of IOException the stream is not opened, not need to close it
            this.in = new ExtendedDataInputStream(new BufferedInputStream(Files.newInputStream(filename, StandardOpenOption.READ), 64 * 1024));
        }

        public LogEntryWithSequenceNumber nextEntry() throws IOException {
            byte entryStart;
            try {
                entryStart = in.readByte();
            } catch (EOFException okEnd) {
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
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    private void openNewLedger() throws LogNotAvailableException {
        writeLock.lock();
        try {
            if (writer != null) {
                LOGGER.log(Level.SEVERE, "closing actual file {0}", writer.filename);
                writer.close();
            }
            ensureDirectories();
            currentLedgerId++;
            lastSequenceNumber = -1;
            writer = new CommitFileWriter(currentLedgerId);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        } finally {
            writeLock.unlock();
        }
    }

    public FileCommitLog(Path logDirectory, long maxLogFileSize) {
        this.maxLogFileSize = maxLogFileSize;
        this.logDirectory = logDirectory.toAbsolutePath();
        LOGGER.log(Level.SEVERE, "logdirectory: {0}, maxLogFileSize {1} bytes", new Object[]{logDirectory, maxLogFileSize});
    }

    @Override
    public LogSequenceNumber log(LogEntry edit, boolean synch) throws LogNotAvailableException {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "log {0}", edit);
        }
        writeLock.lock();
        try {
            if (writer == null) {
                throw new LogNotAvailableException(new Exception("not yet writable"));
            }
            long newSequenceNumber = ++lastSequenceNumber;
            writer.writeEntry(newSequenceNumber, edit, synch);
            if (writtenBytes > maxLogFileSize) {
                openNewLedger();
            }
            return new LogSequenceNumber(currentLedgerId, newSequenceNumber);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public List<LogSequenceNumber> log(List<LogEntry> entries, boolean synch) throws LogNotAvailableException {
        List<LogSequenceNumber> res = new ArrayList<>();
        int size = entries.size();
        for (int i = 0; i < size; i++) {
            boolean synchLast = i == size - 1;
            res.add(log(entries.get(i), synchLast));
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
            for (Path p : names) {
                LOGGER.log(Level.SEVERE, "logfile is {0}", p.toAbsolutePath());
                String name = p.getFileName().toString().replace(LOGFILEEXTENSION, "");
                long ledgerId = Long.parseLong(name, 16);
                if (ledgerId > currentLedgerId) {
                    currentLedgerId = ledgerId;
                }
                try (CommitFileReader reader = new CommitFileReader(ledgerId)) {
                    LogEntryWithSequenceNumber n = reader.nextEntry();
                    while (n != null) {
                        lastSequenceNumber = n.logSequenceNumber.offset;
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
            LOGGER.log(Level.SEVERE, "Max ledgerId is {0}", new Object[]{currentLedgerId});
        } catch (Exception err) {
            throw new LogNotAvailableException(err);
        }

    }

    @Override
    public void startWriting() throws LogNotAvailableException {
        openNewLedger();
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
        if (writer != null) {
            try {
                writer.close();
            } finally {
                closed = true;
            }
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
        return new LogSequenceNumber(currentLedgerId, lastSequenceNumber);
    }

}
