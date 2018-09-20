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
package herddb.index.rocksdb;

import herddb.core.AbstractIndexManager;
import herddb.core.HerdDBInternalException;
import herddb.core.PostCheckpointAction;
import herddb.index.IndexOperation;
import herddb.index.KeyToPageIndex;
import herddb.index.PrimaryIndexPrefixScan;
import herddb.index.PrimaryIndexRangeScan;
import herddb.index.PrimaryIndexSeek;
import herddb.log.LogSequenceNumber;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.sql.SQLRecordKeyFunction;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import herddb.utils.FileUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

/**
 * Implementation of {@link KeyToPageIndex} with a backing RocksDB
 *
 * @author enrico.olivelli
 */
public class RocksDBKeyToPageIndex implements KeyToPageIndex {

    static {
        RocksDB.loadLibrary();
    }

    private final File path;
    private final Options options;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private RocksDB db;

    public RocksDBKeyToPageIndex(File path, String tableName) {
        this.path = path;
        this.options = new Options();
        this.writeOptions = new WriteOptions();
        this.readOptions = new ReadOptions();
        this.readOptions.setVerifyChecksums(false);
    }

    @Override
    public long getUsedMemory() {
        return 0;
    }

    @Override
    public boolean requireLoadAtStartup() {
        return false;
    }

    @Override
    public long size() {
        try {
            return db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException ex) {
            throw new HerdDBInternalException(ex);
        }
    }

    @Override
    public void start(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        try {
            Files.createDirectories(path.toPath());
            if (sequenceNumber.equals(LogSequenceNumber.START_OF_TIME)) {
                options.setCreateIfMissing(true);
            }
            File directory = checkpointDirectory(sequenceNumber);
            if (directory.isDirectory()) {
                LOG.log(Level.INFO, "Starting PK from checkpoint at {0}", directory.getAbsolutePath());
                if (sequenceNumber.equals(LogSequenceNumber.START_OF_TIME)) {
                    // at START_OF_TIME it is impossible to have data
                    // this is the first work directory of the index
                    // if the server wants to boot at START_OF_TIME
                    // the index must always be empty
                    FileUtils.cleanDirectory(directory.toPath());
                }
                db = RocksDB.open(options, directory.getAbsolutePath());
            } else {
                LOG.log(Level.INFO, "No PK checkpoint at {0}", directory.getAbsolutePath());
                LOG.log(Level.INFO, "Starting PK at {0}", directory.getAbsolutePath());
                db = RocksDB.open(options, directory.getAbsolutePath());
            }
            LOG.log(Level.INFO, "Estimated num records {0}", size());

        } catch (RocksDBException | IOException ex) {
            writeOptions.close();
            options.close();
            readOptions.close();
            throw new DataStorageManagerException(ex);
        }
    }
    private static final Logger LOG = Logger.getLogger(RocksDBKeyToPageIndex.class.getName());

    @Override
    public void close() {
        if (db != null) {
            db.close();
        }
        options.close();
        writeOptions.close();
        readOptions.close();
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
        try {
            Checkpoint cp = Checkpoint.create(db);
            File directory = checkpointDirectory(sequenceNumber);
            if (directory.isDirectory()) {
                LOG.log(Level.INFO, "checkpoint already exists at {0}", directory.getAbsolutePath());
            } else {
                cp.createCheckpoint(directory.getAbsolutePath());
            }
            return Collections.emptyList();
        } catch (RocksDBException ex) {
            throw new DataStorageManagerException(ex);
        }
    }

    private File checkpointDirectory(LogSequenceNumber sequenceNumber) {
        File directory = new File(path, "checkpoint_" + sequenceNumber.ledgerId + "_" + sequenceNumber.offset);
        return directory;
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void truncate() {
        byte[] first = null;
        byte[] last = null;
        try (RocksIterator it = db.newIterator()) {
            it.seekToFirst();
            if (it.isValid()) {
                first = it.key();
                last = first;
            }
            it.seekToLast();
            if (it.isValid()) {
                last = it.key();
            }
        }
        if (first != null) {
            last = new Bytes(last).next().data;
            LOG.log(Level.INFO, "truncate table at {0} - {1}", new Object[]{Bytes.arraytohexstring(first), Bytes.arraytohexstring(last)});

            try {
                db.deleteRange(first, last);
            } catch (RocksDBException ex) {
                throw new DataStorageManagerException(ex);
            }
        } else {
            LOG.info("no record to truncate");
        }
    }

    @Override
    public Stream<Map.Entry<Bytes, Long>> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext, AbstractIndexManager index) throws DataStorageManagerException, StatementExecutionException {
        if (operation instanceof PrimaryIndexSeek) {
            PrimaryIndexSeek seek = (PrimaryIndexSeek) operation;
            byte[] seekValue = seek.value.computeNewValue(null, context, tableContext);
            if (seekValue == null) {
                return Stream.empty();
            }
            Bytes key = Bytes.from_array(seekValue);
            Long pageId = get(key);
            if (pageId == null) {
                return Stream.empty();
            }
            return Stream.of(new AbstractMap.SimpleImmutableEntry<>(key, pageId));
        }

        if (operation instanceof PrimaryIndexPrefixScan) {

            PrimaryIndexPrefixScan scan = (PrimaryIndexPrefixScan) operation;
//            SQLRecordKeyFunction value = sis.value;
            byte[] refvalue = scan.value.computeNewValue(null, context, tableContext);
            Bytes firstKey = Bytes.from_array(refvalue);
            Bytes lastKey = firstKey.next();

            return rangeScan(firstKey, lastKey);
        }

        // Remember that the IndexOperation can return more records
        // every predicate (WHEREs...) will always be evaluated anyway on every record, in order to guarantee correctness
        if (index != null) {
            return index.recordSetScanner(operation, context, tableContext, this);
        }
        if (operation == null) {
            try (RocksIterator iter = db.newIterator()) {
                iter.seekToFirst();
                // this is only a Proof-of-concept, a real impl will wrap the iterator
                List<Map.Entry<Bytes, Long>> dummy = new ArrayList<>();
                while (iter.isValid()) {
                    Map.Entry<Bytes, Long> entry = new AbstractMap.SimpleImmutableEntry<>(Bytes.from_array(iter.key()), Bytes.toLong(iter.value(), 0));
                    dummy.add(entry);
                    iter.next();
                }
                return dummy.stream();
            }

        } else if (operation instanceof PrimaryIndexRangeScan) {
            Bytes refminvalue;
            PrimaryIndexRangeScan sis = (PrimaryIndexRangeScan) operation;
            SQLRecordKeyFunction minKey = sis.minValue;
            if (minKey != null) {
                refminvalue = Bytes.from_array(minKey.computeNewValue(null, context, tableContext));
            } else {
                refminvalue = null;
            }
            Bytes refmaxvalue;
            SQLRecordKeyFunction maxKey = sis.maxValue;
            if (maxKey != null) {
                refmaxvalue = Bytes.from_array(maxKey.computeNewValue(null, context, tableContext));
            } else {
                refmaxvalue = null;
            }
            return rangeScan(refminvalue, refmaxvalue);
        }
        throw new DataStorageManagerException("operation " + operation + " not implemented on " + this.getClass());

    }

    private Stream<Map.Entry<Bytes, Long>> rangeScan(Bytes refminvalue, Bytes refmaxvalue) {
        try (RocksIterator iter = db.newIterator()) {
            if (refminvalue != null) {
                iter.seekForPrev(refminvalue.data);
            } else {
                iter.seekToFirst();
            }
            // this is only a Proof-of-concept, a real impl will wrap the iterator
            List<Map.Entry<Bytes, Long>> _dummy = new ArrayList<>();
            while (iter.isValid()) {
                byte[] key = iter.key();
                if (refmaxvalue != null
                        && Bytes.compare(key, refmaxvalue.data) > 0) {
                    break;
                }
                Map.Entry<Bytes, Long> entry = new AbstractMap.SimpleImmutableEntry<>(Bytes.from_array(iter.key()), Bytes.toLong(iter.value(), 0));
                _dummy.add(entry);
                iter.next();
            }
            return _dummy.stream();
        }
    }

    @Override
    public void put(Bytes key, Long currentPage) {
        try {
            db.put(writeOptions, key.data, Bytes.longToByteArray(currentPage));
        } catch (RocksDBException ex) {
            throw new HerdDBInternalException(ex);
        }
    }
    private static final byte[] dummy = new byte[1];

    @Override
    public boolean containsKey(Bytes key) {
        try {
            return db.get(readOptions, key.data, dummy) != RocksDB.NOT_FOUND;
        } catch (RocksDBException ex) {
            throw new HerdDBInternalException(ex);
        }
    }

    @Override
    public Long get(Bytes key) {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(8);
        try {
            if (buffer.arrayOffset() == 0 && buffer.hasArray()) {
                int found = db.get(readOptions, key.data, buffer.array());
                return found != RocksDB.NOT_FOUND ? Bytes.toLong(buffer.array(), 0) : null;
            } else {
                byte[] res = db.get(key.data);
                return res != null ? Bytes.toLong(res, 0) : null;
            }
        } catch (RocksDBException ex) {
            throw new HerdDBInternalException(ex);
        } finally {
            buffer.release();
        }
    }

    @Override
    public Long remove(Bytes key) {
        try {
            Long prev = get(key);
            db.delete(writeOptions, key.data);
            return prev;
        } catch (RocksDBException ex) {
            throw new HerdDBInternalException(ex);
        }
    }

    @Override
    public boolean isSortedAscending() {
        return true;
    }

}
