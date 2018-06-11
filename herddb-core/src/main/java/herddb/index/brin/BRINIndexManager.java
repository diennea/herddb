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
package herddb.index.brin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import herddb.codec.RecordSerializer;
import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.HerdDBInternalException;
import herddb.core.MemoryManager;
import herddb.core.PageReplacementPolicy;
import herddb.core.PostCheckpointAction;
import herddb.core.TableSpaceManager;
import herddb.index.IndexOperation;
import herddb.index.SecondaryIndexPrefixScan;
import herddb.index.SecondaryIndexRangeScan;
import herddb.index.SecondaryIndexSeek;
import herddb.log.CommitLog;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.sql.SQLRecordKeyFunction;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;

/**
 * Block-range like index with pagination managed by a {@link PageReplacementPolicy}
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class BRINIndexManager extends AbstractIndexManager {

    private static final Logger LOGGER = Logger.getLogger(BRINIndexManager.class.getName());
    LogSequenceNumber bootSequenceNumber;
    private final AtomicLong newPageId = new AtomicLong(1);
    private final BlockRangeIndex<Bytes, Bytes> data;
    private final IndexDataStorage<Bytes, Bytes> storageLayer = new IndexDataStorageImpl();

    public BRINIndexManager(Index index, MemoryManager memoryManager, AbstractTableManager tableManager, CommitLog log, DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID, long transaction) {
        super(index, tableManager, dataStorageManager, tableSpaceManager.getTableSpaceUUID(), log, transaction);
        this.data = new BlockRangeIndex<>(memoryManager.getMaxLogicalPageSize(), memoryManager.getDataPageReplacementPolicy(), storageLayer);
    }

    private static final class PageContents {

        public static final int TYPE_METADATA = 9;
        public static final int TYPE_BLOCKDATA = 10;

        private int type;
        private List<Map.Entry<Bytes, Bytes>> pageData;
        private List<BlockRangeIndexMetadata.BlockMetadata<Bytes>> metadata;

        byte[] serialize() throws IOException {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
                 ExtendedDataOutputStream eout = new ExtendedDataOutputStream(out)) {
                 serialize(eout);

                 eout.flush();

                 return out.toByteArray();
            }
        }

        void serialize(ExtendedDataOutputStream out) throws IOException {
            out.writeVLong(2); // version
            out.writeVLong(0); // flags for future implementations

            out.writeVInt(this.type);
            switch (type) {
                case TYPE_METADATA:
                    out.writeVInt(metadata.size());
                    for (BlockRangeIndexMetadata.BlockMetadata<Bytes> md : metadata) {
                        /* First key is null if is the head block */
                        out.writeArray(md.firstKey == null ? null : md.firstKey.data);
                        out.writeVInt(md.blockId);
                        out.writeVLong(md.size);
                        out.writeVLong(md.pageId);
                    }
                    break;
                case TYPE_BLOCKDATA:
                    out.writeVInt(pageData.size());
                    for (Map.Entry<Bytes, Bytes> entry : pageData) {
                        out.writeArray(entry.getKey().data);
                        out.writeArray(entry.getValue().data);
                    }
                    break;
                default:
                    throw new IllegalStateException("bad index page type " + type);
            }
        }

        static PageContents deserialize(ExtendedDataInputStream in) throws IOException {
            long version = in.readVLong(); // version
            long flags = in.readVLong(); // flags for future implementations

            /* Only version 2 actually supported, older versions will need a full rebuild */
            if (version < 2) {
                throw new UnsupportedMetadataVersionException(version);
            }

            if (version > 2 || flags != 0) {
                throw new DataStorageManagerException("corrupted index page");
            }

            PageContents result = new PageContents();
            result.type = in.readVInt();
            switch (result.type) {
                case TYPE_METADATA:
                    int blocks = in.readVInt();
                    result.metadata = new ArrayList<>();
                    for (int i = 0; i < blocks; i++) {
                        /* First key is null if is the head block */
                        byte[] fk = in.readArray();
                        Bytes firstKey = (fk == null) ? null : Bytes.from_array(fk);
                        int blockId = in.readVInt();
                        long size = in.readVLong();
                        long pageId = in.readVLong();
                        BlockRangeIndexMetadata.BlockMetadata<Bytes> md
                            = new BlockRangeIndexMetadata.BlockMetadata<>(firstKey, blockId, size, pageId);
                        result.metadata.add(md);
                    }
                    break;
                case TYPE_BLOCKDATA:
                    int values = in.readVInt();
                    result.pageData = new ArrayList<>(values);
                    for (int i = 0; i < values; i++) {
                        Bytes key = Bytes.from_array(in.readArray());
                        Bytes value = Bytes.from_array(in.readArray());
                        result.pageData.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
                    }
                    break;
                default:
                    throw new IOException("bad index page type " + result.type);
            }
            return result;
        }

        static PageContents deserialize(byte[] pagedata) throws IOException {
            try (SimpleByteArrayInputStream in = new SimpleByteArrayInputStream(pagedata);
                ExtendedDataInputStream ein = new ExtendedDataInputStream(in)) {
                return deserialize(ein);
            }
        }
    }

    /**
     * Signal that an old unsupported version of page metadata has been read. In
     * such cases the index will need a full rebuild.
     */
    private static final class UnsupportedMetadataVersionException extends HerdDBInternalException {

        /** Default Serial Version UID */
        private static final long serialVersionUID = 1L;

        private final long version;

        public UnsupportedMetadataVersionException(long version) {
            super();
            this.version = version;
        }
    }

    @Override
    protected boolean doStart(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, " start index {0} uuid {1}", new Object[]{index.name, index.uuid});
        bootSequenceNumber = sequenceNumber;

        if (LogSequenceNumber.START_OF_TIME.equals(sequenceNumber)) {
            /* Empty index (booting from the start) */
            this.data.boot(new BlockRangeIndexMetadata<>(Collections.emptyList()));
            LOGGER.log(Level.SEVERE, "loaded empty index {0}", new Object[]{index.name});

            return true;
        } else {

            IndexStatus status;
            try {
                status = dataStorageManager.getIndexStatus(tableSpaceUUID, index.uuid, sequenceNumber);
            } catch (DataStorageManagerException e) {
                LOGGER.log(Level.SEVERE, "cannot load index {0} due to {1}, it will be rebuilt", new Object[] {index.name, e});
                return false;
            }

            try {
                PageContents metadataBlock = PageContents.deserialize(status.indexData);
                this.data.boot(new BlockRangeIndexMetadata<>(metadataBlock.metadata));
            } catch (IOException e) {
                throw new DataStorageManagerException(e);
            } catch (UnsupportedMetadataVersionException e) {
                LOGGER.log(Level.SEVERE, "cannot load index {0} due to an old metadata version ({1}) found, it will be rebuilt", new Object[] {index.name, e.version});
                return false;
            }

            newPageId.set(status.newPageId);
            LOGGER.log(Level.SEVERE, "loaded index {0} {1} blocks", new Object[]{index.name, this.data.getNumBlocks()});
            return true;
        }

    }

    @Override
    public void rebuild() throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "rebuilding index {0}", index.name);
        data.clear();
        Table table = tableManager.getTable();
        tableManager.scanForIndexRebuild(r -> {
            DataAccessor values = r.getDataAccessor(table);
            Bytes key = RecordSerializer.serializePrimaryKey(values, table, table.primaryKey);
            Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index, index.columnNames);
//            LOGGER.log(Level.SEVERE, "adding " + key + " -> " + values);
            recordInserted(key, indexKey);
        });
        long _stop = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "rebuilding index {0} took {1}", new Object[]{index.name, (_stop - _start) + " ms"});
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
        try {
            BlockRangeIndexMetadata<Bytes> metadata = data.checkpoint();
            PageContents page = new PageContents();
            page.type = PageContents.TYPE_METADATA;
            page.metadata = metadata.getBlocksMetadata();
            byte[] contents = page.serialize();
            Set<Long> activePages = new HashSet<>();
            page.metadata.forEach(b -> {
                activePages.add(b.pageId);
            });
            IndexStatus indexStatus = new IndexStatus(index.name, sequenceNumber, newPageId.get(), activePages, contents);
            List<PostCheckpointAction> result = new ArrayList<>();
            result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, indexStatus, pin));

            LOGGER.log(Level.INFO, "checkpoint index {0} finished: logpos {1}, {2} blocks",
                    new Object[] {index.name, sequenceNumber, Integer.toString(page.metadata.size())});
            LOGGER.log(Level.FINE, "checkpoint index {0} finished: logpos {1}, pages {2}",
                    new Object[] {index.name, sequenceNumber, activePages});

            return result;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        dataStorageManager.unPinIndexCheckpoint(tableSpaceUUID, index.uuid, sequenceNumber);
    }

    @Override
    protected Stream<Bytes> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext) throws StatementExecutionException {
        if (operation instanceof SecondaryIndexSeek) {
            SecondaryIndexSeek sis = (SecondaryIndexSeek) operation;
            SQLRecordKeyFunction value = sis.value;
            byte[] refvalue = value.computeNewValue(null, context, tableContext);
            List<Bytes> result = data.search(Bytes.from_array(refvalue));
            if (result != null) {
                return result.stream();
            } else {
                return Stream.empty();
            }
        } else if (operation instanceof SecondaryIndexPrefixScan) {
            SecondaryIndexPrefixScan sis = (SecondaryIndexPrefixScan) operation;
            SQLRecordKeyFunction value = sis.value;
            byte[] refvalue = value.computeNewValue(null, context, tableContext);
            Bytes firstKey = Bytes.from_array(refvalue);
            Bytes lastKey = firstKey.next();
            return data.query(firstKey, lastKey);

        } else if (operation instanceof SecondaryIndexRangeScan) {

            Bytes firstKey = null;
            Bytes lastKey = null;

            SecondaryIndexRangeScan sis = (SecondaryIndexRangeScan) operation;
            SQLRecordKeyFunction minKey = sis.minValue;
            if (minKey != null) {
                byte[] refminvalue = minKey.computeNewValue(null, context, tableContext);
                firstKey = Bytes.from_array(refminvalue);
            }

            SQLRecordKeyFunction maxKey = sis.maxValue;
            if (maxKey != null) {
                byte[] refmaxvalue = maxKey.computeNewValue(null, context, tableContext);
                lastKey = Bytes.from_array(refmaxvalue);
            }
            LOGGER.log(Level.FINE, "range scan on {0}.{1}, from {2} to {1}", new Object[]{index.table, index.name, firstKey, lastKey});
            return data.query(firstKey, lastKey);

        } else {
            throw new UnsupportedOperationException("unsuppported index access type " + operation);
        }
    }

    @Override
    public void recordDeleted(Bytes key, Bytes indexKey) {
        data.delete(indexKey, key);
    }

    @Override
    public void recordInserted(Bytes key, Bytes indexKey) {
        data.put(indexKey, key);
    }

    @Override
    public void recordUpdated(Bytes key, Bytes indexKeyRemoved, Bytes indexKeyAdded) {
        if (Objects.equals(indexKeyRemoved, indexKeyAdded)) {
            return;
        }
        // BEWARE that this operation is not atomic
        if (indexKeyAdded != null) {
            data.put(indexKeyAdded, key);
        }
        if (indexKeyRemoved != null) {
            data.delete(indexKeyRemoved, key);
        }
    }

    private class IndexDataStorageImpl implements IndexDataStorage<Bytes, Bytes> {

        @Override
        public List<Map.Entry<Bytes, Bytes>> loadDataPage(long pageId) throws IOException {
            try {

                PageContents contents = dataStorageManager.readIndexPage(tableSpaceUUID, index.uuid, pageId, in -> {
                    return PageContents.deserialize(in);
                });

                if (contents.type != PageContents.TYPE_BLOCKDATA) {
                    throw new IOException("page " + pageId + " does not contain blocks data");
                }
                return contents.pageData;
            } catch (DataStorageManagerException err) {
                throw new IOException(err);
            }
        }

        @Override
        public long createDataPage(List<Map.Entry<Bytes, Bytes>> values) throws IOException {
            try {
                PageContents contents = new PageContents();
                contents.type = PageContents.TYPE_BLOCKDATA;
                contents.pageData = values;

                long pageId = newPageId.getAndIncrement();
                dataStorageManager.writeIndexPage(tableSpaceUUID, index.uuid, pageId, (out) -> contents.serialize(out));

                return pageId;
            } catch (DataStorageManagerException err) {
                throw new IOException(err);
            }
        }
    }

    @Override
    public void truncate() throws DataStorageManagerException {
        this.data.clear();
        dropIndexData();
    }

    @Override
    public void close() {
        data.clear();
    }

    public int getNumBlocks() {
        return data.getNumBlocks();
    }

}
