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
import herddb.index.brin.BlockRangeIndexMetadata.BlockMetadata;
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
import herddb.utils.ByteArrayCursor;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SystemProperties;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Block-range like index with pagination managed by a {@link PageReplacementPolicy}
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class BRINIndexManager extends AbstractIndexManager {

    private static final boolean VALIDATE_CHECKPOINT_METADATA = SystemProperties.getBooleanSystemProperty(
            "herddb.index.brin.validatecheckpointmetadata", true);

    private static final Logger LOGGER = Logger.getLogger(BRINIndexManager.class.getName());

    LogSequenceNumber bootSequenceNumber;
    private final AtomicLong newPageId = new AtomicLong(1);
    private final BlockRangeIndex<Bytes, Bytes> data;
    private final IndexDataStorage<Bytes, Bytes> storageLayer = new IndexDataStorageImpl();

    public BRINIndexManager(Index index, MemoryManager memoryManager, AbstractTableManager tableManager, CommitLog log, DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID, long transaction) {
        super(index, tableManager, dataStorageManager, tableSpaceManager.getTableSpaceUUID(), log, transaction);
        this.data = new BlockRangeIndex<>(
                memoryManager.getMaxLogicalPageSize(),
                memoryManager.getDataPageReplacementPolicy(),
                storageLayer);
    }

    private static class PageContents {

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
            out.writeVLong(3); // version
            out.writeVLong(0); // flags for future implementations

            out.writeVInt(this.type);
            switch (type) {
                case TYPE_METADATA:
                    out.writeVInt(metadata.size());
                    for (BlockRangeIndexMetadata.BlockMetadata<Bytes> md : metadata) {
                        /* First key is null if is the head block */
                        boolean head = md.firstKey == null;
                        /* Next block is null if is the tail block */
                        boolean tail = md.nextBlockId == null;

                        /* Prepares hasd/tail flags */
                        byte blockFlags = 0;
                        if (head) {
                            blockFlags |= BlockMetadata.HEAD;
                        }
                        if (tail) {
                            blockFlags |= BlockMetadata.TAIL;
                        }
                        out.writeByte(blockFlags);

                        if (!head) {
                            out.writeArray(md.firstKey);
                        }
                        out.writeZLong(md.blockId);
                        out.writeVLong(md.size);
                        out.writeVLong(md.pageId);

                        if (!tail) {
                            out.writeZLong(md.nextBlockId);
                        }
                    }
                    break;
                case TYPE_BLOCKDATA:
                    out.writeVInt(pageData.size());
                    for (Map.Entry<Bytes, Bytes> entry : pageData) {
                        out.writeArray(entry.getKey());
                        out.writeArray(entry.getValue());
                    }
                    break;
                default:
                    throw new IllegalStateException("bad index page type " + type);
            }
        }

        static PageContents deserialize(ByteArrayCursor in) throws IOException {
            long version = in.readVLong(); // version
            long flags = in.readVLong(); // flags for future implementations

            /* Only version 3 actually supported, older versions will need a full rebuild */
            if (version < 3) {
                throw new UnsupportedMetadataVersionException(version);
            }

            if (version > 3 || flags != 0) {
                throw new DataStorageManagerException("corrupted index page");
            }

            PageContents result = new PageContents();
            result.type = in.readVInt();
            switch (result.type) {
                case TYPE_METADATA:
                    int blocks = in.readVInt();
                    result.metadata = new ArrayList<>();
                    for (int i = 0; i < blocks; i++) {
                        byte blockFlags = in.readByte();
                        Bytes firstKey;
                        if ((blockFlags & BlockMetadata.HEAD) > 0) {
                            /* Is HEAD */
                            /* First key is null if is the head block */
                            firstKey = null;
                        } else {
                            firstKey = in.readBytesNoCopy();
                        }

                        long blockId = in.readZLong();
                        long size = in.readVLong();
                        long pageId = in.readVLong();

                        Long nextBlockId;
                        if ((blockFlags & BlockMetadata.TAIL) > 0) {
                            /* Is TAIL */
                            /* Next block is null if is the tail block */
                            nextBlockId = null;
                        } else {
                            nextBlockId = in.readZLong();
                        }

                        BlockRangeIndexMetadata.BlockMetadata<Bytes> md = new BlockRangeIndexMetadata.BlockMetadata<>(firstKey, blockId, size, pageId, nextBlockId);
                        result.metadata.add(md);
                    }
                    break;
                case TYPE_BLOCKDATA:
                    int values = in.readVInt();
                    result.pageData = new ArrayList<>(values);
                    for (int i = 0; i < values; i++) {
                        Bytes key = in.readBytesNoCopy();
                        Bytes value = in.readBytesNoCopy();
                        result.pageData.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
                    }
                    break;
                default:
                    throw new IOException("bad index page type " + result.type);
            }
            return result;
        }

        static PageContents deserialize(byte[] pagedata) throws IOException {
            try (ByteArrayCursor ein = ByteArrayCursor.wrap(pagedata)) {
                return deserialize(ein);
            }
        }
    }

    /**
     * Signal that an old unsupported version of page metadata has been read. In
     * such cases the index will need a full rebuild.
     */
    private static class UnsupportedMetadataVersionException extends HerdDBInternalException {

        /**
         * Default Serial Version UID
         */
        private static final long serialVersionUID = 1L;

        private final long version;

        public UnsupportedMetadataVersionException(long version) {
            super();
            this.version = version;
        }
    }

    @Override
    protected boolean doStart(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        LOGGER.log(Level.FINE, " start BRIN index {0} uuid {1}", new Object[]{index.name, index.uuid});

        dataStorageManager.initIndex(tableSpaceUUID, index.uuid);

        bootSequenceNumber = sequenceNumber;

        if (LogSequenceNumber.START_OF_TIME.equals(sequenceNumber)) {
            /* Empty index (booting from the start) */
            this.data.boot(BlockRangeIndexMetadata.empty());
            LOGGER.log(Level.FINE, "loaded empty index {0}", new Object[]{index.name});

            return true;
        } else {

            IndexStatus status;
            try {
                status = dataStorageManager.getIndexStatus(tableSpaceUUID, index.uuid, sequenceNumber);
            } catch (DataStorageManagerException e) {
                LOGGER.log(Level.SEVERE, "cannot load index {0} due to {1}, it will be rebuilt", new Object[]{index.name, e});
                return false;
            }

            try {
                PageContents metadataBlock = PageContents.deserialize(status.indexData);
                this.data.boot(new BlockRangeIndexMetadata<>(metadataBlock.metadata));
            } catch (IOException e) {
                throw new DataStorageManagerException(e);
            } catch (UnsupportedMetadataVersionException e) {
                LOGGER.log(Level.SEVERE, "cannot load index {0} due to an old metadata version ({1}) found, it will be rebuilt", new Object[]{index.name, e.version});
                return false;
            }

            newPageId.set(status.newPageId);
            LOGGER.log(Level.INFO, "loaded index {0} {1} blocks", new Object[]{index.name, this.data.getNumBlocks()});
            return true;
        }

    }

    @Override
    public void rebuild() throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        LOGGER.log(Level.FINE, "building index {0}", index.name);
        dataStorageManager.initIndex(tableSpaceUUID, index.uuid);
        data.reset();
        Table table = tableManager.getTable();
        AtomicLong count = new AtomicLong();
        tableManager.scanForIndexRebuild(r -> {
            DataAccessor values = r.getDataAccessor(table);
            Bytes key = RecordSerializer.serializePrimaryKey(values, table, table.primaryKey);
            Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index, index.columnNames);
//            LOGGER.log(Level.SEVERE, "adding " + key + " -> " + values);
            recordInserted(key, indexKey);
            count.incrementAndGet();
        });
        long _stop = System.currentTimeMillis();
        if (count.intValue() > 0) {
            LOGGER.log(Level.INFO, "building index {0} took {1}, scanned {2} records", new Object[]{index.name, (_stop - _start) + " ms", count});
        }
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
        try {
            BlockRangeIndexMetadata<Bytes> metadata = data.checkpoint();

            /* Checks metadata consistency with actual data structure */
            if (VALIDATE_CHECKPOINT_METADATA) {
                boolean invalid = false;
                /*
                 * Metadata are saved/recovered in reverse order so "next" block has been already created
                 */
                Long nextID = null;
                for (BlockRangeIndexMetadata.BlockMetadata<Bytes> blockData : metadata.getBlocksMetadata()) {

                    /* Medatada safety check (do not trust blindly ordering) */
                    if (blockData.nextBlockId != null) {
                        if (nextID == null) {
                            LOGGER.log(Level.WARNING, "Wrong next block on index {0}, expected notingh but {0} found",
                                    new Object[]{index.name, blockData.nextBlockId});
                            invalid = true;
                        } else if (nextID != blockData.nextBlockId.longValue()) {
                            LOGGER.log(Level.WARNING, "Wrong next block on index {0}, expected {1} but {2} found",
                                    new Object[]{index.name, nextID, blockData.nextBlockId});
                            invalid = true;
                        }
                    } else {
                        if (nextID != null) {
                            LOGGER.log(Level.WARNING, "Wrong next block on index {0}, expected {1} but nothing found",
                                    new Object[]{index.name, nextID});
                            invalid = true;
                        }

                    }

                    nextID = blockData.blockId;
                }

                if (invalid) {
                    LOGGER.log(Level.WARNING, data.generateDetailedInternalStatus());
                }
            }


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
                    new Object[]{index.name, sequenceNumber, Integer.toString(page.metadata.size())});
            LOGGER.log(Level.FINE, "checkpoint index {0} finished: logpos {1}, pages {2}",
                    new Object[]{index.name, sequenceNumber, activePages});

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
            return data.query(Bytes.from_array(refvalue));

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
        if (indexKey == null) {
            return;
        }
        data.delete(indexKey, key);
    }

    @Override
    public void recordInserted(Bytes key, Bytes indexKey) {
        if (indexKey == null) {
            return;
        }
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
        this.data.reset();
        truncateIndexData();
    }

    @Override
    public void close() {
        data.clear();
    }

    public int getNumBlocks() {
        return data.getNumBlocks();
    }

}
