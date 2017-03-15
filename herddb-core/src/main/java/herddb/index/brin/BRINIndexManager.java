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
import java.io.InputStream;
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

import javax.xml.ws.Holder;

import herddb.codec.RecordSerializer;
import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
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
import herddb.storage.FullIndexScanConsumer;
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
    public static final int MAX_BLOCK_SIZE = 10000;
    public static final int MAX_LOADED_BLOCKS = 1000;
    LogSequenceNumber bootSequenceNumber;
    private final AtomicLong newPageId = new AtomicLong(1);
    private final BlockRangeIndex<Bytes, Bytes> data;
    private final IndexDataStorage<Bytes, Bytes> storageLayer = new IndexDataStorageImpl();

    public BRINIndexManager(Index index, MemoryManager memoryManager, AbstractTableManager tableManager, CommitLog log, DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID, long transaction) {
        super(index, tableManager, dataStorageManager, tableSpaceManager.getTableSpaceUUID(), log, transaction);
        this.data = new BlockRangeIndex<>(MAX_BLOCK_SIZE, memoryManager.getDataPageReplacementPolicy(), storageLayer);
    }

    private static final class PageContents {

        public static final int TYPE_METADATA = 9;
        public static final int TYPE_BLOCKDATA = 10;

        private int type;
        private List<Map.Entry<Bytes, Bytes>> pageData;
        private List<BlockRangeIndexMetadata.BlockMetadata<Bytes>> metadata;

        byte[] serialize() throws IOException {
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(oo)) {
                doo.writeVInt(this.type);
                switch (type) {
                    case TYPE_METADATA:
                        doo.writeVInt(metadata.size());
                        for (BlockRangeIndexMetadata.BlockMetadata<Bytes> md : metadata) {
                            doo.writeArray(md.firstKey.data);
                            doo.writeArray(md.lastKey.data);
                            doo.writeVInt(md.blockId);
                            doo.writeVLong(md.size);
                            doo.writeVLong(md.pageId);
                        }
                        break;
                    case TYPE_BLOCKDATA:
                        doo.writeVInt(pageData.size());
                        for (Map.Entry<Bytes, Bytes> entry : pageData) {
                            doo.writeArray(entry.getKey().data);
                            doo.writeArray(entry.getValue().data);
                        }
                        break;
                    default:
                        throw new IllegalStateException("bad index page type " + type);
                }
            }
            return oo.toByteArray();
        }

        static PageContents deserialize(byte[] pagedata) throws IOException {
            try (InputStream in = new SimpleByteArrayInputStream(pagedata);
                ExtendedDataInputStream ein = new ExtendedDataInputStream(in)) {
                PageContents result = new PageContents();
                result.type = ein.readVInt();
                switch (result.type) {
                    case TYPE_METADATA:
                        int blocks = ein.readVInt();
                        result.metadata = new ArrayList<>();
                        for (int i = 0; i < blocks; i++) {
                            Bytes firstKey = Bytes.from_array(ein.readArray());
                            Bytes lastKey = Bytes.from_array(ein.readArray());
                            int blockId = ein.readVInt();
                            long size = ein.readVLong();
                            long pageId = ein.readVLong();
                            BlockRangeIndexMetadata.BlockMetadata<Bytes> md
                                = new BlockRangeIndexMetadata.BlockMetadata<>(firstKey, lastKey, blockId, size, pageId);
                            result.metadata.add(md);
                        }
                        break;
                    case TYPE_BLOCKDATA:
                        int values = ein.readVInt();
                        result.pageData = new ArrayList<>(values);
                        for (int i = 0; i < values; i++) {
                            Bytes key = Bytes.from_array(ein.readArray());
                            Bytes value = Bytes.from_array(ein.readArray());
                            result.pageData.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
                        }
                        break;
                    default:
                        throw new IOException("bad index page type " + result.type);
                }
                return result;
            }
        }
    }

    @Override
    public void start() throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, " start index {0}", new Object[]{index.name});
        bootSequenceNumber = log.getLastSequenceNumber();
        Holder<PageContents> metadataBlock = new Holder<>();
        dataStorageManager.fullIndexScan(tableSpaceUUID, index.name,
            new FullIndexScanConsumer() {

            @Override
            public void acceptIndexStatus(IndexStatus indexStatus) {
                LOGGER.log(Level.SEVERE, "recovery index " + indexStatus.indexName + " at " + indexStatus.sequenceNumber);
                bootSequenceNumber = indexStatus.sequenceNumber;
            }

            @Override
            public void acceptPage(long pageId, byte[] pagedata) {
                if (newPageId.get() <= pageId) {
                    newPageId.set(pageId + 1);
                }
                LOGGER.log(Level.FINEST, "recovery index " + index.name + ", acceptPage " + pageId + " pagedata: " + pagedata.length + " bytes");
                try {
                    PageContents pg = PageContents.deserialize(pagedata);
                    switch (pg.type) {
                        case PageContents.TYPE_METADATA:
                            metadataBlock.value = pg;
                            LOGGER.log(Level.INFO, "recovery index " + index.name + ", metatadata page " + pageId + " contains " + pg.metadata.size() + " blocks metadata");
                            break;
                        case PageContents.TYPE_BLOCKDATA:
                            LOGGER.log(Level.INFO, "recovery index " + index.name + ", data page " + pageId + " contains " + pg.pageData.size() + " entries");
                            break;
                        default:
                            LOGGER.log(Level.SEVERE, "recovery index {0}, ignore page type {1}", new Object[]{index.name, pg.type});
                            break;
                    }
                } catch (IOException err) {
                    throw new RuntimeException(err);
                }
            }

        });

        if (metadataBlock.value != null) {
            this.data.boot(new BlockRangeIndexMetadata<>(metadataBlock.value.metadata));
        } else {
            this.data.boot(new BlockRangeIndexMetadata<>(Collections.emptyList()));
        }
        LOGGER.log(Level.SEVERE, "loaded index {0} {1} blocks", new Object[]{index.name, this.data.getNumBlocks()});
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
//            LOGGER.log(Level.SEVERE, "adding " + key + " -> " + values);
            recordInserted(key, values);
        });
        long _stop = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "rebuilding index {0} took {1}", new Object[]{index.name, (_stop - _start) + " ms"});
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        try {
            BlockRangeIndexMetadata<Bytes> metadata = data.checkpoint();
            PageContents page = new PageContents();
            page.type = PageContents.TYPE_METADATA;
            page.metadata = metadata.getBlocksMetadata();
            long newPage = newPageId.incrementAndGet();
            dataStorageManager.writeIndexPage(tableSpaceUUID, index.name, newPage, page.serialize());
            Set<Long> activePages = new HashSet<>();
            activePages.add(newPage);
            page.metadata.forEach(b -> {
                activePages.add(b.pageId);
            });
            IndexStatus indexStatus = new IndexStatus(index.name, sequenceNumber, activePages, null);
            List<PostCheckpointAction> result = new ArrayList<>();
            result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.name, indexStatus));
            LOGGER.log(Level.SEVERE, "checkpoint index {0} finished, {1} blocks, pages {2}", new Object[]{index.name, page.metadata.size() + "", activePages + ""});
            return result;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
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
    public void recordDeleted(Bytes key, DataAccessor values) {
        Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index, index.columnNames);
        data.delete(indexKey, key);
    }

    @Override
    public void recordInserted(Bytes key, DataAccessor values) {
        Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index, index.columnNames);
        data.put(indexKey, key);
    }

    @Override
    public void recordUpdated(Bytes key, DataAccessor previousValues, DataAccessor newValues) {
        Bytes indexKeyRemoved = RecordSerializer.serializePrimaryKey(previousValues, index, index.columnNames);
        Bytes indexKeyAdded = RecordSerializer.serializePrimaryKey(newValues, index, index.columnNames);
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
                byte[] pageData = dataStorageManager.readIndexPage(tableSpaceUUID, index.name, pageId);
                PageContents contents = PageContents.deserialize(pageData);
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
                byte[] serialized = contents.serialize();
                long pageId = newPageId.incrementAndGet();
                dataStorageManager.writeIndexPage(tableSpaceUUID, index.name, pageId, serialized);
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

}
