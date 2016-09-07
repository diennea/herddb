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
import herddb.core.PostCheckpointAction;
import herddb.core.TableSpaceManager;
import herddb.index.IndexOperation;
import herddb.index.SecondaryIndexPrefixScan;
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
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.xml.ws.Holder;

/**
 * Block-range like index
 *
 * @author enrico.olivelli
 */
public class BRINIndexManager extends AbstractIndexManager {

    private static final Logger LOGGER = Logger.getLogger(BRINIndexManager.class.getName());
    LogSequenceNumber bootSequenceNumber;
    private final AtomicLong newPageId = new AtomicLong(1);
    private final BlockRangeIndex<Bytes, Bytes> data = new BlockRangeIndex<>(10000);

    public BRINIndexManager(Index index, AbstractTableManager tableManager, CommitLog log, DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID, long transaction) {
        super(index, tableManager, dataStorageManager, tableSpaceManager.getTableSpaceUUID(), log, transaction);
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
                            doo.writeVInt(md.size);
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
            try (ByteArrayInputStream in = new ByteArrayInputStream(pagedata);
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
                            int size = ein.readVInt();
                            BlockRangeIndexMetadata.BlockMetadata<Bytes> md
                                = new BlockRangeIndexMetadata.BlockMetadata<>(firstKey, lastKey, blockId, size);
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
        LOGGER.log(Level.SEVERE, "loading in memory all the keys for index {1}", new Object[]{index.name});
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
                LOGGER.log(Level.SEVERE, "recovery index " + index.name + ", acceptPage " + pageId + " pagedata: " + pagedata.length);
                try {
                    PageContents pg = PageContents.deserialize(pagedata);
                    switch (pg.type) {
                        case PageContents.TYPE_METADATA:
                            metadataBlock.value = pg;
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
        LOGGER.log(Level.SEVERE, "loaded index {1}", new Object[]{index.name});
    }

    @Override
    public void rebuild() throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "rebuilding index {0}", index.name);
        data.clear();
        Table table = tableManager.getTable();
        tableManager.scanForIndexRebuild(r -> {
            Map<String, Object> values = r.toBean(table);
            Bytes key = RecordSerializer.serializePrimaryKey(values, table);
            LOGGER.log(Level.SEVERE, "adding " + key + " -> " + values);
            recordInserted(key, values);
        });
        long _stop = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "rebuilding index {0} took {1]", new Object[]{index.name, (_stop - _start) + " ms"});
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        return Collections.emptyList();
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

        } else {
            throw new UnsupportedOperationException("unsuppported index access type " + operation);
        }
    }

    @Override
    public void recordDeleted(Bytes key, Map<String, Object> values) {
        Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index);
        if (indexKey == null) {
            // valore non indicizzabile, contiene dei null
            return;
        }
        data.delete(indexKey, key);
    }

    @Override
    public void recordInserted(Bytes key, Map<String, Object> values) {
        Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index);
        if (indexKey == null) {
            // valore non indicizzabile, contiene dei null
            return;
        }
        data.put(indexKey, key);
    }

    @Override
    public void recordUpdated(Bytes key, Map<String, Object> previousValues, Map<String, Object> newValues) {
        Bytes indexKeyRemoved = RecordSerializer.serializePrimaryKey(previousValues, index);
        Bytes indexKeyAdded = RecordSerializer.serializePrimaryKey(newValues, index);
        if (indexKeyRemoved == null && indexKeyAdded == null) {
            return;
        }
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

}
