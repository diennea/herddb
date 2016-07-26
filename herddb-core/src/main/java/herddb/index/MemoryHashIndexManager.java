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
package herddb.index;

import herddb.codec.RecordSerializer;
import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.PostCheckpointAction;
import herddb.core.TableSpaceManager;
import herddb.log.CommitLog;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.model.Transaction;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * HASH index. The index resides entirely in memory. It is serialized fully on the IndexStatus structure
 *
 * @author enrico.olivelli
 */
public class MemoryHashIndexManager extends AbstractIndexManager {

    private static final Logger LOGGER = Logger.getLogger(MemoryHashIndexManager.class.getName());

    private final ConcurrentHashMap<Bytes, List<Bytes>> data = new ConcurrentHashMap<>();

    public MemoryHashIndexManager(Index index, AbstractTableManager tableManager, CommitLog log, DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID, long transaction) {
        super(index, tableManager, dataStorageManager, tableSpaceManager.getTableSpaceUUID(), log);
    }

    LogSequenceNumber bootSequenceNumber;

    @Override
    public void start() throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "loading in memory all the keys for index {1}", new Object[]{index.name});
        bootSequenceNumber = log.getLastSequenceNumber();

        dataStorageManager.fullIndexScan(tableSpaceUUID, index.name,
                new FullIndexScanConsumer() {

            @Override
            public void acceptIndexStatus(IndexStatus indexStatus) {
                LOGGER.log(Level.SEVERE, "recovery index "+indexStatus.indexName+" at " + indexStatus.sequenceNumber);
                bootSequenceNumber = indexStatus.sequenceNumber;
                if (indexStatus.indexData != null) {
                    ByteArrayInputStream indexData = new ByteArrayInputStream(indexStatus.indexData);
                    try (ExtendedDataInputStream oo = new ExtendedDataInputStream(indexData)) {
                        int size = oo.readVIntNoEOFException();
                        for (int i = 0; i < size; i++) {
                            byte[] indexKey = oo.readArray();
                            int entrySize = oo.readVInt();
                            List<Bytes> value = new ArrayList<>(entrySize);
                            for (int kk = 0; kk < entrySize; kk++) {
                                byte[] tableKey = oo.readArray();
                                value.add(Bytes.from_array(tableKey));
                            }
                            data.put(Bytes.from_array(indexKey), value);
                        }
                    } catch (IOException error) {
                        throw new RuntimeException(error);
                    }
                }
            }

        });

        LOGGER.log(Level.SEVERE, "loaded {0} keys for index {1}", new Object[]{data.size(), index.name});
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
            recordInserted(key, values, null);
        });
        long _stop = System.currentTimeMillis();
        LOGGER.log(Level.SEVERE, "rebuilding index {0} took {1]", new Object[]{index.name, (_stop - _start) + " ms"});
    }

    @Override
    public Stream<Bytes> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext) {
        if (operation instanceof SecondaryIndexSeek) {
            SecondaryIndexSeek sis = (SecondaryIndexSeek) operation;
            SQLRecordKeyFunction value = sis.value;
            Predicate<Map.Entry<Bytes, List<Bytes>>> predicate = (Map.Entry<Bytes, List<Bytes>> entry) -> {
                try {
                    byte[] refvalue = value.computeNewValue(null, context, tableContext);
                    return Arrays.equals(refvalue, entry.getKey().data);
                } catch (StatementExecutionException err) {
                    throw new RuntimeException(err);
                }
            };
            return data
                    .entrySet()
                    .stream()
                    .filter(predicate)
                    .map(entry -> entry.getValue())
                    .flatMap(l -> l.stream());

        } else {
            throw new UnsupportedOperationException("unsuppported index access type " + operation);
        }

    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        List<PostCheckpointAction> result = new ArrayList<>();

        LOGGER.log(Level.SEVERE, "flush index {0}", new Object[]{index.name});
        ByteArrayOutputStream indexData = new ByteArrayOutputStream();
        long count = 0;
        try (ExtendedDataOutputStream oo = new ExtendedDataOutputStream(indexData)) {
            oo.writeVInt(data.size());
            for (Map.Entry<Bytes, List<Bytes>> entry : data.entrySet()) {
                oo.writeArray(entry.getKey().data);
                List<Bytes> entrydata = entry.getValue();
                oo.writeVInt(entrydata.size());
                for (Bytes v : entrydata) {
                    oo.writeArray(v.data);
                    count++;
                }
            }
        } catch (IOException error) {
            throw new DataStorageManagerException(error);
        }

        IndexStatus indexStatus = new IndexStatus(index.name, sequenceNumber, indexData.toByteArray());
        result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.name, indexStatus));
        LOGGER.log(Level.SEVERE, "checkpoint index {0} finished, {1} entries", new Object[]{index.name, count + ""});
        return result;
    }

    @Override
    public void recordDeleted(Bytes key, Map<String, Object> values, Transaction transaction) {
        Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index);
        if (indexKey == null) {
            // valore non indicizzabile, contiene dei null
            return;
        }
        removeValueFromIndex(indexKey, key);
    }

    private void removeValueFromIndex(Bytes indexKey, Bytes key) {
        data.merge(indexKey, Collections.singletonList(key), (actual, newList) -> {
            if (actual.size() == 1) {
                return null;
            } else {
                actual.removeAll(newList);
                return actual;
            }
        });
    }

    @Override
    public void recordInserted(Bytes key, Map<String, Object> values, Transaction transaction) {
        Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index);
        if (indexKey == null) {
            // valore non indicizzabile, contiene dei null
            return;
        }
        addValueToIndex(indexKey, key);
    }

    private void addValueToIndex(Bytes indexKey, Bytes key) {
        data.merge(indexKey, Collections.singletonList(key), (actual, newList) -> {
            List<Bytes> result = new ArrayList<>(actual.size() + 1);
            result.addAll(actual);
            result.addAll(newList);
            return result;
        });
    }

    @Override
    public void recordUpdated(Bytes key, Map<String, Object> previousValues, Map<String, Object> newValues, Transaction transaction) {
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
            addValueToIndex(indexKeyAdded, key);
        }
        if (indexKeyRemoved != null) {
            removeValueFromIndex(indexKeyRemoved, key);
        }
    }

    @Override
    public void close() {
        data.clear();
    }

}
