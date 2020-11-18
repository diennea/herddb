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
import herddb.sql.SQLRecordKeyFunction;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.Holder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HASH index. The index resides entirely in memory. It is serialized fully on
 * the IndexStatus structure
 *
 * @author enrico.olivelli
 */
public class MemoryHashIndexManager extends AbstractIndexManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryHashIndexManager.class.getName());

    private final ConcurrentHashMap<Bytes, List<Bytes>> data = new ConcurrentHashMap<>();
    private final AtomicLong newPageId = new AtomicLong(1);

    public MemoryHashIndexManager(Index index, AbstractTableManager tableManager, CommitLog log, DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID,
                                  long transaction,
                                  int writeLockTimeout, int readLockTimeout) {
        super(index, tableManager, dataStorageManager, tableSpaceManager.getTableSpaceUUID(), log, transaction,
                writeLockTimeout, readLockTimeout);
    }

    LogSequenceNumber bootSequenceNumber;

    @Override
    protected boolean doStart(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        LOGGER.info("loading in memory all the keys for mem index {}", new Object[]{index.name});
        bootSequenceNumber = sequenceNumber;

        dataStorageManager.initIndex(tableSpaceUUID, index.uuid);

        if (LogSequenceNumber.START_OF_TIME.equals(sequenceNumber)) {
            /* Empty index (booting from the start) */
            LOGGER.info("loaded empty index {}", new Object[]{index.name});
            return true;
        } else {

            IndexStatus status;
            try {
                status = dataStorageManager.getIndexStatus(tableSpaceUUID, index.uuid, sequenceNumber);
            } catch (DataStorageManagerException e) {
                LOGGER.error("cannot load index {} due to {}, it will be rebuilt", new Object[]{index.name, e});
                return false;
            }

            for (long pageId : status.activePages) {
                LOGGER.info("recovery index {}, load {}", new Object[]{index.name, pageId});

                Map<Bytes, List<Bytes>> read = dataStorageManager.readIndexPage(tableSpaceUUID, index.uuid, pageId, in -> {
                    Map<Bytes, List<Bytes>> deserialized = new HashMap<>();

                    long version = in.readVLong(); // version
                    long flags = in.readVLong(); // flags for future implementations
                    if (version != 1 || flags != 0) {
                        throw new DataStorageManagerException("corrupted index page");
                    }
                    int size = in.readVInt();
                    for (int i = 0; i < size; i++) {
                        Bytes indexKey = in.readBytesNoCopy();
                        int entrySize = in.readVInt();
                        List<Bytes> value = new ArrayList<>(entrySize);
                        for (int kk = 0; kk < entrySize; kk++) {
                            Bytes tableKey = in.readBytesNoCopy();
                            value.add(tableKey);
                        }
                        deserialized.put(indexKey, value);
                    }

                    return deserialized;
                });

                data.putAll(read);
            }

            newPageId.set(status.newPageId);
            LOGGER.info("loaded {} keys for index {}", new Object[]{data.size(), index.name});
            return true;
        }
    }

    @Override
    public void rebuild() throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        LOGGER.info("building index {}", index.name);
        dataStorageManager.initIndex(tableSpaceUUID, index.uuid);
        data.clear();
        Table table = tableManager.getTable();
        tableManager.scanForIndexRebuild(r -> {
            DataAccessor values = r.getDataAccessor(table);
            Bytes key = RecordSerializer.serializeIndexKey(values, table, table.primaryKey);
            Bytes indexKey = RecordSerializer.serializeIndexKey(values, index, index.columnNames);
//            LOGGER.error("adding " + key + " -> " + values);
            recordInserted(key, indexKey);
        });
        long _stop = System.currentTimeMillis();
        LOGGER.info("building index {} took {}", new Object[]{index.name, (_stop - _start) + " ms"});
    }

    @Override
    public Stream<Bytes> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext) throws StatementExecutionException {
        if (operation instanceof SecondaryIndexSeek) {
            SecondaryIndexSeek sis = (SecondaryIndexSeek) operation;
            SQLRecordKeyFunction value = sis.value;
            byte[] refvalue = value.computeNewValue(null, context, tableContext);
            List<Bytes> result = data.get(Bytes.from_array(refvalue));
            if (result != null) {
                return result.stream();
            } else {
                return Stream.empty();
            }
        } else if (operation instanceof SecondaryIndexPrefixScan) {
            SecondaryIndexPrefixScan sis = (SecondaryIndexPrefixScan) operation;
            SQLRecordKeyFunction value = sis.value;
            byte[] refvalue = value.computeNewValue(null, context, tableContext);
            Predicate<Map.Entry<Bytes, List<Bytes>>> predicate = (Map.Entry<Bytes, List<Bytes>> entry) -> {
                Bytes recordValue = entry.getKey();
                return recordValue.startsWith(refvalue.length, refvalue);
            };
            return data
                    .entrySet()
                    .stream()
                    .filter(predicate)
                    .map(entry -> entry.getValue())
                    .flatMap(l -> l.stream());

        } else if (operation instanceof SecondaryIndexRangeScan) {
            Bytes refminvalue;

            SecondaryIndexRangeScan sis = (SecondaryIndexRangeScan) operation;
            SQLRecordKeyFunction minKey = sis.minValue;
            if (minKey != null) {
                refminvalue = Bytes.from_nullable_array(minKey.computeNewValue(null, context, tableContext));
            } else {
                refminvalue = null;
            }

            Bytes refmaxvalue;
            SQLRecordKeyFunction maxKey = sis.maxValue;
            if (maxKey != null) {
                refmaxvalue = Bytes.from_nullable_array(maxKey.computeNewValue(null, context, tableContext));
            } else {
                refmaxvalue = null;
            }
            Predicate<Map.Entry<Bytes, List<Bytes>>> predicate;
            if (refminvalue != null && refmaxvalue == null) {
                predicate = (Map.Entry<Bytes, List<Bytes>> entry) -> {
                    Bytes datum = entry.getKey();
                    return datum.compareTo(refminvalue) >= 0;
                };
            } else if (refminvalue == null && refmaxvalue != null) {
                predicate = (Map.Entry<Bytes, List<Bytes>> entry) -> {
                    Bytes datum = entry.getKey();
                    return datum.compareTo(refmaxvalue) <= 0;
                };
            } else if (refminvalue != null && refmaxvalue != null) {
                predicate = (Map.Entry<Bytes, List<Bytes>> entry) -> {
                    Bytes datum = entry.getKey();
                    return datum.compareTo(refmaxvalue) <= 0
                            && datum.compareTo(refminvalue) >= 0;
                };
            } else {
                predicate = (Map.Entry<Bytes, List<Bytes>> entry) -> {
                    return true;
                };
            }
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
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
        if (createdInTransaction > 0) {
            LOGGER.info("checkpoint for index " + index.name + " skipped, this index is created on transaction " + createdInTransaction + " which is not committed");
            return Collections.emptyList();
        }
        List<PostCheckpointAction> result = new ArrayList<>();

        LOGGER.info("flush index {}", new Object[]{index.name});

        long pageId = newPageId.getAndIncrement();
        Holder<Long> count = new Holder<>();

        dataStorageManager.writeIndexPage(tableSpaceUUID, index.uuid, pageId, (out) -> {

            long entries = 0;
            out.writeVLong(1); // version
            out.writeVLong(0); // flags for future implementations
            out.writeVInt(data.size());
            for (Map.Entry<Bytes, List<Bytes>> entry : data.entrySet()) {
                out.writeArray(entry.getKey());
                List<Bytes> entrydata = entry.getValue();
                out.writeVInt(entrydata.size());
                for (Bytes v : entrydata) {
                    out.writeArray(v);
                    ++entries;
                }
            }

            count.value = entries;

        });

        IndexStatus indexStatus = new IndexStatus(index.name, sequenceNumber, newPageId.get(), Collections.singleton(pageId), null);
        result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, indexStatus, pin));

        LOGGER.info("checkpoint index {} finished: logpos {}, {} entries, page {}",
                new Object[]{index.name, sequenceNumber, Long.toString(count.value), Long.toString(pageId)});

        return result;
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        dataStorageManager.unPinIndexCheckpoint(tableSpaceUUID, index.uuid, sequenceNumber);
    }

    @Override
    public void recordDeleted(Bytes key, Bytes indexKey) {
        if (indexKey == null) {
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
    public void recordInserted(Bytes key, Bytes indexKey) {
        if (indexKey == null) {
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
    public void recordUpdated(Bytes key, Bytes indexKeyRemoved, Bytes indexKeyAdded) {
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

    @Override
    public void truncate() throws DataStorageManagerException {
        data.clear();
    }

    @Override
    public boolean valueAlreadyMapped(Bytes key, Bytes primaryKey) throws DataStorageManagerException {
        if (primaryKey == null) {
            // new record, error if there is any mapping
            return data.containsKey(key);
        } else {
            // updating a record, error if there is a mapping to another record
            List<Bytes> current = data.getOrDefault(key, Collections.emptyList());
            return !current.isEmpty()
                    && !current.contains(primaryKey);
        }
    }

}
