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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

import herddb.core.PostCheckpointAction;
import herddb.log.LogSequenceNumber;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.sql.SQLRecordKeyFunction;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import herddb.utils.Holder;

/**
 * Implementation of KeyToPageIndex which uses any ConcurrentMap
 *
 * @author enrico.olivelli
 */
public class ConcurrentMapKeyToPageIndex implements KeyToPageIndex {

    private final ConcurrentMap<Bytes, Long> map;
    private final AtomicLong usedMemory = new AtomicLong();

    // assume that an entry holds 24 bytes (a Long pointer + long value + extra overhead)
    private final static long ENTRY_OVERHEAD = 8 + 8 + 8 + 8 + 8;

    public ConcurrentMapKeyToPageIndex(ConcurrentMap<Bytes, Long> map) {
        this.map = map;
        this.map.keySet().forEach(this::keyAdded);
    }

    public ConcurrentMap<Bytes, Long> getMap() {
        return map;
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public void put(Bytes key, Long currentPage) {
        Long res = map.put(key, currentPage);
        if (res == null) {
            keyAdded(key);
        }
    }

    @Override
    public boolean put(Bytes key, Long newPage, Long expectedPage) {
        if (expectedPage == null) {
            final Long opage = map.putIfAbsent(key, newPage);
            return opage == null;
        } else {
            /*
             * We need to keep track if the update was really done. Reading computeIfPresent result won't
             * suffice, it can be equal to newPage even if no replacement was done (the map contained already
             * newPage mapping and expectedPage was different)
             */
            Holder<Boolean> holder = new Holder<>(Boolean.FALSE);
            map.computeIfPresent(key, (skey, spage) -> {
                if (spage.equals(expectedPage)) {
                    holder.value = Boolean.TRUE;
                    return newPage;
                }
                return spage;
            });

            return holder.value.booleanValue();
        }

    }

    private void keyAdded(Bytes key) {
        usedMemory.addAndGet(key.getLength() + ENTRY_OVERHEAD);
    }

    private void keyRemoved(Bytes key) {
        usedMemory.addAndGet(-key.getLength() - ENTRY_OVERHEAD);
    }

    @Override
    public boolean containsKey(Bytes key) {
        return map.containsKey(key);
    }

    @Override
    public Long get(Bytes key) {
        return map.get(key);
    }

    @Override
    public Long remove(Bytes key) {
        Long res = map.remove(key);
        if (res != null) {
            keyRemoved(key);
        }
        return res;
    }

    @Override
    public boolean isSortedAscending() {
        return false;
    }

    @Override
    public Stream<Map.Entry<Bytes, Long>> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext, herddb.core.AbstractIndexManager index) throws DataStorageManagerException {

        if (operation instanceof PrimaryIndexSeek) {
            PrimaryIndexSeek seek = (PrimaryIndexSeek) operation;
            byte[] seekValue = seek.value.computeNewValue(null, context, tableContext);
            if (seekValue == null) {
                return Stream.empty();
            }
            Bytes key = Bytes.from_array(seekValue);
            Long pageId = map.get(key);
            if (pageId == null) {
                return Stream.empty();
            }
            return Stream.of(new AbstractMap.SimpleImmutableEntry<>(key, pageId));
        }

        // Remember that the IndexOperation can return more records
        // every predicate (WHEREs...) will always be evaluated anyway on every record, in order to guarantee correctness
        if (index != null) {
            return index.recordSetScanner(operation, context, tableContext, this);
        }
        if (operation == null) {
            Stream<Map.Entry<Bytes, Long>> baseStream = map.entrySet().stream();
            return baseStream;
        } else if (operation instanceof PrimaryIndexPrefixScan) {
            PrimaryIndexPrefixScan scan = (PrimaryIndexPrefixScan) operation;
            byte[] prefix;
            try {
                prefix = scan.value.computeNewValue(null, context, tableContext);
            } catch (StatementExecutionException err) {
                throw new RuntimeException(err);
            }
            Predicate<Map.Entry<Bytes, Long>> predicate = (Map.Entry<Bytes, Long> t) -> {
                Bytes fullrecordKey = t.getKey();
                return fullrecordKey.startsWith(prefix.length, prefix);
            };
            Stream<Map.Entry<Bytes, Long>> baseStream = map.entrySet().stream();
            return baseStream.filter(predicate);
        } else if (operation instanceof PrimaryIndexRangeScan) {

            Bytes refminvalue;
            PrimaryIndexRangeScan sis = (PrimaryIndexRangeScan) operation;
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
            Predicate<Map.Entry<Bytes, Long>> predicate;
            if (refminvalue != null && refmaxvalue == null) {
                predicate = (Map.Entry<Bytes, Long> entry) -> {
                    Bytes datum = entry.getKey();
                    return datum.compareTo(refminvalue) >= 0;
                };
            } else if (refminvalue == null && refmaxvalue != null) {
                predicate = (Map.Entry<Bytes, Long> entry) -> {
                    Bytes datum = entry.getKey();
                    return datum.compareTo(refmaxvalue) <= 0;
                };
            } else if (refminvalue != null && refmaxvalue != null) {
                predicate = (Map.Entry<Bytes, Long> entry) -> {
                    Bytes datum = entry.getKey();
                    return datum.compareTo(refmaxvalue) <= 0
                            && datum.compareTo(refminvalue) >= 0;
                };
            } else {
                predicate = (Map.Entry<Bytes, Long> entry) -> {
                    return true;
                };
            }
            Stream<Map.Entry<Bytes, Long>> baseStream = map.entrySet().stream();
            return baseStream.filter(predicate);
        } else {
            throw new DataStorageManagerException("operation " + operation + " not implemented on " + this.getClass());
        }
    }

    @Override
    public void close() {
        map.clear();
        usedMemory.set(0);
    }

    @Override
    public void truncate() {
        map.clear();
        usedMemory.set(0);
    }

    @Override
    public long getUsedMemory() {
        return usedMemory.get();
    }

    @Override
    public boolean requireLoadAtStartup() {
        /* Require a full table scan at startup */
        return true;
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
        /* No checkpoint, isn't persisted */
        return Collections.emptyList();
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        /* No checkpoint, isn't persisted */
    }

    @Override
    public void start(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        /* No work needed, this implementation require a full table scan at startup instead */
    }

}
