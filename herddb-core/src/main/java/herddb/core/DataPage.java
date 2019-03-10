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
package herddb.core;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import herddb.model.Record;
import herddb.utils.Bytes;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A page of data loaded in memory
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public final class DataPage extends Page<TableManager> {

    /**
     * Constant entry size take in account map entry nodes:
     *
     * <pre>
     * COUNT       AVG       SUM   DESCRIPTION
     *     1        32        32   java.util.HashMap$Node
     * </pre>
     */
    public static final long CONSTANT_ENTRY_BYTE_SIZE = 32;

    public static final long estimateEntrySize(Bytes key, byte[] value) {
        return Record.estimateSize(key, value) + DataPage.CONSTANT_ENTRY_BYTE_SIZE;
    }

    public static final long estimateEntrySize(Record record) {
        return record.getEstimatedSize() + DataPage.CONSTANT_ENTRY_BYTE_SIZE;
    }

    public final long maxSize;
    public final boolean immutable;

    private final Map<Bytes, Record> data;
    // this is only for debug. NOT TO be COMMITTED
    public final CopyOnWriteArrayList<Bytes> debugSeenKeys = new CopyOnWriteArrayList<>();

    public final AtomicLong usedMemory;

    /**
     * Access lock, exists only for mutable pages ({@code immutable == false})
     */
    public final ReadWriteLock pageLock;

    /**
     * Writability flag of mutable pages, mutable pages can switch to a not
     * writable state when flushed, to be accessed only under {@link #pageLock}
     */
    public boolean writable;

    DataPage(TableManager owner, long pageId, long maxSize, long estimatedSize, Map<Bytes, Record> data, boolean immutable) {
        super(owner, pageId);
        this.maxSize = maxSize;
        this.immutable = immutable;
        this.writable = !immutable;
        this.debugSeenKeys.addAll(data.keySet());

        this.data = data;
        this.usedMemory = new AtomicLong(estimatedSize);

        pageLock = immutable ? null : new ReentrantReadWriteLock(false);
    }
    
    Record remove(Bytes key) {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }

        final Record prev = data.remove(key);
        if (prev != null) {
            final long size = estimateEntrySize(prev);
            usedMemory.addAndGet(-size);
        }

        return prev;
    }

    Record get(Bytes key) {
        return data.get(key);
    }

    boolean put(Record record) {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }

        this.debugSeenKeys.add(record.key);
        final Record prev = data.put(record.key, record);

        final long newSize = estimateEntrySize(record);
        if (newSize > maxSize) {
            throw new IllegalStateException(
                    "record too big to fit in any page " + newSize + " / " + maxSize + " bytes");
        }

        final long diff = prev == null
                ? newSize : newSize - estimateEntrySize(prev);

        final long target = maxSize - diff;

        final long old = usedMemory.getAndAccumulate(diff, (curr, change) -> curr > target ? curr : curr + diff);

        if (old > target) {
            /* Remove the added key */
            data.remove(record.key);

            return false;
        }

        return true;
    }

    void putNoMemoryHandle(Record record) {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }
        this.debugSeenKeys.add(record.key);
        data.put(record.key, record);
    }

    void removeNoMemoryHandle(Record record) {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }
        data.remove(record.key);
    }

    boolean isEmpty() {
        return data.isEmpty();
    }

    int size() {
        return data.size();
    }
       
    Collection<Record> getRecordsForFlush() {
        return data.values();
    }
    
    Set<Bytes> getKeysForDebug() {
        return data.keySet();
    }

    long getUsedMemory() {
        return usedMemory.get();
    }

    void clear() {
        if (immutable) {
            throw new IllegalStateException("page " + pageId + " is immutable!");
        }
        data.clear();
        usedMemory.set(0);
    }

    @Override
    public String toString() {
        return "DataPage{" + "pageId=" + pageId + ", immutable=" + immutable + ", writable=" + writable + ", usedMemory=" + usedMemory + ", debugSeenKeys " + this.debugSeenKeys + '}';
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        return prime + (int) (pageId ^ (pageId >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DataPage other = (DataPage) obj;
        return pageId == other.pageId;
    }

    void flushRecordsCache() {
        data.values().forEach(r -> r.clearCache());
    }

}
