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
import java.util.Collections;

/**
 * A page of data loaded in memory
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class DataPage extends Page<TableManager> {

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
    public boolean readonly;

    public Map<Bytes, Record> data;

    public final AtomicLong usedMemory;

    /**
     * Access lock, exists only for mutable pages ({@code readonly == false})
     */
    public final ReadWriteLock pageLock;

    /**
     * Unloaded flag, to be accessed only under {@link #pageLock}
     */
    public boolean unloaded = false;

    public DataPage(TableManager owner, long pageId, long maxSize, long estimatedSize, Map<Bytes, Record> data, boolean readonly) {
        super(owner, pageId);
        this.maxSize = maxSize;
        this.readonly = readonly;
        this.data = data;
        this.usedMemory = new AtomicLong(estimatedSize);

        pageLock = readonly ? null : new ReentrantReadWriteLock(false);
    }

    Record remove(Bytes key) {
        if (readonly) {
            throw new IllegalStateException("page " + pageId + " is readonly!");
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
        if (readonly) {
            throw new IllegalStateException("page " + pageId + " is readonly!");
        }

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

    boolean isEmpty() {
        return data.isEmpty();
    }

    int size() {
        return data.size();
    }

    long getUsedMemory() {
        return usedMemory.get();
    }

    void clear() {
        if (readonly) {
            throw new IllegalStateException("page " + pageId + " is readonly!");
        }
        data.clear();
        usedMemory.set(0);
    }

    @Override
    public String toString() {
        return "DataPage{" + "pageId=" + pageId + ", readonly=" + readonly + ", usedMemory=" + usedMemory + '}';
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

    void makeImmutable() {
        pageLock.writeLock().lock();
        try {
            if (!unloaded) {
                throw new IllegalStateException("cannot convert to immutable if loaded=false, pageid " + pageId);
            }
            if (readonly) {
                throw new IllegalStateException("cannot convert to immutable if readonly=true, pageid " + pageId);
            }
            this.readonly = true;
            this.unloaded = false;
            this.data = Collections.unmodifiableMap(data);
        } finally {
            pageLock.writeLock().unlock();
        }
    }

}
