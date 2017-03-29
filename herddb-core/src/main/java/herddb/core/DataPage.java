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

/**
 * A page of data loaded in memory
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class DataPage extends Page<TableManager> {

    public final long maxSize;
    public final boolean readonly;

    public final Map<Bytes, Record> data;

    public final AtomicLong usedMemory;

    /** Access lock, exists only for mutable pages ({@code readonly == false}) */
    public final ReadWriteLock pageLock;

    /** Unloaded flag, to be accessed only under {@link #pageLock} */
    public boolean unloaded = false;

    public DataPage(TableManager owner, long pageId, long maxSize, long estimatedSize, Map<Bytes, Record> data, boolean readonly) {
        super(owner,pageId);
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
        return data.remove(key);
    }

    Record get(Bytes key) {
        return data.get(key);
    }

    boolean put(Record record) {
        if (readonly) {
            throw new IllegalStateException("page " + pageId + " is readonly!");
        }

        final Record prev = data.put(record.key, record);

        final long newSize = record.getEstimatedSize();
        if (newSize > maxSize) {
            throw new IllegalStateException(
                    "record too big to fit in any page " + newSize + " / " + maxSize + " bytes");
        }

        final long diff = prev == null ?
                newSize : newSize - prev.getEstimatedSize();

        final long target = maxSize - diff;


        final long old = usedMemory.getAndAccumulate(diff, (curr,change) -> curr > target ? curr : curr + diff);

        if( old > target ) {
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

}
