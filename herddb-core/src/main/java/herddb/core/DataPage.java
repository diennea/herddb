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

import herddb.model.Record;
import herddb.utils.Bytes;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A page of data loaded in memory
 *
 * @author enrico.olivelli
 */
public class DataPage {

    private static final Logger LOGGER = Logger.getLogger(DataPage.class.getName());

    /**
     * Page metadata used by {@link PageReplacementPolicy}
     */
    public static interface DataPageMetaData {

        public TableManager getOwner();

        public long getPageId();
    }

    public final TableManager owner;
    public final long pageId;
    public final boolean readonly;

    public final Map<Bytes, Record> data;

    public final AtomicLong usedMemory;

    /**
     * Page metadata used by {@link PageReplacementPolicy}
     */
    public DataPageMetaData metadata;

    public DataPage(TableManager owner, long pageId, long estimatedSize, Map<Bytes, Record> data, boolean readonly) {
        this.owner = owner;
        this.pageId = pageId;
        this.readonly = readonly;
        this.data = data;
        this.usedMemory = new AtomicLong(estimatedSize);
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

    boolean put(Bytes key, Record newRecord, long maxSize) {
        if (readonly) {
            throw new IllegalStateException("page " + pageId + " is readonly!");
        }

        Record prev = data.put(key, newRecord);
        if (prev != null) {
            return usedMemory.addAndGet(newRecord.value.getEstimatedSize() - prev.value.getEstimatedSize()) >= maxSize;
        } else {
            return usedMemory.addAndGet(newRecord.value.getEstimatedSize()) >= maxSize;
        }
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
