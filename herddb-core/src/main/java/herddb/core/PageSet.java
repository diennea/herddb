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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

import herddb.model.Record;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;

/**
 * Status of the set of pages of a Table
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public final class PageSet {

    private static final Logger LOGGER = Logger.getLogger(PageSet.class.getName());

    /** Dirty pages map (dirty/dirtysize)*/
    private final Map<Long,LongAdder> dirtyPages = new ConcurrentHashMap<>();

    /** Active stored pages (active/size+avg)*/
    private final Map<Long,DataPageMetaData> activePages = new ConcurrentHashMap<>();

    public static final class DataPageMetaData {

        final long size;
        final long avgRecordSize;

        public DataPageMetaData(long size, long avgRecordSize) {
            super();
            this.size = size;
            this.avgRecordSize = avgRecordSize;
        }

        public DataPageMetaData(DataPage page) {
            super();
            this.size = page.getUsedMemory();
            this.avgRecordSize = size / page.data.size();
        }

        public void serialize(ExtendedDataOutputStream output) throws IOException {
            output.writeVLong(size);
            output.writeVLong(avgRecordSize);
        }

        public static final DataPageMetaData deserialize(ExtendedDataInputStream input) throws IOException {
            return new DataPageMetaData(input.readVLong(), input.readVLong());
        }

        @Override
        public String toString() {
            return '[' + Long.toString(size) + ',' + Long.toString(avgRecordSize) + ']';
        }

    }
    void setActivePagesAtBoot(Map<Long,DataPageMetaData> activePagesAtBoot) {
        this.activePages.clear();
        this.activePages.putAll(activePagesAtBoot);
    }

    void setDirtyPagesAtBoot(Map<Long,Long> dirtyPagesAtBoot) {
        this.dirtyPages.clear();
        dirtyPagesAtBoot.forEach((k,v) -> {
            LongAdder dirtiness = new LongAdder();
            dirtiness.add(v);
            dirtyPages.put(k, dirtiness);
        });
    }


    Map<Long,DataPageMetaData> getActivePages() {
        return new HashMap<>(activePages);
    }

    int getActivePagesCount() {
        return activePages.size();
    }

    void truncate() {
        dirtyPages.clear();
        activePages.clear();
    }

    void setPageDirty(Long pageId) {
        final DataPageMetaData metadata = activePages.get(pageId);
        setPageDirty(pageId, metadata.avgRecordSize);
    }

    void setPageDirty(Long pageId, long size) {
        final LongAdder dirtiness = dirtyPages.computeIfAbsent(pageId, k -> {
            LOGGER.log(Level.FINEST, "now page " + pageId + " is dirty");
            return new LongAdder();
        });
        dirtiness.add(size);
    }

    void setPageDirty(Long pageId, Record dirtyRecord) {
        if(dirtyRecord == null) {
            setPageDirty(pageId);
        } else {
            setPageDirty(pageId,dirtyRecord.getEstimatedSize());
        }
    }

    @Override
    public String toString() {
        return "PageSet{" + "dirtyPages=" + dirtyPages + ", activePages=" + activePages + "}";
    }

    int getDirtyPagesCount() {
        return dirtyPages.size();
    }

    Map<Long,Long> getDirtyPages() {
        Map<Long,Long> map = new HashMap<>();
        dirtyPages.forEach((k,v) -> map.put(k,v.sum()));
        return map;
    }

    void pageCreated(Long pageId, DataPage page) {
        activePages.put(pageId, new DataPageMetaData(page));
    }

    void checkpointDone(Set<Long> dirtyPagesFlushed) {
        activePages.keySet().removeAll(dirtyPagesFlushed);
        dirtyPages.keySet().removeAll(dirtyPagesFlushed);
        LOGGER.log(Level.SEVERE, "checkpointDone " + dirtyPagesFlushed.size() + ", now activePages:"
                + activePages.size() + " dirty " + dirtyPages.size());
    }

}
