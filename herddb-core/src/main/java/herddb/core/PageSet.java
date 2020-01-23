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

import herddb.model.Record;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Status of the set of pages of a Table
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public final class PageSet {

    private static final Logger LOGGER = Logger.getLogger(PageSet.class.getName());

    /**
     * Active stored pages (active/size,average-record-size,dirt,hasDeletions)
     */
    private final ConcurrentMap<Long, DataPageMetaData> activePages = new ConcurrentHashMap<>();

    public static final class DataPageMetaData {

        final long size;
        final long avgRecordSize;
        final LongAdder dirt;

        public DataPageMetaData(DataPage page) {
            super();
            this.size = page.getUsedMemory();
            this.avgRecordSize = size / page.size();
            this.dirt = new LongAdder();
        }

        private DataPageMetaData(long size, long avgRecordSize, long dirt) {
            super();
            this.size = size;
            this.avgRecordSize = avgRecordSize;
            this.dirt = new LongAdder();
            this.dirt.add(dirt);
        }

        public void serialize(ExtendedDataOutputStream output) throws IOException {
            output.writeVLong(size);
            output.writeVLong(avgRecordSize);
            output.writeVLong(dirt.sum());
        }

        public static DataPageMetaData deserialize(ExtendedDataInputStream input) throws IOException {
            return new DataPageMetaData(input.readVLong(), input.readVLong(), input.readVLong());
        }

        @Override
        public String toString() {
            return '['
                    + Long.toString(size) + ','
                    + avgRecordSize + ','
                    + dirt.sum() + ']';
        }

    }

    void setActivePagesAtBoot(Map<Long, DataPageMetaData> activePagesAtBoot) {
        this.activePages.clear();
        this.activePages.putAll(activePagesAtBoot);
    }

    Map<Long, DataPageMetaData> getActivePages() {
        return new HashMap<>(activePages);
    }

    Set<Long> getActivePagesIDs() {
        return Collections.unmodifiableSet(activePages.keySet());
    }

    int getActivePagesCount() {
        return activePages.size();
    }

    void truncate() {
        activePages.clear();
    }

    void setPageDirty(Long pageId) {
        final DataPageMetaData metadata = activePages.get(pageId);
        if (metadata == null) {
            LOGGER.log(Level.SEVERE,
                    "Detected an attempt to set as dirty an unknown page " + pageId + ". Known pages: " + activePages);
            throw new IllegalStateException("attempted to set an unknown page as dirty " + pageId);
        }
        metadata.dirt.add(metadata.avgRecordSize);
    }

    void setPageDirty(Long pageId, long size) {
        final DataPageMetaData metadata = activePages.get(pageId);
        if (metadata == null) {
            LOGGER.log(Level.SEVERE,
                    "Detected an attempt to set as dirty an unknown page " + pageId + ". Known pages: " + activePages);
            throw new IllegalStateException("attempted to set an unknown page as dirty " + pageId);
        }
        metadata.dirt.add(size);
    }

    void setPageDirty(Long pageId, Record dirtyRecord) {
        if (dirtyRecord == null) {
            setPageDirty(pageId);
        } else {
            setPageDirty(pageId, DataPage.estimateEntrySize(dirtyRecord));
        }
    }

    @Override
    public String toString() {
        return "PageSet{" + activePages + "}";
    }

    int getDirtyPagesCount() {
        return activePages.values().stream().mapToInt(meta -> meta.dirt.sum() > 0 ? 1 : 0).sum();
    }

    /**
     * @throws IllegalStateException if a page with give id already existed
     */
    void pageCreated(Long pageId, DataPage page) throws IllegalStateException {
        /* Don't really add page if already exists */
        final DataPageMetaData old = activePages.putIfAbsent(pageId, new DataPageMetaData(page));
        if (old != null) {
            LOGGER.log(Level.SEVERE,
                    "Detected concurrent creation of page " + page.pageId + ", writable: " + page.writable);

            throw new IllegalStateException("Creating a new page already existing! Page " + pageId);
        }
    }

    void checkpointDone(Collection<Long> pagesFlushed) {
        activePages.keySet().removeAll(pagesFlushed);
    }

}
