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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import herddb.core.DataPage.DataPageMetaData;

/**
 * Simple randomic implementation of {@link PageReplacementPolicy}.
 * <p>
 * Discarding pegas will been choosen in a randomic way.
 * </p>
 *
 * @author diego.salvi
 */
public class RandomPageReplacementPolicy implements PageReplacementPolicy {

    private final PlainMetadata[] pages;
    private final Map<PlainMetadata,Integer> positions;

    private final Random random = new Random();

    /** Modification lock */
    private final Lock lock = new ReentrantLock();

    public RandomPageReplacementPolicy (int size) {
        pages = new PlainMetadata[size];

        positions = new HashMap<>(size);
    }


    @Override
    public PlainMetadata add(DataPage page) {
        lock.lock();

        final PlainMetadata metadata = new PlainMetadata(page);
        page.metadata = metadata;

        try {
            int count = positions.size();
            if (count < pages.length) {
                pages[count] = metadata;
                positions.put(metadata,count);

                return null;
            } else {
                int position = random.nextInt(count);

                PlainMetadata old = pages[position];
                positions.remove(old);

                pages[position] = metadata;
                positions.put(metadata, position);

                return old;
            }
        } finally {
            lock.unlock();
        }
    }


    public PlainMetadata pop() {
        lock.lock();
        try {
            int count = positions.size();
            int position = random.nextInt(count);

            PlainMetadata old = pages[position];
            positions.remove(old);

            if (count > 0) {
                PlainMetadata moving = pages[count -1];
                pages[position] = moving;
                positions.put(moving, position);
            }

            return old;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        return positions.size();
    }

    @Override
    public int capacity() {
        return pages.length;
    }


    @Override
    public void remove(Collection<DataPage> pages) {
        for(DataPage page : pages) {
            remove(page);
        }
    }



    @Override
    public boolean remove(DataPage page) {
        lock.lock();

        final PlainMetadata metadata = (PlainMetadata) page.metadata;

        try {
            Integer position = positions.get(metadata);

            if (position == null) return false;

            int count = positions.size();
            if (count > 0) {
                PlainMetadata moving = pages[count -1];
                pages[position] = moving;
                positions.put(moving, position);
            }

            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            positions.clear();

            for(int i = 0; i < pages.length; ++i) {
                pages[i] = null;
            }
        } finally {
            lock.unlock();
        }
    }


    @Override
    public ConcurrentMap<Long, DataPage> createObservedPagesMap() {
        /* No observation needed in a random strategy */
        return new ConcurrentHashMap<>();
    }

    /**
     * Implementation of {@link DataPageMetaData} with all data needed for {@link RandomPageReplacement}.
     *
     * @author diego.salvi
     */
    private static final class PlainMetadata implements DataPageMetaData {

        public final TableManager owner;
        public final long pageId;

        private final int hashcode;

        public PlainMetadata(DataPage datapage) {
            this(datapage.owner, datapage.pageId);
        }

        public PlainMetadata(TableManager owner, long pageId) {
            super();
            this.owner = owner;
            this.pageId = pageId;

            hashcode = Objects.hash(owner,pageId);
        }

        @Override
        public int hashCode() {
            return hashcode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof PlainMetadata)) {
                return false;
            }
            PlainMetadata other = (PlainMetadata) obj;
            if (owner == null) {
                if (other.owner != null) {
                    return false;
                }
            } else if (!owner.equals(other.owner)) {
                return false;
            }
            if (pageId != other.pageId) {
                return false;
            }
            return true;
        }

        @Override
        public TableManager getOwner() {
            return owner;
        }

        @Override
        public long getPageId() {
            return pageId;
        }

        @Override
        public String toString() {
            return "PlainMetadata {pageId=" + pageId + ", owner=" + owner + '}';
        }
    }
}
