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

package herddb.index.brin;

import herddb.core.Page;
import herddb.core.Page.Metadata;
import herddb.core.PageReplacementPolicy;
import herddb.storage.DataStorageManagerException;
import herddb.utils.EnsureLongIncrementAccumulator;
import herddb.utils.SizeAwareObject;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Very Simple BRIN (Block Range Index) implementation with pagination managed by a {@link PageReplacementPolicy}
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public final class BlockRangeIndex<K extends Comparable<K> & SizeAwareObject, V extends SizeAwareObject> {

    private static final Logger LOG = Logger.getLogger(BlockRangeIndex.class.getName());

    private static final long ENTRY_CONSTANT_BYTE_SIZE = 93;
    private static final long BLOCK_CONSTANT_BYTE_SIZE = 128;

    private final long maxPageBlockSize;
    private final long minPageBlockSize;

    private final ConcurrentNavigableMap<BlockStartKey<K>, Block<K, V>> blocks = new ConcurrentSkipListMap<>();
    private final AtomicLong currentBlockId = new AtomicLong(0L);

    private final IndexDataStorage<K, V> dataStorage;

    private final PageReplacementPolicy pageReplacementPolicy;

    public BlockRangeIndex(long maxBlockSize, PageReplacementPolicy pageReplacementPolicy) {
        this(maxBlockSize, pageReplacementPolicy, new MemoryIndexDataStorage<>());
    }

    public BlockRangeIndex(long maxBlockSize, PageReplacementPolicy pageReplacementPolicy, IndexDataStorage<K, V> dataStorage) {
        this.maxPageBlockSize = maxBlockSize - BLOCK_CONSTANT_BYTE_SIZE;
        if (maxBlockSize < 0) {
            throw new IllegalArgumentException("page size to small to store any index entry: " + maxBlockSize);
        }
        this.minPageBlockSize = maxBlockSize / 3;

        this.pageReplacementPolicy = pageReplacementPolicy;
        this.dataStorage = dataStorage;
    }

    static final class BlockStartKey<K extends Comparable<K>> implements Comparable<BlockStartKey<K>> {

        static final BlockStartKey<?> HEAD_KEY = new BlockStartKey<>(null, 0L);

        public final K minKey;
        public final long blockId;

        @Override
        public String toString() {
            if (minKey == null) {
                return "BlockStartKey{HEAD}";
            } else {
                return "BlockStartKey{" + minKey + "," + blockId + '}';
            }
        }

        @SuppressWarnings("unchecked")
        public static <X extends Comparable<X>> BlockStartKey<X> valueOf(X minKey, long segmentId) {
            if (minKey == null) {
                if (segmentId != HEAD_KEY.blockId) {
                    throw new IllegalArgumentException();
                }
                return (BlockStartKey<X>) HEAD_KEY;
            }
            return new BlockStartKey<>(minKey, segmentId);
        }

        private BlockStartKey(K minKey, long segmentId) {
            this.minKey = minKey;
            this.blockId = segmentId;
        }

        @Override
        public int compareTo(BlockStartKey<K> o) {
            if (o == this) {
                return 0;
            } else if (HEAD_KEY == this) {
                return -1;
            } else if (o == HEAD_KEY) {
                return 1;
            }
            int diff = this.minKey.compareTo(o.minKey);
            if (diff != 0) {
                return diff;
            }
            return Long.compare(blockId, o.blockId);
        }

        public int compareMinKey(K other) {
            if (HEAD_KEY == this) {
                return -1;
            }

            return this.minKey.compareTo(other);
        }


        @Override
        public int hashCode() {
            int hash = 3;
            hash = 67 * hash + Objects.hashCode(this.minKey);
            hash = 67 * hash + Long.hashCode(this.blockId);
            return hash;
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
            final BlockStartKey<?> other = (BlockStartKey<?>) obj;
            if (this.blockId != other.blockId) {
                return false;
            }
            return Objects.equals(this.minKey, other.minKey);
        }

    }

    private static final class BRINPage<Key extends Comparable<Key> & SizeAwareObject, Val extends SizeAwareObject>
            extends Page<Block<Key, Val>> {

        public BRINPage(Block<Key, Val> owner, long blockId) {
            super(owner, blockId);
        }

        @Override
        public String toString() {
            return "BRINPage [owner=" + owner + ", pageId=" + pageId + "]";
        }

    }

    private long evaluateEntrySize(K key, V value) {
        final long size = key.getEstimatedSize() + value.getEstimatedSize() + ENTRY_CONSTANT_BYTE_SIZE;
        if (size > maxPageBlockSize) {
            throw new IllegalStateException(
                    "entry too big to fit in any page " + size + " bytes");
        }
        return size;
    }

    private static class PutState<Key extends Comparable<Key> & SizeAwareObject, Val extends SizeAwareObject> {
        Block<Key, Val> next;

        public PutState() {
            super();
        }
    }

    private static class DeleteState<Key extends Comparable<Key> & SizeAwareObject, Val extends SizeAwareObject> {
        Block<Key, Val> next;

        public DeleteState() {
            super();
        }
    }

    private static class LookupState<Key extends Comparable<Key> & SizeAwareObject, Val extends SizeAwareObject> {
        Block<Key, Val> next;
        List<Val> found;

        public LookupState() {
            super();

            found = new ArrayList<>();
        }
    }

    static final class Block<Key extends Comparable<Key> & SizeAwareObject, Val extends SizeAwareObject> implements Page.Owner {

        final BlockRangeIndex<Key, Val> index;
        final BlockStartKey<Key> key;

        NavigableMap<Key, List<Val>> values;
        long size;
        Block<Key, Val> next;

        private final ReentrantLock lock = new ReentrantLock(true);

        private volatile boolean loaded;
        private volatile boolean dirty;
        private volatile long pageId;

        private final BRINPage<Key, Val> page;

        /**
         * Constructor for restore operations
         */
        public Block(BlockRangeIndex<Key, Val> index, BlockStartKey<Key> key, long size, long pageId, Block<Key, Val> next) {
            this.index = index;
            this.key = key;

            this.size = size;
            this.next = next;

            this.loaded = false;
            this.dirty = false;
            this.pageId = pageId;

            /* Immutable Block ID */
            page = new BRINPage<>(this, key.blockId);
        }

        /**
         * Constructor for head block
         */
        @SuppressWarnings("unchecked")
        public Block(BlockRangeIndex<Key, Val> index) {
            this.index = index;
            this.key = (BlockStartKey<Key>) BlockStartKey.HEAD_KEY;

            this.values = new TreeMap<>();
            this.size = 0;

            this.loaded = true;
            this.dirty = true;
            this.pageId = IndexDataStorage.NEW_PAGE;

            /* Immutable Block ID */
            page = new BRINPage<>(this, key.blockId);
        }

        /**
         * Construtor for split operations
         */
        private Block(BlockRangeIndex<Key, Val> index, BlockStartKey<Key> key, NavigableMap<Key, List<Val>> values, long size, Block<Key, Val> next) {
            this.index = index;
            this.key = key;

            this.values = values;
            this.size = size;
            this.next = next;

            this.loaded = true;
            this.dirty = true;
            this.pageId = IndexDataStorage.NEW_PAGE;

            /* Immutable Block ID */
            page = new BRINPage<>(this, key.blockId);
        }

        long getSize() {
            return size;
        }

        boolean isLoaded() {
            return loaded;
        }

        boolean isDirty() {
            return dirty;
        }

        private void mergeAddValue(Key key1, Val value, Map<Key, List<Val>> values) {
            List<Val> valuesForKey = values.get(key1);
            if (valuesForKey == null) {
                valuesForKey = new ArrayList<>();
                values.put(key1, valuesForKey);
            }
            valuesForKey.add(value);
        }

        void addValue(Key key, Val value, PutState<Key, Val> state) {

            /* Eventual new block from split. It must added to PageReplacementPolicy only after lock release */
            Block<Key, Val> newblock = null;
            lock.lock();
            Page.Metadata unload = null;
            try {

                Block<Key, Val> currentNext = this.next;
                if (currentNext != null && currentNext.key.compareMinKey(key) <= 0) {
                    // unfortunately this occours during split
                    // put #1 -> causes split
                    // put #2 -> designates this block for put, but the split is taking place
                    // put #1 returns
                    // put #2 needs to addValue to the 'next' (split result) block not to this

                    /* Add to next */
                    state.next = currentNext;
                    return;
                }

                unload = ensureBlockLoadedWithoutUnload();

                mergeAddValue(key, value, values);
                size += index.evaluateEntrySize(key, value);
                dirty = true;

                if (size > index.maxPageBlockSize) {
                    newblock = split();
                }
            } finally {
                lock.unlock();
                if (unload != null) {
                    unload.owner.unload(unload.pageId);
                }
            }

            if (newblock != null) {
                final Page.Metadata newBlockUnload = index.pageReplacementPolicy.add(newblock.page);
                if (newBlockUnload != null) {
                    newBlockUnload.owner.unload(newBlockUnload.pageId);
                }
            }

            /* Added */
            /* No more next */
            state.next = null;
        }

        void delete(Key key, Val value, DeleteState<Key, Val> state) {

            lock.lock();
            Page.Metadata unload = null;
            try {

                final Block<Key, Val> currentNext = this.next;

                /*
                 * Compare deletion key with next block min key. If no next block exists
                 * comparison is set to an arbitrary value needed only for initialization
                 */
                final int nextMinKeyCompare = currentNext == null ? -1 : currentNext.key.compareMinKey(key);

                /*
                 * If reached during split this block could be too "small". In such case we
                 * don't need to attempt any load but send directly to next. Pay attention that
                 * if next block min key is equal to needed key we must look in this node too
                 * (the key list could be split between both nodes)
                 */
                if (currentNext == null || nextMinKeyCompare >= 0) {
                    unload = ensureBlockLoadedWithoutUnload();
                    List<Val> valuesForKey = values.get(key);
                    if (valuesForKey != null) {
                        boolean removed = valuesForKey.remove(value);
                        if (removed) {
                            if (valuesForKey.isEmpty()) {
                                values.remove(key);
                            }
                            size -= index.evaluateEntrySize(key, value);
                            dirty = true;

                            /* Value removed, stop deletions */
                            /* No more next */
                            state.next = null;

                            return;
                        }
                    }
                }


                /*
                 * Propagate to next only if it exist AND next min key isn't greater than requested
                 * key (ie: only if next could have any useful data)
                 */
                if (currentNext != null && nextMinKeyCompare <= 0) {
                    state.next = currentNext;
                } else {
                    /* No more next */
                    state.next = null;
                }

            } finally {
                lock.unlock();
                if (unload != null) {
                    unload.owner.unload(unload.pageId);
                }
            }

        }

        void lookUpRange(Key firstKey, Key lastKey, LookupState<Key, Val> state) {

            lock.lock();
            Page.Metadata unload = null;

            /*
             * If we got here means that at some point this block had a min key compatible
             * with lookup limits. Because min key never change we could be skipped only if
             * first requested key is greater than next min key (due to an occurring split
             * while selecting the blocks)
             */

            try {

                final Block<Key, Val> currentNext = this.next;

                if (firstKey != null && lastKey != null) {

                    /*
                     * If reached during split this block could be too "small". In such case we
                     * don't need to attempt any load but send directly to next. Pay attention that
                     * if next block min key is equal to needed key we must look in this node too
                     * (the key list could be split between both nodes)
                     */
                    if (currentNext == null || currentNext.key.compareMinKey(firstKey) >= 0) {
                        // index seek case
                        unload = ensureBlockLoadedWithoutUnload();
                        if (firstKey.equals(lastKey)) {
                            List<Val> seek = values.get(firstKey);
                            if (seek != null && !seek.isEmpty()) {
                                state.found.addAll(seek);
                            }
                        } else if (lastKey.compareTo(firstKey) < 0) {
                            // no value is possible
                        } else {
                            values.subMap(firstKey, true, lastKey, true).forEach((k, seg) -> {
                                state.found.addAll(seg);
                            });
                        }
                    }

                } else if (firstKey != null) {

                    /*
                     * If reached during split this block could be too "small". In such case we
                     * don't need to attempt any load but send directly to next. Pay attention that
                     * if next block min key is equal to needed key we must look in this node too
                     * (the key list could be split between both nodes)
                     */
                    if (currentNext == null || currentNext.key.compareMinKey(firstKey) >= 0) {
                        // index seek case
                        unload = ensureBlockLoadedWithoutUnload();
                        values.tailMap(firstKey, true).forEach((k, seg) -> {
                            state.found.addAll(seg);
                        });
                    }
                } else {
                    unload = ensureBlockLoadedWithoutUnload();
                    values.headMap(lastKey, true).forEach((k, seg) -> {
                        state.found.addAll(seg);
                    });
                }
                /*
                 * Propagate to next only if it exist AND next min key is less or equal than requested
                 * last key (ie: only if next could have any useful data)
                 */
                if (currentNext != null && (lastKey == null || currentNext.key.compareMinKey(lastKey) <= 0)) {
                    state.next = currentNext;
                } else {

                    /* No more next */
                    state.next = null;
                }

            } finally {
                lock.unlock();
                if (unload != null) {
                    unload.owner.unload(unload.pageId);
                }
            }
        }

        void ensureBlockLoaded() {
            final Page.Metadata unload = ensureBlockLoadedWithoutUnload(true);
            if (unload != null) {
                unload.owner.unload(unload.pageId);
            }
        }

        void ensureBlockLoadedLocally() {
            ensureBlockLoadedWithoutUnload(false);
        }

        Page.Metadata ensureBlockLoadedWithoutUnload() {
            return ensureBlockLoadedWithoutUnload(true);
        }

        private Page.Metadata ensureBlockLoadedWithoutUnload(boolean addPageToPageReplacementPolicy) {
            if (!loaded) {
                try {
                    values = new TreeMap<>();

                    /*
                     * Skip load and add if we already know that there isn't data. Add is a really
                     * little overhead but load should read from disk so we just skip such unuseful
                     * roundtrip
                     */
                    if (size != 0) {
                        List<Map.Entry<Key, Val>> loadDataPage = index.dataStorage.loadDataPage(pageId);

                        for (Map.Entry<Key, Val> entry : loadDataPage) {
                            mergeAddValue(entry.getKey(), entry.getValue(), values);
                        }
                    }

                    loaded = true;

                    if (addPageToPageReplacementPolicy) {
                        /* Deferred page unload */
                        final Page.Metadata unload = index.pageReplacementPolicy.add(page);
                        return unload;
                    }

                    return null;

                } catch (IOException err) {
                    throw new RuntimeException(err);
                }
            } else {
                /* Signal the page hit to replacement policy only if we were willing to load the page on it */
                if (addPageToPageReplacementPolicy) {
                    index.pageReplacementPolicy.pageHit(page);
                }
            }

            return null;
        }

        /**
         * Return the newly generated block if any
         */
        private Block<Key, Val> split() {
            if (size < index.maxPageBlockSize) {
                throw new IllegalStateException("Split on a non overflowing block");
            }

            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "Split: FK " + key, new Object[]{key});
            }
            NavigableMap<Key, List<Val>> keepValues = new TreeMap<>();
            NavigableMap<Key, List<Val>> otherValues = new TreeMap<>();

            final long splitSize = size / 2;

            long mySize = 0;
            long otherSize = 0;
            for (Map.Entry<Key, List<Val>> entry : values.entrySet()) {
                final Key key = entry.getKey();
                for (Val v : entry.getValue()) {
                    final long entrySize = index.evaluateEntrySize(key, v);
                    if (mySize < splitSize) {
                        mergeAddValue(key, v, keepValues);
                        mySize += entrySize;
                    } else {
                        mergeAddValue(key, v, otherValues);
                        otherSize += entrySize;
                    }
                }
            }

            if (otherValues.isEmpty()) {
                return null;
            }

            Key newOtherMinKey = otherValues.firstKey();

            long newBlockId = index.currentBlockId.incrementAndGet();

            /*
             * For right sorting reasons block id must be negative if the splitting key is greater or equals
             * than the current min key. Otherwise, when splitting a block with the same splitting key and min
             * key the block id must be positive.
             *
             * Positive or negative means: negative: the new block must precede any other block with the same
             * "new" min key that could exists positive: the new block must follow the current splitting bloc
             * (this case is done right because we add elements in the greater block possible: if we have more
             * blocks with the same min key we use the last one).
             */
            /**
             * <pre>
             *        +-----------------+    +-----------------+
             *        | MinKey: 1       |    | MinKey: 3       |
             * ... -> | BlockId: 10     | -> | BlockId: 11     | -> ...
             *        | Keys: 1, 2, 2   |    | Keys: 3, 4      |
             *        | Occupancy: 100% |    | Occupancy: 66%  |
             *        +-----------------+    +-----------------+
             *
             * Adding key 2 with next block id 12: create a new block (2,-12)
             *
             *        +-----------------+    +-----------------+    +-----------------+
             *        | MinKey: 1       |    | MinKey: 2       |    | MinKey: 3       |
             * ... -> | BlockId: 10     | -> | BlockId: -12    | -> | BlockId: 11     | -> ...
             *        | Keys: 1, 2      |    | Keys: 2, 2      |    | Keys: 3, 4      |
             *        | Occupancy: 66%  |    | Occupancy: 66%  |    | Occupancy: 66%  |
             *        +-----------------+    +-----------------+    +-----------------+
             *
             * Adding key 2 with next block id 13: add to block (2,-12)
             *
             *        +-----------------+    +-----------------+    +-----------------+
             *        | MinKey: 1       |    | MinKey: 2       |    | MinKey: 3       |
             * ... -> | BlockId: 10     | -> | BlockId: -12    | -> | BlockId: 11     | -> ...
             *        | Keys: 1, 2      |    | Keys: 2, 2, 2   |    | Keys: 3, 4      |
             *        | Occupancy: 66%  |    | Occupancy: 100% |    | Occupancy: 66%  |
             *        +-----------------+    +-----------------+    +-----------------+
             *
             * Adding key 2 with next block id 13: create a new block (2,13)
             *
             *        +-----------------+    +-----------------+    +-----------------+    +-----------------+
             *        | MinKey: 1       |    | MinKey: 2       |    | MinKey: 2       |    | MinKey: 3       |
             * ... -> | BlockId: 10     | -> | BlockId: -12    | -> | BlockId: 13     | -> | BlockId: 11     | -> ...
             *        | Keys: 1, 2      |    | Keys: 2, 2      |    | Keys: 2, 2      |    | Keys: 3, 4      |
             *        | Occupancy: 66%  |    | Occupancy: 66%  |    | Occupancy: 66%  |    | Occupancy: 66%  |
             *        +-----------------+    +-----------------+    +-----------------+    +-----------------+
             *
             * Notice than being the splitting (2,-12) permits to sort in the right order with the new block (2,13).
             * The same thing would have happened if the block (1,10) was the HEAD block instead (just it doesn't have
             * a min key).
             * </pre>
             */
            if (key == BlockStartKey.HEAD_KEY || !key.minKey.equals(newOtherMinKey)) {
                newBlockId = -newBlockId;
            }

            Block<Key, Val> newblock = new Block<>(index, BlockStartKey.valueOf(newOtherMinKey, newBlockId), otherValues, otherSize, next);

            this.next = newblock;
            this.size = mySize;
            this.values = keepValues;


            /*
             * First publish the new block then reduce this block min/max keys. If done otherwise a
             * concurrent lookup thread could miss the block containing the right data.
             *
             * blocks.put acts as memory barrier (contains at least a volatile access thus reordering is
             * blocked [happen before])
             */
            // access to external field, this is the cause of most of the concurrency problems
            index.blocks.put(newblock.key, newblock);

            return newblock;

        }

        private BlockRangeIndexMetadata.BlockMetadata<Key> checkpointNoLock() throws IOException {
            if (!dirty || !loaded) {
                final Long nextBlockId = next == null ? null : next.key.blockId;
                return new BlockRangeIndexMetadata.BlockMetadata<>(key.minKey, key.blockId, size, pageId, nextBlockId);
            }
            List<Map.Entry<Key, Val>> result = new ArrayList<>();
            values.forEach((k, l) -> {
                l.forEach(v -> {
                    result.add(new AbstractMap.SimpleImmutableEntry<>(k, v));
                });
            });

            long newPageId = index.dataStorage.createDataPage(result);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("checkpoint block " + key + ": newpage -> " + newPageId + " with " + values.size() + " entries x " + result.size() + " pointers");
            }
            this.dirty = false;
            this.pageId = newPageId;

            final Long nextBlockId = next == null ? null : next.key.blockId;
            return new BlockRangeIndexMetadata.BlockMetadata<>(key.minKey, key.blockId, size, pageId, nextBlockId);
        }

        private BlockRangeIndexMetadata.BlockMetadata<Key> checkpoint() throws IOException {
            lock.lock();
            try {
                return checkpointNoLock();
            } finally {
                lock.unlock();
            }

        }

        boolean unloadNoLock() throws IOException {
            if (!loaded) {
                return false;
            }
            if (dirty) {
                checkpoint();
            }
            values = null;
            loaded = false;

            return true;
        }

        boolean unload() throws IOException {
            lock.lock();
            try {
                return unloadNoLock();
            } finally {
                lock.unlock();
            }
        }

        /**
         * Do not execute a checkpoint during unload procedure even if needed.
         */
        private boolean forcedUnload() {
            lock.lock();
            try {
                if (!loaded) {
                    return false;
                }
                values = null;
                loaded = false;

                return true;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void unload(long pageId) {
            if (page.pageId == this.page.pageId) {
                try {
                    unload();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new IllegalArgumentException("Expecting to receive managed page " + this.page.pageId + " got " + pageId);
            }
        }

        @Override
        public String toString() {
            return "Block{" + "key=" + key + ", minKey=" + key.minKey + ", size=" + size + ", next=" + (next == null ? null : next.key) + "}";
        }

    }

    public int getNumBlocks() {
        return blocks.size();
    }

    private BlockRangeIndexMetadata.BlockMetadata<K> merge(Block<K, V> first, List<Block<K, V>> merging) throws IOException {

        final boolean fineEnabled = LOG.isLoggable(Level.FINE);

        /* No real merge */
        if (merging.isEmpty()) {

            if (fineEnabled) {
                LOG.fine("block " + first.pageId + " (" + first.key + ") has " + first + " byte size at checkpoint");
            }
            return first.checkpoint();
        }


        /*
         * Data is provided in reverse order (smaller block is last) so we must iterate
         * in reverse order (to preserve "next" relationship)
         */
        final ListIterator<Block<K, V>> iterator = merging.listIterator(merging.size());

        Page.Metadata unload = null;

        first.lock.lock();
        BlockRangeIndexMetadata.BlockMetadata<K> metadata;

        try {

            /* We need block data to attempt merge */
            unload = first.ensureBlockLoadedWithoutUnload();

            while (iterator.hasPrevious()) {

                Block<K, V> other = iterator.previous();

                other.lock.lock();
                try {

                    /* Real merging is needed only if there is some data to merge, otherwise we just "delete" it */
                    if (other.size != 0) {

                        /* Loading other block data on local thread memory, it will be discarded at merge end */
                        other.ensureBlockLoadedLocally();

                        /* Recover potetially overwritten data before merge. */
                        List<V> potentialOverwrite = first.values.get(other.key.minKey);

                        /* Merge maps */
                        first.values.putAll(other.values);

                        /*
                         * Blocks splitted on the same key: the have a List each with the same key! We
                         * still use putAll because is more efficient on TreeMap when given a SortedMap
                         * but we need to restore original list data from first value (due to overwrite)
                         */
                        if (potentialOverwrite != null) {
                            /* Restore overwiritten data */
                            first.values.merge(other.key.minKey, potentialOverwrite,
                                    (l1, l2) -> {
                                        l1.addAll(l2);
                                        return l1;
                                    });
                        }

                        first.size += other.size;
                    }

                    if (fineEnabled) {

                        if (other.size != 0) {
                            LOG.fine("unlinking block " + first.pageId + " (" + first.key + ") from merged block "
                                    + other.pageId + " (" + other.key + ")");
                        } else {
                            LOG.fine("unlinking block " + first.pageId + " (" + first.key + ") from deleted block "
                                    + other.pageId + " (" + other.key + ")");
                        }

                        if (other.next != null) {
                            LOG.fine("linking block " + first.pageId + " (" + first.key + ") to real next block "
                                    + other.next.pageId + " (" + other.next.key + ")");
                        }
                    }

                    /* Update next reference */
                    first.next = other.next;

                    /* Data not needed anymore, no checkpoint needed for discarded blocks */
                    other.forcedUnload();

                    /* Remove the block from knowledge */
                    blocks.remove(other.key);

                } finally {
                    other.lock.unlock();
                }

            }

            metadata = first.checkpointNoLock();

        } finally {
            first.lock.unlock();

            /* Deferred unload */
            if (unload != null) {
                unload.owner.unload(unload.pageId);
            }
        }

        if (fineEnabled) {
            LOG.fine("merged block " + first.pageId + " (" + first.key + ") has " + first.size + " byte size at checkpoint");
        }

        return metadata;

    }

    public BlockRangeIndexMetadata<K> checkpoint() throws IOException {
        final boolean fineEnabled = LOG.isLoggable(Level.FINE);

        List<BlockRangeIndexMetadata.BlockMetadata<K>> blocksMetadata = new ArrayList<>();

        /* Reverse ordering! */
        Iterator<Block<K, V>> iterator = blocks.descendingMap().values().iterator();

        long mergeSize = 0;

        Block<K, V> lastMergeReference = null;
        List<Block<K, V>> mergeReferences = new ArrayList<>();

        /* Inverse iteration! (Last block is HEAD block) */
        while (iterator.hasNext()) {
            Block<K, V> block = iterator.next();

            block.lock.lock();

            try {

                final long size = block.size;

                if (size < minPageBlockSize) {

                    /* Attempt merges! */
                    mergeSize += size;

                    /* Do merges if overflowing */
                    if (mergeSize > maxPageBlockSize) {

                        /*
                         * If mergeSize now is greater than mexPageBlockSize but size is lower than
                         * minPageBlockSize means that there is at least lastMergeReference not null.
                         */

                        /*
                         * This should be merged with merge stream but it would create a node too big,
                         * we must merge remaining stream and create a new stream.
                         */

                        /* Merge blocks, unloading merged ones and checkpointing last merge reference */
                        BlockRangeIndexMetadata.BlockMetadata<K> merged = merge(lastMergeReference, mergeReferences);

                        blocksMetadata.add(merged);

                        /*
                         * Remove handled merge references (but set lastMergeReference to current block,
                         * it will be merged in next iterations)
                         */
                        lastMergeReference = block;
                        mergeReferences.clear();

                        /* Reset merge size to current block size */
                        mergeSize = size;

                    } else {

                        /* There is still space for merging */
                        if (lastMergeReference != null) {
                            mergeReferences.add(lastMergeReference);
                        }
                        lastMergeReference = block;
                    }

                } else {

                    /* Do not attempt merges! */

                    /* If we have pending merges we must merge them now */
                    if (lastMergeReference != null) {

                        /* Merge blocks, unloading merged ones and checkpointing last merge reference */
                        BlockRangeIndexMetadata.BlockMetadata<K> merged = merge(lastMergeReference, mergeReferences);

                        blocksMetadata.add(merged);

                        /* Remove handled merge references */
                        lastMergeReference = null;
                        mergeReferences.clear();

                        /* Reset merge size to 0 */
                        mergeSize = 0;

                    }

                    /* Now checkpointing current block (no merge) */

                    if (fineEnabled) {
                        LOG.fine("block " + block.pageId + " (" + block.key + ") has " + size + " byte size at checkpoint");
                    }

                    /* We already have the lock */
                    blocksMetadata.add(block.checkpointNoLock());

                }

            } finally {
                block.lock.unlock();
            }
        }

        /*
         * We need to handled any remaining merges if exists (lastMergeReference is the head now).
         */
        if (lastMergeReference != null) {

            /* Merge blocks, unloading merged ones and checkpointing last merge reference */
            BlockRangeIndexMetadata.BlockMetadata<K> merged = merge(lastMergeReference, mergeReferences);

            blocksMetadata.add(merged);

            /* Remove handled merge references */
            lastMergeReference = null;
            mergeReferences.clear();
        }

        return new BlockRangeIndexMetadata<>(blocksMetadata);

    }

    String generateDetailedInternalStatus() {

        final StringBuilder builder = new StringBuilder();

        builder.append("\nBRIN detailed internal status:\n")
                .append(" - currentBlockId: ").append(currentBlockId.get()).append('\n')
                .append(" - minPageBlockSize: ").append(minPageBlockSize).append('\n')
                .append(" - maxPageBlockSize: ").append(maxPageBlockSize).append('\n')
                .append(" - blocks: ").append(blocks.size()).append('\n');

        builder.append("Ordered blocks navigation:").append('\n');

        blocks.forEach((k, b) -> {
            builder
                    .append("----[Block ").append(k.blockId)
                    .append(" MinKey ").append(k.minKey).append("]----").append('\n')
                    .append("key: ").append("block ").append(b.key.blockId)
                    .append(" minKey ").append(b.key.minKey).append('\n');
            if (b.next == null) {
                builder
                        .append("next: null").append('\n');
            } else {
                builder
                        .append("next: block").append(b.next.key.blockId)
                        .append(" min key ").append(b.next.key.minKey).append('\n');
            }
            builder
                    .append("pageId: ").append(b.pageId).append('\n')
                    .append("size: ").append(b.size).append('\n')
                    .append("loaded: ").append(b.loaded).append('\n')
                    .append("dirty: ").append(b.dirty).append('\n');
        });

        return builder.toString();

    }

    public void put(K key, V value) {

        /* Lookup from the last possible block where we could insert the value */
        final BlockStartKey<K> lookUp = BlockStartKey.valueOf(key, Long.MAX_VALUE);

        final PutState<K, V> state = new PutState<>();

        /* There is always at least the head block! */
        state.next = blocks.floorEntry(lookUp).getValue();
        do {
            state.next.addValue(key, value, state);
        } while (state.next != null);

    }

    public void delete(K key, V value) {

        /* Lookup from the first possible block that could contain the value*/
        final BlockStartKey<K> lookUp = BlockStartKey.valueOf(key, Long.MIN_VALUE);

        final DeleteState<K, V> state = new DeleteState<>();

        /* There is always at least the head block! */
        state.next = blocks.floorEntry(lookUp).getValue();
        do {
            state.next.delete(key, value, state);
        } while (state.next != null);
    }

    public List<V> search(K firstKey, K lastKey) {

        final LookupState<K, V> state = new LookupState<>();

        if (firstKey != null) {
            /* Lookup from the first possible block that could contain the first lookup key*/
            final BlockStartKey<K> lookUp = BlockStartKey.valueOf(firstKey, Long.MIN_VALUE);

            /* There is always at least the head block! */
            state.next = blocks.floorEntry(lookUp).getValue();
        } else {
            /* Use first entry and not HEAD block lookup just because has better performances */
            state.next = blocks.firstEntry().getValue();
        }

        do {
            state.next.lookUpRange(firstKey, lastKey, state);
        } while (state.next != null);

        return state.found;
    }

    public List<V> search(K key) {
        return search(key, key);
    }

    public Stream<V> query(K firstKey, K lastKey) {
        return search(firstKey, lastKey).stream();
    }

    public Stream<V> query(K key) {
        return query(key, key);
    }

    public boolean containsKey(K key) {
        return !search(key, key).isEmpty();
    }

    public void boot(BlockRangeIndexMetadata<K> metadata) throws DataStorageManagerException {
        if (metadata.getBlocksMetadata().size() > 0) {
            LOG.info("boot index, with " + metadata.getBlocksMetadata().size() + " blocks");
        }
        if (metadata.getBlocksMetadata().size() == 0) {

            reset();

        } else {

            clear();

            /* Metadata are saved/recovered in reverse order so "next" block has been already created */
            Block<K, V> next = null;
            for (BlockRangeIndexMetadata.BlockMetadata<K> blockData : metadata.getBlocksMetadata()) {
                /*
                 * TODO: if the system is restart with a different (smaller) page size old blocks will remain
                 * bigger until a split occurs.
                 */

                /* Medatada safety check (do not trust blindly ordering) */
                if (blockData.nextBlockId != null) {
                    if (next == null) {
                        throw new DataStorageManagerException("Wrong next block, expected notingh but "
                                + blockData.nextBlockId + " found");
                    } else if (next.key.blockId != blockData.nextBlockId.longValue()) {
                        throw new DataStorageManagerException("Wrong next block, expected " + next.key.blockId + " but "
                                + blockData.nextBlockId + " found");
                    }
                } else {
                    if (next != null) {
                        throw new DataStorageManagerException(
                                "Wrong next block, expected " + next.key.blockId + " but nothing found");
                    }

                }

                final BlockStartKey<K> key = BlockStartKey.valueOf(blockData.firstKey, blockData.blockId);

                next = new Block<>(this, key, blockData.size, blockData.pageId, next);
                blocks.put(key, next);

                currentBlockId.accumulateAndGet(Math.abs(next.key.blockId), EnsureLongIncrementAccumulator.INSTANCE);
            }
        }

    }

    @SuppressWarnings("unchecked")
    void reset() {

        clear();

        /* Create the head block */
        final Block<K, V> headBlock = new Block<>(this);
        blocks.put((BlockStartKey<K>) BlockStartKey.HEAD_KEY, headBlock);

        final Metadata unload = pageReplacementPolicy.add(headBlock.page);
        if (unload != null) {
            unload.owner.unload(unload.pageId);
        }

        currentBlockId.set(0);
    }

    void clear() {

        for (Block<K, V> block : blocks.values()) {
            /* Unload if loaded */
            if (block.forcedUnload()) {
                pageReplacementPolicy.remove(block.page);
            }
        }

        blocks.clear();
    }

    ConcurrentNavigableMap<BlockStartKey<K>, Block<K, V>> getBlocks() {
        return blocks;
    }

    long getCurrentBlockId() {
        return currentBlockId.get();
    }

}
