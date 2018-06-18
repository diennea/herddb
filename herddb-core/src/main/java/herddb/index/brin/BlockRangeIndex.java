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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import herddb.core.Page;
import herddb.core.Page.Metadata;
import herddb.core.PageReplacementPolicy;
import herddb.storage.DataStorageManagerException;
import herddb.utils.EnsureLongIncrementAccumulator;
import herddb.utils.SizeAwareObject;

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
    private final ConcurrentNavigableMap<BlockStartKey<K>, Block<K,V>> blocks = new ConcurrentSkipListMap<>();
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

        public static final <X extends Comparable<X>> BlockStartKey<X> valueOf(X minKey, long segmentId) {
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
            if (!Objects.equals(this.minKey, other.minKey)) {
                return false;
            }
            return true;
        }

    }

    private static final class BRINPage<KEY extends Comparable<KEY> & SizeAwareObject, VAL extends SizeAwareObject>
            extends Page<Block<KEY, VAL>> {

        public BRINPage(Block<KEY,VAL> owner, long blockId) {
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

    private static final class DeleteState <KEY extends Comparable<KEY> & SizeAwareObject, VAL extends SizeAwareObject> {
        final Set<BlockStartKey<KEY>> visited;
        boolean deleted;
        Block<KEY,VAL> next;

        public DeleteState() {
            super();

            visited = new HashSet<>();
        }
    }

    static final class Block<KEY extends Comparable<KEY> & SizeAwareObject, VAL extends SizeAwareObject> implements Page.Owner {

        final BlockRangeIndex<KEY,VAL> index;
        final BlockStartKey<KEY> key;

        NavigableMap<KEY, List<VAL>> values;
        long size;
        Block<KEY,VAL> next;

        private final ReentrantLock lock = new ReentrantLock(true);
        private volatile boolean loaded;
        private volatile boolean dirty;
        private volatile long pageId;

        private final BRINPage<KEY,VAL> page;

        public Block(BlockRangeIndex<KEY,VAL> index, long blockId, KEY firstKey, long size, long pageId, Block<KEY,VAL> next) {
            this.index = index;
            this.key = BlockStartKey.valueOf(firstKey, blockId);

            this.size = size;
            this.next = next;

            this.loaded = false;
            this.dirty = false;
            this.pageId = pageId;

            /* Immutable Block ID */
            page = new BRINPage<>(this, blockId);
        }

        @Deprecated
        public Block(BlockRangeIndex<KEY,VAL> index, KEY firstKey, VAL firstValue) {
            this.index = index;
            this.key = (BlockStartKey<KEY>) BlockStartKey.HEAD_KEY;

            /* TODO: estimate a better sizing for array list */
            List<VAL> firstKeyValues = new ArrayList<>();
            firstKeyValues.add(firstValue);
            values = new TreeMap<>();
            values.put(firstKey, firstKeyValues);
            this.size = index.evaluateEntrySize(firstKey, firstValue);

            this.loaded = true;
            this.dirty = true;
            this.pageId = IndexDataStorage.NEW_PAGE;

            /* Immutable Block ID */
            page = new BRINPage<>(this, key.blockId);
        }

        /** Construtor for split operations */
        private Block(BlockRangeIndex<KEY,VAL> index, KEY minKey, NavigableMap<KEY, List<VAL>> values, long size, Block<KEY,VAL> next) {
            this.index = index;
            this.key = BlockStartKey.valueOf(minKey, index.currentBlockId.incrementAndGet());

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

        private void mergeAddValue(KEY key1, VAL value, Map<KEY, List<VAL>> values) {
            List<VAL> valuesForKey = values.get(key1);
            if (valuesForKey == null) {
                valuesForKey = new ArrayList<>();
                values.put(key1, valuesForKey);
            }
            valuesForKey.add(value);
        }

        Block<KEY,VAL> addValue(KEY key, VAL value) {

            /* Eventual new block from split. It must added to PageReplacementPolicy only after lock release */
            Block<KEY,VAL> newblock = null;
            lock.lock();
            try {
                Block<KEY,VAL> next = this.next;
                if (next != null && next.key.compareMinKey(key) <= 0) {
                    // unfortunately this occours during split
                    // put #1 -> causes split
                    // put #2 -> designates this block for put, but the split is taking place
                    // put #1 returns
                    // put #2 needs to addValue to the 'next' (split result) block not to this

                    /* Add to next */
                    return next;
                }
                ensureBlockLoaded();

                mergeAddValue(key, value, values);
                size += index.evaluateEntrySize(key, value);
                dirty = true;

                if (size > index.maxPageBlockSize) {
                    newblock = split();
                }
            } finally {
                lock.unlock();
            }

            if (newblock != null) {
                final Metadata unload = index.pageReplacementPolicy.add(newblock.page);
                if (unload != null) {
                    unload.owner.unload(unload.pageId);
                }
            }

            /* Added */
            return null;
        }



        void delete(KEY key, VAL value, DeleteState<KEY,VAL> state) {

            if (!state.visited.add(this.key)) {
                /* No more next */
                state.next = null;
                return;
            }

            lock.lock();

            try {

                final Block<KEY,VAL> currentNext = this.next;

                /*
                 * Compare deletion key with next block min key. If no next block exists
                 * comparison is set to an arbitrary value needed only for initialization
                 */
                final int nextMinKeyCompare = currentNext == null ?
                        -1 : currentNext.key.compareMinKey(key);

                /*
                 * If reached during split this block could be too "small". In such case we
                 * don't need to attempt any load but send directly to next. Pay attention that
                 * if next block min key is equal to needed key we must look in this node too
                 * (the key list could be split between both nodes)
                 */
                if (currentNext == null || nextMinKeyCompare >= 0) {
                    ensureBlockLoaded();
                    List<VAL> valuesForKey = values.get(key);
                    if (valuesForKey != null) {
                        boolean removed = valuesForKey.remove(value);
                        if (removed) {
                            if (valuesForKey.isEmpty()) {
                                values.remove(key);
                            }
                            size -= index.evaluateEntrySize(key, value);
                            dirty = true;

                            /* Value removed, stop deletions */
                            state.deleted = true;

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
            }

        }

       void ensureBlockLoaded() {
            if (!loaded) {
                try {
                    values = new TreeMap<>();
                    List<Map.Entry<KEY,VAL>> loadDataPage = index.dataStorage.loadDataPage(pageId);

                    for (Map.Entry<KEY,VAL> entry : loadDataPage) {
                        mergeAddValue(entry.getKey(), entry.getValue(), values);
                    }

                    loaded = true;

                    /* Dereferenced page unload */
                    final Page.Metadata unload = index.pageReplacementPolicy.add(page);
                    if (unload != null) {
                        unload.owner.unload(unload.pageId);
                    }

                } catch (IOException err) {
                    throw new RuntimeException(err);
                }
            } else {
                index.pageReplacementPolicy.pageHit(page);
            }
        }

        Block<KEY,VAL> lookUpRange(KEY firstKey, KEY lastKey, Set<BlockStartKey<KEY>> visitedBlocks, List<List<VAL>> results) {
            if (!visitedBlocks.add(this.key)) {
                return null;
            }

            lock.lock();

            /*
             * If we got here means that at some point this block had a min key compatible
             * with lookup limits. Because min key never change we could be skipped only if
             * first requested key is greater than next min key (due to an occurring split
             * while selecting the blocks)
             */

            try {

                final Block<KEY,VAL> currentNext = this.next;

                if (firstKey != null && lastKey != null) {

                    /*
                     * If reached during split this block could be too "small". In such case we
                     * don't need to attempt any load but send directly to next. Pay attention that
                     * if next block min key is equal to needed key we must look in this node too
                     * (the key list could be split between both nodes)
                     */
                    if ( currentNext == null || currentNext.key.compareMinKey(firstKey) >= 0) {
                        // index seek case
                        ensureBlockLoaded();
                        if (firstKey.equals(lastKey)) {
                            List<VAL> seek = values.get(firstKey);
                            if (seek != null && !seek.isEmpty()) {
                                results.add(seek);
                            }
                        } else if (lastKey.compareTo(firstKey) < 0) {
                            // no value is possible
                        } else {
                            values.subMap(firstKey, true, lastKey, true).forEach((k, seg) -> {
                                results.add(seg);
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
                    if ( currentNext == null || currentNext.key.compareMinKey(firstKey) >= 0) {
                        // index seek case
                        ensureBlockLoaded();
                        values.tailMap(firstKey, true).forEach((k, seg) -> {
                            results.add(seg);
                        });
                    }
                } else {
                    ensureBlockLoaded();
                    values.headMap(lastKey, true).forEach((k, seg) -> {
                        results.add(seg);
                    });
                }

                /*
                 * Propagate to next only if it exist AND current max key isn't greater than requested
                 * last key (ie: only if next could have any useful data)
                 */

                /*
                 * Propagate to next only if it exist AND next min key is less or equal than requested
                 * last key (ie: only if next could have any useful data)
                 */
                if (currentNext != null && (lastKey == null || currentNext.key.compareMinKey(lastKey) <= 0)) {
                    return currentNext;
                }

            } finally {
                lock.unlock();
            }

            return null;
        }

        /**
         * Return the newly generated block if any
         */
        private Block<KEY,VAL> split() {
            if (size < index.maxPageBlockSize) {
                throw new IllegalStateException("Split on a non overflowing block");
            }

            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "Split: FK " + key, new Object[]{key});
            }
            NavigableMap<KEY, List<VAL>> keepValues = new TreeMap<>();
            NavigableMap<KEY, List<VAL>> otherValues = new TreeMap<>();

            final long splitSize = size / 2;

            long mySize = 0;
            long otherSize = 0;
            for (Map.Entry<KEY, List<VAL>> entry : values.entrySet()) {
                final KEY key = entry.getKey();
                for (VAL v : entry.getValue()) {
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

            KEY newOtherMinKey = otherValues.firstKey();

            Block<KEY,VAL> newblock = new Block<>(index, newOtherMinKey, otherValues, otherSize, next);

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

        private BlockRangeIndexMetadata.BlockMetadata<KEY> checkpoint() throws IOException {
            lock.lock();
            try {
                if (!dirty || !loaded) {
                    final Long nextBlockId = next == null ? null : next.key.blockId;
                    return new BlockRangeIndexMetadata.BlockMetadata<>(key.minKey, key.blockId, size, pageId, nextBlockId);
                }
                List<Map.Entry<KEY,VAL>> result = new ArrayList<>();
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
            } finally {
                lock.unlock();
            }

        }

        boolean unload() throws IOException {
            lock.lock();
            try {
                if (!loaded) {
                    return false;
                }
                if (dirty) {
                    checkpoint();
                }
                values = null;
                loaded = false;

                return true;
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

    public void put(K key, V value) {
        BlockStartKey<K> lookUp = BlockStartKey.valueOf(key, Long.MAX_VALUE);
        while (!tryPut(key, value, lookUp)) {
        }
    }

    public BlockRangeIndexMetadata<K> checkpoint() throws IOException {
        final boolean fineEnabled = LOG.isLoggable(Level.FINE);

        List<BlockRangeIndexMetadata.BlockMetadata<K>> blocksMetadata = new ArrayList<>();

        /* Reverse ordering! */
        Iterator<Block<K,V>> iterator = blocks.descendingMap().values().iterator();
        Block<K,V> nextReference = null;
        boolean deletionStream = false;

        while(iterator.hasNext()) {
            Block<K,V> block = iterator.next();

            BlockRangeIndexMetadata.BlockMetadata<K> metadata = block.checkpoint();
            /* Do not discard head block */
            if (metadata.size != 0 || block.key == BlockStartKey.HEAD_KEY) {

                /*
                 * If this is the first non empty block after a deletion "stream" end the stream
                 * and change next block to real not empty next block
                 */
                if (deletionStream) {
                    deletionStream = false;

                    if (fineEnabled) {
                        if (block.next != null) {
                            LOG.fine("unlinking block " + block.pageId + " (" + block.key + ") from deleted block "
                                    + block.next.pageId + " (" + block.next.key + ")");
                        }
                        if (nextReference != null) {
                            LOG.fine("linking block " + block.pageId + " (" + block.key + ") to real not empty next block "
                                    + nextReference.next.pageId + " (" + nextReference.next.key + ")");
                        }
                    }

                    /*
                     * Changes next reference, done with lock to force next publishing.
                     *
                     * TODO: it could be done better.
                     */
                    block.lock.lock();
                    try {
                        block.next = nextReference;
                        metadata.nextBlockId = nextReference == null ? null : nextReference.key.blockId;
                    } finally {
                        block.lock.unlock();
                    }

                }

                if (fineEnabled) {
                    LOG.fine("block " + block.pageId + " ("+ block.key+ ") has " + metadata.size + " records at checkpoint");
                }

                blocksMetadata.add(metadata);
            } else {
                LOG.info("block " + block.pageId + " ("+ block.key+ ") is empty at checkpoint: discarding");

                /* Unload the block from memory */
                block.unload();

                /* Remove the block from knowledge */
                iterator.remove();

                /*
                 * If not already in a deletion stream save the next not empty block reference,
                 * it will be needed at the end of block deletion stream. Otherwise current
                 * block next block is already deleted too, we must not save his reference!
                 */
                if (!deletionStream) {
                    /* Save deleted next reference */
                    nextReference = block.next;
                }

                deletionStream = true;
            }
        }

        return new BlockRangeIndexMetadata<>(blocksMetadata);
    }


    private boolean tryPut(K key, V value, BlockStartKey<K> lookUp) {
        Map.Entry<BlockStartKey<K>, Block<K,V>> segmentEntry = blocks.floorEntry(lookUp);
        if (segmentEntry == null) {

            final Block<K,V> headBlock = new Block<>(this, key, value);
            headBlock.lock.lock();

            try {
                boolean added = blocks.putIfAbsent((BlockStartKey<K>) BlockStartKey.HEAD_KEY, headBlock) == null;

                /* Set the block as "loaded" only if has been really added */
                if (added) {
                    final Metadata unload = pageReplacementPolicy.add(headBlock.page);
                    if (unload != null) {
                        unload.owner.unload(unload.pageId);
                    }
                }

                return added;
            } finally {
                headBlock.lock.unlock();
            }
        }


        Block<K,V> block = segmentEntry.getValue();

        do {
            block = block.addValue(key, value);
        } while(block != null);

        return true;

    }

    public void delete(K key, V value) {
        final DeleteState<K,V> state = new DeleteState<>();
        for(Block<K,V> block : findCandidates(key,key)) {
            state.next = block;
            do {
                state.next.delete(key, value, state);
                if (state.deleted) {
                    /* Deleted record no more iterations needed */
                    return;
                }
            } while (state.next != null);
        }
    }

    public Stream<V> query(K firstKey, K lastKey) {
        Set<BlockStartKey<K>> visitedBlocks = new HashSet<>();
        List<List<V>> found = new ArrayList<>();
        for(Block<K,V> block : findCandidates(firstKey, lastKey)) {
            Block<K,V> current = block;
            do {
                current = current.lookUpRange(firstKey, lastKey, visitedBlocks, found);
            } while (current != null);
        }
        return found.stream().flatMap(List::stream);
    }

    public List<V> lookUpRange(K firstKey, K lastKey) {
        return query(firstKey, lastKey).collect(Collectors.toList());
    }

    private Collection<Block<K,V>> findCandidates(K firstKey, K lastKey) {

        if (firstKey == null && lastKey == null) {
            throw new IllegalArgumentException();
        }

        BlockStartKey<K> floor = null;
        BlockStartKey<K> ceil = null;

        if (firstKey!= null) {
            floor = blocks.floorKey(BlockStartKey.valueOf(firstKey, -1L));
        }

        if (lastKey != null) {
            ceil = blocks.ceilingKey(BlockStartKey.valueOf(lastKey, Long.MAX_VALUE));
        }

        if (floor == null) {

            /* A LT or EQ key of first key doesn't exists (ex: the first key is the key after the first key) */
            if (ceil == null) {

                /* A GT or EQ key of last key doesn't exists (ex: the last key is the key before the last key) */

                /* Full scan */
                return blocks.values();

            } else {

                boolean ceilInclusive = ceil.compareMinKey(lastKey) == 0;

                return blocks.headMap(ceil, ceilInclusive).values();

            }

        } else {

            if (ceil == null) {

                /* A GT or EQ key of last key doesn't exists (ex: the last key is the key before the last key) */
                return blocks.tailMap(floor, true).values();

            } else {

                boolean ceilInclusive = ceil.compareMinKey(lastKey) == 0;

                return blocks.subMap(floor, true, ceil, ceilInclusive).values();

            }

        }

    }

    public List<V> search(K key) {
        return lookUpRange(key, key);
    }

    public boolean containsKey(K key) {
        return !lookUpRange(key, key).isEmpty();
    }

    public void boot(BlockRangeIndexMetadata<K> metadata) throws DataStorageManagerException {
        LOG.severe("boot index, with " + metadata.getBlocksMetadata().size() + " blocks");

        clear();

        /* Metadata are saved/recovered in reverse order so "next" block has been already created */
        Block<K,V> next = null;
        for (BlockRangeIndexMetadata.BlockMetadata<K> blockData : metadata.getBlocksMetadata()) {
            /*
             * TODO: if the system is restart with a different (smaller) page size old blocks will remain
             * bigger until a split occurs.
             */

            /* Medatada safety check (do not trust blindly ordering) */
            if (blockData.nextBlockId != null) {
                if (next == null || next.key.blockId != blockData.nextBlockId.longValue()) {
                    throw new DataStorageManagerException("Wron next block, expected " + next.key.blockId + " but "
                            + blockData.nextBlockId + " found");
                }
            } else {
                if (next != null) {
                    throw new DataStorageManagerException("Wron next block, expected notingh but "
                            + blockData.nextBlockId + " found");
                }
            }

            next = new Block<>(this, blockData.blockId, blockData.firstKey, blockData.size, blockData.pageId, next);
            blocks.put(next.key, next);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("boot block at " + next.key + " " + next.key.minKey);
            }
            currentBlockId.accumulateAndGet(next.key.blockId, EnsureLongIncrementAccumulator.INSTANCE);
        }
    }

    void clear() {

        for (Block<K,V> block : blocks.values()) {
            /* Unload if loaded */
            if (block.forcedUnload()) {
                pageReplacementPolicy.remove(block.page);
            }
        }

        blocks.clear();
    }

    ConcurrentNavigableMap<BlockStartKey<K>, Block<K,V>> getBlocks() {
        return blocks;
    }

}
