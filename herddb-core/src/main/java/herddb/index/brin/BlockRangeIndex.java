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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import herddb.core.Page;
import herddb.core.Page.Metadata;
import herddb.core.PageReplacementPolicy;
import herddb.utils.EnsureIntegerIncrementAccumulator;
import herddb.utils.SizeAwareObject;

/**
 * Very Simple BRIN (Block Range Index) implementation with pagination managed by a {@link PageReplacementPolicy}
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class BlockRangeIndex<K extends Comparable<K> & SizeAwareObject, V extends SizeAwareObject> {

    private static final Logger LOG = Logger.getLogger(BlockRangeIndex.class.getName());

    private static final long ENTRY_CONSTANT_BYTE_SIZE = 93;
    private static final long BLOCK_CONSTANT_BYTE_SIZE = 128;

    private final long maxPageBlockSize;
    private final ConcurrentNavigableMap<BlockStartKey<K>, Block> blocks = new ConcurrentSkipListMap<>();
    private final AtomicInteger blockIdGenerator = new AtomicInteger();

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

        static final BlockStartKey<?> HEAD_KEY = new BlockStartKey<>(null, -1);

        public final K minKey;
        public final int blockId;

        @Override
        public String toString() {
            if (minKey == null) {
                return "BlockStartKey{HEAD}";
            } else {
                return "BlockStartKey{" + minKey + "," + blockId + '}';
            }
        }

        public static final <X extends Comparable<X>> BlockStartKey<X> valueOf(X minKey, int segmentId) {
            if (minKey == null) {
                if (segmentId != -1) {
                    throw new IllegalArgumentException();
                }
                return (BlockStartKey<X>) HEAD_KEY;
            }
            return new BlockStartKey<>(minKey, segmentId);
        }

        private BlockStartKey(K minKey, int segmentId) {
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
            return Integer.compare(blockId, o.blockId);
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
            hash = 67 * hash + this.blockId;
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

    private final class BRINPage extends Page<Block> {

        public BRINPage(Block owner, long blockId) {
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

    class Block implements Page.Owner {

        final BlockStartKey<K> key;
        NavigableMap<K, List<V>> values;
        long size;
        Block next;
        private final ReentrantLock lock = new ReentrantLock(true);
        private volatile boolean loaded;
        private volatile boolean dirty;
        private volatile long pageId;

        private final BRINPage page;

        public Block(int blockId, K firstKey, long size, long pageId) {
            this.key = BlockStartKey.valueOf(firstKey, blockId);
            this.size = size;
            this.loaded = false;
            this.dirty = false;
            this.pageId = pageId;

            /* Immutable Block ID */
            page = new BRINPage(this, blockId);
        }

        public Block(K firstKey, V firstValue) {
            this.key = (BlockStartKey<K>) BlockStartKey.HEAD_KEY;
            /* TODO: estimate a better sizing for array list */
            List<V> firstKeyValues = new ArrayList<>();
            firstKeyValues.add(firstValue);
            values = new TreeMap<>();
            values.put(firstKey, firstKeyValues);
            this.size = evaluateEntrySize(firstKey, firstValue);
            this.loaded = true;
            this.dirty = true;
            this.pageId = IndexDataStorage.NEW_PAGE;

            /* Immutable Block ID */
            page = new BRINPage(this, key.blockId);
        }

        private Block(K newOtherMinKey, NavigableMap<K, List<V>> other_values, long size) {
            this.key = BlockStartKey.valueOf(newOtherMinKey, blockIdGenerator.incrementAndGet());
            this.values = other_values;
            this.size = size;
            this.loaded = true;
            this.dirty = true;
            this.pageId = IndexDataStorage.NEW_PAGE;

            /* Immutable Block ID */
            page = new BRINPage(this, key.blockId);
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

        private void mergeAddValue(K key1, V value, Map<K, List<V>> values) {
            List<V> valuesForKey = values.get(key1);
            if (valuesForKey == null) {
                valuesForKey = new ArrayList<>();
                values.put(key1, valuesForKey);
            }
            valuesForKey.add(value);
        }

        boolean addValue(K key, V value, ConcurrentNavigableMap<BlockStartKey<K>, Block> blocks) {

            /* Eventual new block from split. It must added to PageReplacementPolicy only after lock release */
            Block newblock = null;
            lock.lock();
            try {
                Block _next = next;
                if (_next != null && _next.key.compareMinKey(key) <= 0) {
                    // unfortunately this occours during split
                    // put #1 -> causes split
                    // put #2 -> designates this block for put, but the split is taking place
                    // put #1 returns
                    // put #2 needs to addValue to the 'next' (split result) block not to this
                    return false;
                }
                ensureBlockLoaded();

                mergeAddValue(key, value, values);
                size += evaluateEntrySize(key, value);
                dirty = true;

                if (size > maxPageBlockSize) {
                    newblock = split(blocks);
                }
            } finally {
                lock.unlock();
            }

            if (newblock != null) {
                final Metadata unload = pageReplacementPolicy.add(newblock.page);
                if (unload != null) {
                    unload.owner.unload(unload.pageId);
                }
            }
            return true;
        }

        Block delete(K key, V value, Set<BlockStartKey<K>> visitedBlocks) {

            if (!visitedBlocks.add(this.key)) {
                return null;
            }

            lock.lock();

            try {

                final Block currentNext = this.next;

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
                    List<V> valuesForKey = values.get(key);
                    if (valuesForKey != null) {
                        boolean removed = valuesForKey.remove(value);
                        if (removed) {
                            if (valuesForKey.isEmpty()) {
                                values.remove(key);
                            }
                            size -= evaluateEntrySize(key, value);
                            dirty = true;
                        }
                    }
                }


                /*
                 * Propagate to next only if it exist AND next min key isn't greater than requested
                 * key (ie: only if next could have any useful data)
                 */
                if (currentNext != null && nextMinKeyCompare <= 0) {
                    return currentNext;
                }

            } finally {
                lock.unlock();
            }

            return null;
        }

       void ensureBlockLoaded() {
            if (!loaded) {
                try {
                    values = new TreeMap<>();
                    List<Map.Entry<K, V>> loadDataPage = dataStorage.loadDataPage(pageId);

                    for (Map.Entry<K, V> entry : loadDataPage) {
                        mergeAddValue(entry.getKey(), entry.getValue(), values);
                    }

                    loaded = true;

                    /* Dereferenced page unload */
                    final Page.Metadata unload = pageReplacementPolicy.add(page);
                    if (unload != null) {
                        unload.owner.unload(unload.pageId);
                    }

                } catch (IOException err) {
                    throw new RuntimeException(err);
                }
            } else {
                pageReplacementPolicy.pageHit(page);
            }
        }

        Block lookUpRange(K firstKey, K lastKey, Set<BlockStartKey<K>> visitedBlocks, List<List<V>> results) {
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

                final Block currentNext = this.next;

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
                            List<V> seek = values.get(firstKey);
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
        private Block split(ConcurrentNavigableMap<BlockStartKey<K>, Block> blocks) {
            if (size < maxPageBlockSize) {
                throw new IllegalStateException("Split on a non overflowing block");
            }

            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "Split: FK " + key, new Object[]{key});
            }
            NavigableMap<K, List<V>> keep_values = new TreeMap<>();
            NavigableMap<K, List<V>> other_values = new TreeMap<>();

            final long splitSize = size / 2;

            long mySize = 0;
            long otherSize = 0;
            for (Map.Entry<K, List<V>> entry : values.entrySet()) {
                final K key = entry.getKey();
                for (V v : entry.getValue()) {
                    final long entrySize = evaluateEntrySize(key, v);
                    if (mySize < splitSize) {
                        mergeAddValue(key, v, keep_values);
                        mySize += entrySize;
                    } else {
                        mergeAddValue(key, v, other_values);
                        otherSize += entrySize;
                    }
                }
            }

            if (other_values.isEmpty()) {
                return null;
            }

            K newOtherMinKey = other_values.firstKey();

            Block newblock = new Block(newOtherMinKey, other_values, otherSize);

            this.next = newblock;
            this.size = mySize;
            this.values = keep_values;

            /*
             * First publish the new block then reduce this block min/max keys. If done otherwise a
             * concurrent lookup thread could miss the block containing the right data.
             *
             * blocks.put acts as memory barrier (contains at least a volatile access thus reordering is
             * blocked [happen before])
             */
            // access to external field, this is the cause of most of the concurrency problems
            blocks.put(newblock.key, newblock);

            return newblock;

        }

        private BlockRangeIndexMetadata.BlockMetadata<K> checkpoint() throws IOException {
            lock.lock();
            try {
                if (!dirty || !loaded) {
                    return new BlockRangeIndexMetadata.BlockMetadata<>(key.minKey, key.blockId, size, pageId);
                }
                List<Map.Entry<K, V>> result = new ArrayList<>();
                values.forEach((k, l) -> {
                    l.forEach(v -> {
                        result.add(new AbstractMap.SimpleImmutableEntry<>(k, v));
                    });
                });

                long newPageId = dataStorage.createDataPage(result);
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("checkpoint block " + key + ": newpage -> " + newPageId + " with " + values.size() + " entries x " + result.size() + " pointers");
                }
                this.dirty = false;
                this.pageId = newPageId;

                return new BlockRangeIndexMetadata.BlockMetadata<>(key.minKey, key.blockId, size, pageId);
            } finally {
                lock.unlock();
            }

        }

        private boolean unload() throws IOException {
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
            return "Block{" + "key=" + key + ", minKey=" + key.minKey + ", size=" + size;
        }

    }

    public int getNumBlocks() {
        return blocks.size();
    }

    public void put(K key, V value) {
        BlockStartKey<K> lookUp = BlockStartKey.valueOf(key, Integer.MAX_VALUE);
        while (!tryPut(key, value, lookUp)) {
        }
    }

    public BlockRangeIndexMetadata<K> checkpoint() throws IOException {
        final boolean fineEnabled = LOG.isLoggable(Level.FINE);

        List<BlockRangeIndexMetadata.BlockMetadata<K>> blocksMetadata = new ArrayList<>();
        Set<Block> deleted = new HashSet<>();

        Iterator<Block> iterator = blocks.values().iterator();
        while(iterator.hasNext()) {
            Block block = iterator.next();

            BlockRangeIndexMetadata.BlockMetadata<K> metadata = block.checkpoint();
            if (metadata.size != 0) {
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

                deleted.add(block);
            }
        }

        if (!deleted.isEmpty()) {
            /* Loop again to remove next pointers to delete blocks */
            for (Block block : blocks.values()) {
                final Block next = block.next;
                if (deleted.contains(next)) {
                    if (fineEnabled) {
                        LOG.warning("unlinking block " + block.pageId + " ("+ block.key+ ") from deleted block " + next.pageId + " ("+ block.key+ ")");
                    }
                    block.next = null;
                }
            }
        }

        return new BlockRangeIndexMetadata<>(blocksMetadata);
    }

    private boolean tryPut(K key, V value, BlockStartKey<K> lookUp) {
        Map.Entry<BlockStartKey<K>, Block> segmentEntry = blocks.floorEntry(lookUp);
        if (segmentEntry == null) {

            final Block headBlock = new Block(key, value);
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
        Block choosenSegment = segmentEntry.getValue();
        return choosenSegment.addValue(key, value, blocks);

    }

    public void delete(K key, V value) {
        Set<BlockStartKey<K>> visitedBlocks = new HashSet<>();
        for(Block block : findCandidates(key,key)) {
            Block current = block;
            do {
                current = current.delete(key, value, visitedBlocks);
            } while (current != null);
        }
    }

    public Stream<V> query(K firstKey, K lastKey) {
        Set<BlockStartKey<K>> visitedBlocks = new HashSet<>();
        List<List<V>> found = new ArrayList<>();
        for(Block block : findCandidates(firstKey, lastKey)) {
            Block current = block;
            do {
                current = current.lookUpRange(firstKey, lastKey, visitedBlocks, found);
            } while (current != null);
        }
        return found.stream().flatMap(List::stream);
    }

    public List<V> lookUpRange(K firstKey, K lastKey) {
        return query(firstKey, lastKey).collect(Collectors.toList());
    }

    private Collection<Block> findCandidates(K firstKey, K lastKey) {

        if (firstKey == null && lastKey == null) {
            throw new IllegalArgumentException();
        }

        BlockStartKey<K> floor = null;
        BlockStartKey<K> ceil = null;

        if (firstKey!= null) {
            floor = blocks.floorKey(BlockStartKey.valueOf(firstKey, -1));
        }

        if (lastKey != null) {
            ceil = blocks.ceilingKey(BlockStartKey.valueOf(lastKey, Integer.MAX_VALUE));
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

    public void boot(BlockRangeIndexMetadata<K> metadata) {
        LOG.severe("boot index, with " + metadata.getBlocksMetadata().size() + " blocks");

        clear();

        for (BlockRangeIndexMetadata.BlockMetadata<K> blockData : metadata.getBlocksMetadata()) {
            /*
             * TODO: if the system is restart with a different (smaller) page size old blocks will remain
             * bigger until a split occurs.
             */
            Block block = new Block(blockData.blockId, blockData.firstKey, blockData.size, blockData.pageId);
            blocks.put(block.key, block);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("boot block at " + block.key + " " + block.key.minKey);
            }
            blockIdGenerator.accumulateAndGet(block.key.blockId, EnsureIntegerIncrementAccumulator.INSTANCE);
        }
    }

    void clear() {

        for (Block block : blocks.values()) {
            /* Unload if loaded */
            if (block.forcedUnload()) {
                pageReplacementPolicy.remove(block.page);
            }
        }

        blocks.clear();
    }

    ConcurrentNavigableMap<BlockStartKey<K>, Block> getBlocks() {
        return blocks;
    }

}
