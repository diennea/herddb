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
import java.util.HashSet;
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
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import herddb.core.Page;
import herddb.core.PageReplacementPolicy;
import herddb.utils.EnsureIntegerIncrementAccumulator;

/**
 * Very Simple BRIN (Block Range Index) implementation with pagination managed by a {@link PageReplacementPolicy}
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class PagedBlockRangeIndex<K extends Comparable<K>, V> {

    private static final Logger LOG = Logger.getLogger(PagedBlockRangeIndex.class.getName());

    private final int maxBlockSize;
    private final ConcurrentNavigableMap<BlockStartKey<K>, Block> blocks = new ConcurrentSkipListMap<>();
    private final AtomicInteger blockIdGenerator = new AtomicInteger();

    private final BlockStartKey<K> HEAD_KEY = new BlockStartKey<>(null, -1);
    private final IndexDataStorage<K,V> dataStorage;

    private final PageReplacementPolicy pageReplacementPolicy;

    public PagedBlockRangeIndex(int maxBlockSize, PageReplacementPolicy pageReplacementPolicy) {
        this(maxBlockSize, pageReplacementPolicy, new MemoryIndexDataStorage<>());
    }

    public PagedBlockRangeIndex(int maxBlockSize, PageReplacementPolicy pageReplacementPolicy, IndexDataStorage<K,V> dataStorage) {
        this.maxBlockSize = maxBlockSize;
        this.pageReplacementPolicy = pageReplacementPolicy;
        this.dataStorage = dataStorage;
    }

    private final class BlockStartKey<K extends Comparable<K>> implements Comparable<BlockStartKey<K>> {

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

        public BlockStartKey(K minKey, int segmentId) {
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

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 73 * hash + Objects.hashCode(this.minKey);
            hash = 73 * hash + (this.blockId ^ (this.blockId >>> 32));
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
    }

    class Block implements Page.Owner {

        final BlockStartKey<K> key;
        K minKey;
        K maxKey;
        NavigableMap<K, List<V>> values;
        int size;
        Block next;
        private final ReentrantLock lock = new ReentrantLock(true);
        private volatile boolean loaded;
        private volatile boolean dirty;
        private volatile long pageId;

        private final Page<Block> page;

        public Block(int blockId, K firstKey, K lastKey, int size, long pageId) {
            this.key = new BlockStartKey<>(firstKey, blockId);
            this.minKey = firstKey;
            this.maxKey = lastKey;
            this.size = size;
            this.loaded = false;
            this.dirty = false;
            this.pageId = pageId;

            /* Immutable Block ID not mutable pageID */
            page = new BRINPage(this, blockId);
        }

        public Block(K firstKey, V firstValue) {
            this.key = HEAD_KEY;
            this.minKey = firstKey;
            List<V> firstKeyValues = new ArrayList<>(maxBlockSize);
            firstKeyValues.add(firstValue);
            values = new TreeMap<>();
            values.put(firstKey, firstKeyValues);
            this.maxKey = firstKey;
            this.size = 1;
            this.loaded = true;
            this.dirty = true;
            this.pageId = IndexDataStorage.NEW_PAGE;

            /* Immutable Block ID not mutable pageID */
            page = new BRINPage(this, HEAD_KEY.blockId);

            /* Unpon "loading" add the block to knowledgebase */
            pageReplacementPolicy.add(page);
        }

        private Block(K newOtherMinKey, K newOtherMaxKey, NavigableMap<K, List<V>> other_values, int size) {
            this.key = new BlockStartKey<>(newOtherMinKey, blockIdGenerator.incrementAndGet());
            this.minKey = newOtherMinKey;
            this.values = other_values;
            this.maxKey = newOtherMaxKey;
            this.size = size;
            this.loaded = true;
            this.dirty = true;
            this.pageId = IndexDataStorage.NEW_PAGE;

            /* Immutable Block ID not mutable pageID */
            page = new BRINPage(this, key.blockId);

            /* Unpon "loading" add the block to knowledgebase */
            pageReplacementPolicy.add(page);
        }

        @Override
        public String toString() {
            return "Block{" + "key=" + key + ", minKey=" + minKey + ", maxKey=" + maxKey + ", size=" + size;
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
            lock.lock();
            try {
                Block _next = next;
                if (_next != null && _next.minKey.compareTo(key) <= 0) {
                    // unfortunately this occours during split
                    // put #1 -> causes split
                    // put #2 -> designates this block for put, but the split is taking place
                    // put #1 returns
                    // put #2 needs to addValue to the 'next' (split result) block not to this
                    return false;
                }
                ensureBlockLoaded();
                mergeAddValue(key, value, values);
                size++;
                dirty = true;
                if (maxKey.compareTo(key) < 0) {
                    maxKey = key;
                }
                if (minKey.compareTo(key) > 0 && this.key.blockId < 0) {
                    minKey = key;
                }
                if (size > maxBlockSize) {
                    split(blocks);
                }
            } finally {
                lock.unlock();
            }
            return true;
        }

        boolean delete(K key, V value, Set<BlockStartKey<K>> visitedBlocks) {
            visitedBlocks.add(this.key);
            lock.lock();
            try {
                ensureBlockLoaded();
                List<V> valuesForKey = values.get(key);
                if (valuesForKey != null) {
                    boolean removed = valuesForKey.remove(value);
                    if (removed) {
                        if (valuesForKey.isEmpty()) {
                            values.remove(key);
                        }
                        size--;
                    }
                }
                if (next != null && !visitedBlocks.contains(next.key)) {
                    next.delete(key, value, visitedBlocks);
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        private void ensureBlockLoaded() {
            if (!loaded) {
                try {
                    values = new TreeMap<>();
                    List<Map.Entry<K, V>> loadDataPage = dataStorage.loadDataPage(pageId);
                    for (Map.Entry<K,V> entry : loadDataPage) {
                        mergeAddValue(entry.getKey(), entry.getValue(), values);
                    }
                    loaded = true;

                    /* Unpon loading add the block to knowledgebase */
                    pageReplacementPolicy.add(page);
                } catch (IOException err) {
                    throw new RuntimeException(err);
                }
            } else {
                pageReplacementPolicy.pageHit(page);
            }
        }

        Stream<V> lookUpRange(K firstKey, K lastKey, Set<BlockStartKey<K>> visitedBlocks) {
            if (!visitedBlocks.add(this.key)) {
                return null;
            }
            List<V> result = new ArrayList<>();
            lock.lock();
            try {
                if (firstKey != null && lastKey != null) {

                    // index seek case
                    ensureBlockLoaded();
                    if (firstKey.equals(lastKey)) {
                        List<V> seek = values.get(firstKey);
                        if (seek != null) {
                            result.addAll(seek);
                        }
                    } else if (lastKey.compareTo(firstKey) < 0) {
                        // no value is possible
                    } else {
                        values.subMap(firstKey, true, lastKey, true).forEach((k, seg) -> {
                            result.addAll(seg);
                        });
                    }
                } else if (firstKey != null) {
                    ensureBlockLoaded();
                    values.tailMap(firstKey, true).forEach((k, seg) -> {
                        result.addAll(seg);
                    });
                } else {
                    ensureBlockLoaded();
                    values.headMap(lastKey, true).forEach((k, seg) -> {
                        result.addAll(seg);
                    });
                }

            } finally {
                lock.unlock();
            }
            Block _next = this.next;
            if (_next != null && !visitedBlocks.contains(_next.key)) {
                Stream<V> lookUpRangeOnNext = _next.lookUpRange(firstKey, lastKey, visitedBlocks);
                if (lookUpRangeOnNext != null && !result.isEmpty()) {
                    return Stream.concat(result.stream(), lookUpRangeOnNext);
                } else if (lookUpRangeOnNext != null) {
                    return lookUpRangeOnNext;
                } else if (!result.isEmpty()) {
                    return result.stream();
                } else {
                    return null;
                }
            } else if (!result.isEmpty()) {
                return result.stream();
            } else {
                return null;
            }
        }

        private void split(ConcurrentNavigableMap<BlockStartKey<K>, Block> blocks) {
            if (size < maxBlockSize) {
                throw new IllegalStateException();
            }

            NavigableMap<K, List<V>> keep_values = new TreeMap<>();
            NavigableMap<K, List<V>> other_values = new TreeMap<>();
            final int splitmid = (maxBlockSize / 2) - 1;
            int count = 0;
            int otherSize = 0;
            int mySize = 0;
            for (Map.Entry<K, List<V>> entry : values.entrySet()) {
                K key = entry.getKey();
                for (V v : entry.getValue()) {
                    if (count <= splitmid) {
                        mergeAddValue(key, v, keep_values);
                        mySize++;
                    } else {
                        mergeAddValue(key, v, other_values);
                        otherSize++;
                    }
                    count++;
                }
            }

            if (!other_values.isEmpty()) {
                K newOtherMinKey = other_values.firstKey();
                K newOtherMaxKey = other_values.lastKey();
                Block newblock = new Block(newOtherMinKey, newOtherMaxKey, other_values, otherSize);

                // access to external field, this is the cause of most of the concurrency problems
                blocks.put(newblock.key, newblock);

                K firstKey = keep_values.firstKey();
                K lastKey = keep_values.lastKey();
                this.next = newblock;
                this.minKey = firstKey;
                this.maxKey = lastKey;
                this.size = mySize;
                this.values = keep_values;
            }
        }

        private BlockRangeIndexMetadata.BlockMetadata<K> checkpoint() throws IOException {
            lock.lock();
            try {
                if (!dirty || !loaded) {
                    return new BlockRangeIndexMetadata.BlockMetadata<>(minKey, maxKey, key.blockId, size, pageId);
                }
                List<Map.Entry<K, V>> result = new ArrayList<>();
                values.forEach((k, l) -> {
                    l.forEach(v -> {
                        result.add(new AbstractMap.SimpleImmutableEntry<>(k, v));
                    });
                });
                long newPageId = dataStorage.createDataPage(result);
                LOG.severe("checkpoint block " + key + ": newpage -> " + newPageId + " with " + values.size() + " entries x " + result.size() + " pointers");
                this.dirty = false;
                this.pageId = newPageId;
                return new BlockRangeIndexMetadata.BlockMetadata<>(minKey, maxKey, key.blockId, size, pageId);
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

    }


    public int getNumBlocks() {
        return blocks.size();
    }

    public void put(K key, V value) {
        BlockStartKey<K> lookUp = new BlockStartKey<>(key, Integer.MAX_VALUE);
        while (!tryPut(key, value, lookUp)) {}
    }

    public BlockRangeIndexMetadata<K> checkpoint() throws IOException {
        List<BlockRangeIndexMetadata.BlockMetadata<K>> blocksMetadata = new ArrayList<>();
        for (Block block : blocks.values()) {
            BlockRangeIndexMetadata.BlockMetadata<K> metadata = block.checkpoint();
            if (metadata.size > 0) {
                LOG.severe("block " + block.key + " has " + metadata.size + " records at checkpoint");
                blocksMetadata.add(metadata);
            } else {
                LOG.severe("block " + block.key + " is empty at checkpoint. discarding");
            }
        }
        return new BlockRangeIndexMetadata<>(blocksMetadata);
    }

    public void unloadAllBlocks() throws IOException {
        for (Block block : blocks.values()) {
            block.unload();
        }
    }

    private boolean tryPut(K key, V value, BlockStartKey<K> lookUp) {
        Map.Entry<BlockStartKey<K>, Block> segmentEntry = blocks.floorEntry(lookUp);
        if (segmentEntry == null) {
            Block headBlock = new Block(key, value);
            boolean result = blocks.putIfAbsent(HEAD_KEY, headBlock) == null;
            return result;
        }
        Block choosenSegment = segmentEntry.getValue();
        return choosenSegment.addValue(key, value, blocks);

    }

    public void delete(K key, V value) {
        Set<BlockStartKey<K>> visitedBlocks = new HashSet<>();
        blocks.values().forEach(b -> b.delete(key, value, visitedBlocks));
    }

    public Stream<V> query(K firstKey, K lastKey) {

        List<Block> candidates = findCandidates(firstKey, lastKey);
        Set<BlockStartKey<K>> visitedBlocks = new HashSet<>();
        return candidates.stream().flatMap((s) -> s.lookUpRange(firstKey, lastKey, visitedBlocks));
    }

    public List<V> lookUpRange(K firstKey, K lastKey) {
        return query(firstKey, lastKey).collect(Collectors.toList());
    }

    private List<Block> findCandidates(K firstKey, K lastKey) {

        if (firstKey == null && lastKey == null) {
            throw new IllegalArgumentException();
        }

        if (firstKey != null && lastKey != null) {
            List<Block> candidates = new ArrayList<>();
            // TreeMap internal iteration is faster then using the Iterator, but we cannot exit from the loop
            blocks.forEach((k, s) -> {
                if (s.minKey.compareTo(lastKey) <= 0 && s.maxKey.compareTo(firstKey) >= 0) {
                    candidates.add(s);
                }
            });
            return candidates;
        } else if (firstKey != null) {
            List<Block> candidates = new ArrayList<>();
            blocks.forEach((k, s) -> {
                if (s.maxKey.compareTo(firstKey) >= 0) {
                    candidates.add(s);
                }
            });
            return candidates;
        } else {
            List<Block> candidates = new ArrayList<>();
            blocks.forEach((k, s) -> {
                if (s.minKey.compareTo(lastKey) <= 0) {
                    candidates.add(s);
                }
            });
            return candidates;
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
        this.blocks.clear();

        for (BlockRangeIndexMetadata.BlockMetadata<K> blockData : metadata.getBlocksMetadata()) {
            Block block = new Block(blockData.blockId, blockData.firstKey, blockData.lastKey, blockData.size, blockData.pageId);
            blocks.put(block.key, block);
            LOG.severe("boot block at " + block.key + " " + block.minKey + " - " + block.maxKey);
            blockIdGenerator.accumulateAndGet(block.key.blockId, EnsureIntegerIncrementAccumulator.INSTANCE);
        }
    }

    void clear() {
        blocks.clear();
    }

    ConcurrentNavigableMap<BlockStartKey<K>, Block> getBlocks() {
        return blocks;
    }

}
