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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Very Simple BRIN (Block Range Index) implementation
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndex<K extends Comparable<K>, V> {

    void clear() {
        blocks.clear();
    }

    public void dump() {
        for (Block<?, ?> b : blocks.values()) {
            System.out.println("BLOCK " + b);
        }
    }

    private static final Logger LOG = Logger.getLogger(BlockRangeIndex.class.getName());

    private final int maxBlockSize;
    private final ConcurrentNavigableMap<BlockStartKey<K>, Block<K, V>> blocks = new ConcurrentSkipListMap<>();
    private final AtomicInteger blockIdGenerator = new AtomicInteger();

    private final BlockStartKey HEAD_KEY = new BlockStartKey(null, -1);

    private final class BlockStartKey<K extends Comparable<K>> implements Comparable<BlockStartKey<K>> {

        public final K minKey;
        public final int blockId;

        @Override
        public String toString() {
            if (minKey == null) {
                return "BlockStartKey{HEAD} " + Integer.toHexString(System.identityHashCode(this));
            } else {
                return "BlockStartKey{" + "minKey=" + minKey + ", segmentId=" + blockId + '}' + Integer.toHexString(System.identityHashCode(this));
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
            hash = 73 * hash + (int) (this.blockId ^ (this.blockId >>> 32));
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

    private class Block<SK extends Comparable<SK>, SV> {

        BlockStartKey<SK> key;
        SK minKey;
        SK maxKey;
        NavigableMap<SK, List<SV>> values;
        int size;
        private final ReentrantLock lock = new ReentrantLock(true);

        public Block(BlockStartKey<SK> key, SK firstKey, SV firstValue) {
            this.key = key;
            this.minKey = firstKey;
            List<SV> firstKeyValues = new ArrayList<>(maxBlockSize);
            firstKeyValues.add(firstValue);
            values = new TreeMap<>();
            values.put(firstKey, firstKeyValues);
            this.maxKey = firstKey;
            this.size = 1;
        }

        private Block(SK newOtherMinKey, SK newOtherMaxKey, ConcurrentSkipListMap<SK, List<SV>> other_values, int size) {
            this.key = new BlockStartKey<>(newOtherMinKey, blockIdGenerator.incrementAndGet());
            this.minKey = newOtherMinKey;
            this.values = other_values;
            this.maxKey = newOtherMaxKey;
            this.size = size;
        }

        @Override
        public String toString() {
            lock.lock();
            try {
                return "Block{" + "key=" + key + ", minKey=" + minKey + ", maxKey=" + maxKey + ", size=" + size + ", values=" + values + '}';
            } finally {
                lock.unlock();
            }
        }

        private void mergeAddValue(SK key1, SV value, Map<SK, List<SV>> values) {
            List<SV> valuesForKey = values.get(key1);
            if (valuesForKey == null) {
                valuesForKey = new ArrayList<>();
                values.put(key1, valuesForKey);
            }
            valuesForKey.add(value);
        }

        void addValue(SK key, SV value, ConcurrentNavigableMap<BlockStartKey<SK>, Block<SK, SV>> blocks) {
            lock.lock();
            try {
                mergeAddValue(key, value, values);
                size++;
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
        }

        synchronized boolean delete(SK key, SV value) {
            lock.lock();
            try {
                List<SV> valuesForKey = values.get(key);
                if (valuesForKey != null) {
                    valuesForKey.remove(value);
                    if (valuesForKey.isEmpty()) {
                        values.remove(key);
                        if (values.isEmpty()) {
                            size--;
                            return true;
                        }
                    }
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        List<SV> lookUpRange(SK firstKey, SK lastKey) {
            lock.lock();
            try {
                List<SV> result = new ArrayList<>();
                if (firstKey != null && lastKey != null) {
                    // index seek case
                    if (firstKey.equals(lastKey)) {
                        List<SV> seek = values.get(firstKey);
                        if (seek == null) {
                            return null;
                        } else {
                            return new ArrayList<>(seek);
                        }
                    }
                    values.subMap(firstKey, true, lastKey, true).forEach((k, seg) -> {
                        result.addAll(seg);
                    });
                } else if (firstKey != null) {
                    values.tailMap(firstKey, true).forEach((k, seg) -> {
                        result.addAll(seg);
                    });
                } else {
                    values.headMap(lastKey, true).forEach((k, seg) -> {
                        result.addAll(seg);
                    });
                }
                return result;
            } finally {
                lock.unlock();
            }

        }

        private void split(ConcurrentNavigableMap<BlockStartKey<SK>, Block<SK, SV>> blocks) {
            if (size < maxBlockSize) {
                throw new IllegalStateException();
            }
            ConcurrentSkipListMap<SK, List<SV>> keep_values = new ConcurrentSkipListMap<>();
            ConcurrentSkipListMap<SK, List<SV>> other_values = new ConcurrentSkipListMap<>();
            final int splitmid = (size / 2) - 1;
            int count = 0;
            int otherSize = 0;
            int mySize = 0;
            for (Map.Entry<SK, List<SV>> entry : values.entrySet()) {
                SK key = entry.getKey();
                for (SV v : entry.getValue()) {
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
            this.values = keep_values;
            this.minKey = keep_values.firstKey();
            this.maxKey = keep_values.lastKey();
            this.size = mySize;
            SK newOtherMinKey = other_values.firstKey();
            SK newOtherMaxKey = other_values.lastKey();
            Block<SK, SV> newblock = new Block<>(newOtherMinKey, newOtherMaxKey, other_values, otherSize);
            blocks.put(newblock.key, newblock);
        }

    }

    public BlockRangeIndex(int maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
    }

    public int getNumSegments() {
        return blocks.size();
    }

    public void put(K key, V value) {
        BlockStartKey<K> lookUp = new BlockStartKey<>(key, Integer.MAX_VALUE);
        while (!tryPut(key, value, lookUp)) {
        }
    }

    private boolean tryPut(K key, V value, BlockStartKey<K> lookUp) {
        Map.Entry<BlockStartKey<K>, Block<K, V>> segmentEntry = blocks.floorEntry(lookUp);
        if (segmentEntry == null) {
            Block<K, V> headBlock = new Block<>(HEAD_KEY, key, value);
            return blocks.putIfAbsent(HEAD_KEY, headBlock) == null;
        }
        Block<K, V> choosenSegment = segmentEntry.getValue();
        choosenSegment.addValue(key, value, blocks);
        return true;
    }

    public void delete(K key, V value) {
        for (Iterator<Block<K, V>> it = blocks.values().iterator(); it.hasNext();) {
            Block<K, V> s = it.next();
            boolean empty = s.delete(key, value);
            if (empty) {
                it.remove();
            }
        }
    }

    public Stream<V> query(K firstKey, K lastKey) {
        List<Block> candidates = findCandidates(firstKey, lastKey);
        return candidates.stream().flatMap(s -> {
            List<V> lookupInBlock = s.lookUpRange(firstKey, lastKey);
            if (lookupInBlock == null || lookupInBlock.isEmpty()) {
                return Stream.empty();
            } else {
                return lookupInBlock.stream();
            }
        });
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

}
