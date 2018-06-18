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

import static org.junit.Assert.assertEquals;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;

import org.junit.Assert;
import org.junit.Test;

import herddb.core.PageReplacementPolicy;
import herddb.core.RandomPageReplacementPolicy;
import herddb.index.brin.BlockRangeIndex.Block;
import herddb.index.brin.BlockRangeIndex.BlockStartKey;
import herddb.utils.Sized;

/**
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexStorageTest {

    @Test
    public void testSimpleReload() throws Exception {

        PageReplacementPolicy policy = new RandomPageReplacementPolicy(10);
        IndexDataStorage<Sized<Integer>, Sized<String>> storage = new MemoryIndexDataStorage<>();

        BlockRangeIndex<Sized<Integer>, Sized<String>> index = new BlockRangeIndex<>(400, policy, storage);

        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));

        BlockRangeIndexMetadata<Sized<Integer>> metadata = index.checkpoint();
        assertEquals(index.getNumBlocks(), metadata.getBlocksMetadata().size());

        BlockRangeIndex<Sized<Integer>, Sized<String>> indexAfterBoot =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(10), storage);

        indexAfterBoot.boot(metadata);
        assertEquals(Sized.valueOf("a"), index.search(Sized.valueOf(1)).get(0));
        assertEquals(Sized.valueOf("b"), index.search(Sized.valueOf(2)).get(0));
        assertEquals(Sized.valueOf("c"), index.search(Sized.valueOf(3)).get(0));
        assertEquals(2, indexAfterBoot.getNumBlocks());

    }

    @Test
    public void testReloadAfterBlockDeletion() throws Exception {

        PageReplacementPolicy policy = new RandomPageReplacementPolicy(10);
        IndexDataStorage<Sized<Integer>, Sized<Integer>> storage = new MemoryIndexDataStorage<>();

        BlockRangeIndex<Sized<Integer>, Sized<Integer>> index = new BlockRangeIndex<>(400, policy, storage);

        int i = 0;
        do {
            Sized<Integer> si = Sized.valueOf(i++);
            index.put(si, si);
        } while(index.getNumBlocks() < 4);

        /* First checkpoint after insertion */
        index.checkpoint();


        /* Now we empty middle blocks */

        /* Map without first key */
        ConcurrentNavigableMap<BlockStartKey<Sized<Integer>>, Block<Sized<Integer>, Sized<Integer>>> sub =
                index.getBlocks().tailMap(index.getBlocks().firstKey(), false);

        /* Second block */
        Block<Sized<Integer>, Sized<Integer>> second = sub.firstEntry().getValue();

        /* Map without second key */
        sub = sub.tailMap(sub.firstKey(), false);

        /* Third block */
        Block<Sized<Integer>, Sized<Integer>> third = sub.firstEntry().getValue();

        /* Copy to avoid concurrent modification */
        List<Entry<Sized<Integer>, Sized<Integer>>> toDelete = new ArrayList<>();

        second.values.forEach((k,vl) -> vl.forEach(v -> toDelete.add(new SimpleEntry<>(k,v))));
        third.values.forEach((k,vl) -> vl.forEach(v -> toDelete.add(new SimpleEntry<>(k,v))));

        /* Delete blocks 2 and 3 */
        toDelete.forEach(e -> index.delete(e.getKey(), e.getValue()));

        /* Checkpoint, should remove a block */
        BlockRangeIndexMetadata<Sized<Integer>> metadata = index.checkpoint();

        assertEquals(2, index.getNumBlocks());
        assertEquals(index.getNumBlocks(), metadata.getBlocksMetadata().size());

        /* Load a new index from data */
        BlockRangeIndex<Sized<Integer>, Sized<Integer>> indexAfterBoot =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(10), storage);

        indexAfterBoot.boot(metadata);

        /* Check data equality between new and old index */
        index.getBlocks().forEach((f,b) -> b.values.forEach((k,vl) -> {
            List<Sized<Integer>> search = indexAfterBoot.search(k);
            Assert.assertEquals(vl, search);
        }));

    }

    @Test
    public void testUnload() throws Exception {

        PageReplacementPolicy policy = new RandomPageReplacementPolicy(10);
        IndexDataStorage<Sized<Integer>, Sized<String>> storage = new MemoryIndexDataStorage<>();

        BlockRangeIndex<Sized<Integer>, Sized<String>> index = new BlockRangeIndex<>(1024, policy, storage);
        for (int i = 0; i < 100; i++) {
            index.put(Sized.valueOf(i), Sized.valueOf("a"));
        }
        BlockRangeIndexMetadata<Sized<Integer>> metadata = index.checkpoint();
        assertEquals(index.getNumBlocks(), metadata.getBlocksMetadata().size());

        for (int i = 0; i < 100; i++) {
            assertEquals(Sized.valueOf("a"), index.search(Sized.valueOf(i)).get(0));
        }

        assertEquals(10,policy.size());

        index.clear();

        /* No pages should remain in memory after unload!! */
        assertEquals(0,policy.size());

    }
}
