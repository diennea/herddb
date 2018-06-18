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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import herddb.core.PageReplacementPolicy;
import herddb.core.RandomPageReplacementPolicy;
import herddb.utils.Sized;

/**
 * Unit tests for BlockRangeIndex
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexTest {

    @Test
    public void testSimpleSplit() {

        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(400, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        dumpIndex(index);
        assertEquals(Sized.valueOf("a"), index.search(Sized.valueOf(1)).get(0));
        assertEquals(Sized.valueOf("b"), index.search(Sized.valueOf(2)).get(0));
        assertEquals(Sized.valueOf("c"), index.search(Sized.valueOf(3)).get(0));
        assertEquals(2, index.getNumBlocks());
    }

    @Test
    public void testNotNullNextAfterSplit() {

        BlockRangeIndex<Sized<Integer>, Sized<Integer>> index =
                new BlockRangeIndex<>(400, new RandomPageReplacementPolicy(10));

        int i = 0;
        do {
            /*
             * Add elements at index head to generate 3 blocks and beeing sure that just the
             * last block has a null next and not because we split only the last block with
             * null next. Inserting top elements we split head block every time and such
             * block should have null next just when is the only one block.
             */
            Sized<Integer> si = Sized.valueOf(--i);
            index.put(si, si);
        } while (index.getNumBlocks() < 3);


        int nulls = index.getBlocks().values().stream().mapToInt(b -> b.next == null ? 1 : 0).sum();

        Assert.assertEquals(1, nulls);
    }

    @Test
    public void testRemoveHead() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.delete(Sized.valueOf(1), Sized.valueOf("a"));
        List<Sized<String>> searchResult = index.search(Sized.valueOf(1));
        assertTrue(searchResult.isEmpty());
    }

    @Test
    public void testSimpleSplitSameKey() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(400, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.put(Sized.valueOf(1), Sized.valueOf("b"));
        index.put(Sized.valueOf(1), Sized.valueOf("c"));
        dumpIndex(index);
        List<Sized<String>> searchResult = index.search(Sized.valueOf(1));
        System.out.println("searchResult:" + searchResult);
        assertEquals(3, searchResult.size());
        assertEquals(Sized.valueOf("a"), searchResult.get(0));
        assertEquals(Sized.valueOf("b"), searchResult.get(1));
        assertEquals(Sized.valueOf("c"), searchResult.get(2));
        assertEquals(2, index.getNumBlocks());
    }

    @Test
    public void testUnboundedSearch() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        assertEquals(3, index.lookUpRange(Sized.valueOf(1), null).size());
        assertEquals(2, index.lookUpRange(null, Sized.valueOf(2)).size());

    }

    @Test
    public void lookupVeryFirstEntry() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        index.put(Sized.valueOf(4), Sized.valueOf("d"));
        index.put(Sized.valueOf(5), Sized.valueOf("e"));
        index.put(Sized.valueOf(6), Sized.valueOf("f"));
        dumpIndex(index);
        List<Sized<String>> searchResult = index.search(Sized.valueOf(1));
        System.out.println("searchResult:" + searchResult);
        assertEquals(1, searchResult.size());

        List<Sized<String>> searchResult2 = index.lookUpRange(Sized.valueOf(1), Sized.valueOf(4));
        System.out.println("searchResult:" + searchResult2);
        assertEquals(4, searchResult2.size());
        assertEquals(Sized.valueOf("a"), searchResult2.get(0));
        assertEquals(Sized.valueOf("b"), searchResult2.get(1));
        assertEquals(Sized.valueOf("c"), searchResult2.get(2));
        assertEquals(Sized.valueOf("d"), searchResult2.get(3));
    }

    @Test
    public void testSimpleSplitInverse() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(400, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        dumpIndex(index);

        assertEquals(Sized.valueOf("a"), index.search(Sized.valueOf(1)).get(0));
        assertEquals(Sized.valueOf("b"), index.search(Sized.valueOf(2)).get(0));
        assertEquals(Sized.valueOf("c"), index.search(Sized.valueOf(3)).get(0));
        assertEquals(2, index.getNumBlocks());
    }

    @Test
    public void testDelete() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(10));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.delete(Sized.valueOf(1), Sized.valueOf("a"));
        assertTrue(index.search(Sized.valueOf(1)).isEmpty());
        assertEquals(Sized.valueOf("b"), index.search(Sized.valueOf(2)).get(0));
        assertEquals(Sized.valueOf("c"), index.search(Sized.valueOf(3)).get(0));

        index.delete(Sized.valueOf(2), Sized.valueOf("b"));
        assertTrue(index.search(Sized.valueOf(2)).isEmpty());
        assertEquals(Sized.valueOf("c"), index.search(Sized.valueOf(3)).get(0));
        index.delete(Sized.valueOf(3), Sized.valueOf("c"));
        assertTrue(index.search(Sized.valueOf(3)).isEmpty());
    }

    /**
     * Verify that deleted entries stay deleted ever after a page unload
     *
     * @author diego.salvi
     */
    @Test
    public void testDeleteAndUnload() throws IOException {

        /* Must be 1 to keep just one page in memory keeping to unload on every page load */
        final PageReplacementPolicy policy = new RandomPageReplacementPolicy(1);

        final IndexDataStorage<Sized<Integer>, Sized<String>> storage = new MemoryIndexDataStorage<>();

        final BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(400, policy, storage);

        /* Add values until block split */
        int elements;
        for (elements = 0; index.getNumBlocks() < 2; elements++) {
            index.put(Sized.valueOf(elements), Sized.valueOf("test_" + elements));
        }

        /* NumBlocks must be greater than 1 to permit unloading */
        Assert.assertTrue(index.getNumBlocks() > 1);

        /* Check every value existance */
        for (int i = 0; i < elements; i++) {
            List<Sized<String>> result = index.search(Sized.valueOf(i));
            Assert.assertEquals(1, result.size());
        }

        /* Remove every value */
        for (int i = 0; i < elements; i++) {
            index.delete(Sized.valueOf(i), Sized.valueOf("test_" + i));
        }

        /* Check every value non existance */
        for (int i = 0; i < elements; i++) {
            List<Sized<String>> result = index.search(Sized.valueOf(i));
            Assert.assertEquals(0, result.size());
        }

        index.clear();
    }

    /**
     * Verify that BRIN blocks next field is managed when pointed block get deleted
     *
     * @author diego.salvi
     */
    @Test
    public void testSplitAndDelete() throws IOException {

        /* Must be 1 to keep just one page in memory keeping to unload on every page load */
        final PageReplacementPolicy policy = new RandomPageReplacementPolicy(1);

        final MemoryIndexDataStorage<Sized<Integer>, Sized<String>> storage = new MemoryIndexDataStorage<>();

        final BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(400, policy, storage);

        /* Add values until block split */
        int elements;
        for (elements = 0; index.getNumBlocks() < 2; elements++) {
            /* All entries have the same key to create contiguous blocks with same keys */
            index.put(Sized.valueOf(1), Sized.valueOf("test_" + elements));
        }

        /* NumBlocks must be greater than 1 (split done) */
        Assert.assertTrue(index.getNumBlocks() > 1);

        /* Check every value existance */
        {
            List<Sized<String>> result = index.search(Sized.valueOf(1));
            Assert.assertEquals(elements, result.size());
        }

        /* Remove every last value until last block is empty */
        for (int i = elements - 1; i > -1; i--) {
            index.delete(Sized.valueOf(1), Sized.valueOf("test_" + i));

            /* Check if last block got emptied */
            if ( index.getBlocks().lastEntry().getValue().getSize() == 0) {
                elements = i;
                break;
            }
        }

        /* Now checkpoint to remove empty block */
        BlockRangeIndexMetadata<Sized<Integer>> metadata = index.checkpoint();

        /* Deletes unreferenced pages from memory store */
        storage.getPages().retainAll(metadata.getBlocksMetadata().stream().map(m -> m.pageId).collect(Collectors.toList()));

        /* Now deleted block has been unloaded AND his page removed from store */

        /* Delete remaining values (next should have been handled to avoid errors) */
        for (int i = 0; i < elements; i++) {
            index.delete(Sized.valueOf(1), Sized.valueOf("test_" + i));
        }

        /* Check every value non existance */
        {
            List<Sized<String>> result = index.search(Sized.valueOf(1));
            Assert.assertEquals(0, result.size());
        }

        index.clear();
    }

    @Test
    public void testMultiple() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(10));
        for (int i = 0; i < 10; i++) {
            index.put(Sized.valueOf(i), Sized.valueOf("test_" + i));
        }
        for (int i = 0; i < 10; i++) {
            List<Sized<String>> result = index.search(Sized.valueOf(i));
            assertEquals(1, result.size());
            assertEquals(Sized.valueOf("test_" + i), result.get(0));
        }
        for (int i = 0; i < 10; i++) {
            index.put(Sized.valueOf(i), Sized.valueOf("test_" + i));
        }
        for (int i = 0; i < 10; i++) {
            if (i == 6) {
                System.out.println("QUI");
            }
            List<Sized<String>> result = index.search(Sized.valueOf(i));

            System.out.println("result for " + i + " :" + result);
            assertEquals(2, result.size());
            assertEquals(Sized.valueOf("test_" + i), result.get(0));
            assertEquals(Sized.valueOf("test_" + i), result.get(1));
        }
        List<Sized<String>> range = index.lookUpRange(Sized.valueOf(3), Sized.valueOf(5));
        assertEquals(6, range.size());

        for (int i = 0; i < 10; i++) {
            index.delete(Sized.valueOf(i), Sized.valueOf("test_" + i));
            index.delete(Sized.valueOf(i), Sized.valueOf("test_" + i));
        }
        for (int i = 0; i < 10; i++) {
            List<Sized<String>> result = index.search(Sized.valueOf(i));
            assertEquals(0, result.size());
        }
    }

    @Test
    public void testManySegments() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(10));
        for (int i = 0; i < 20; i++) {
            index.put(Sized.valueOf(i), Sized.valueOf("test_" + i));
        }
        List<Sized<String>> result = index.lookUpRange(Sized.valueOf(2), Sized.valueOf(10));
        System.out.println("result_" + result);
        for (int i = 2; i <= 10; i++) {
            assertTrue(result.contains(Sized.valueOf("test_" + i)));
        }
        assertEquals(9, result.size());
    }

    private void dumpIndex(BlockRangeIndex<?, ?> index) {
        for (BlockRangeIndex.Block b : index.getBlocks().values()) {
            System.out.println("BLOCK " + b);
        }
    }

}
