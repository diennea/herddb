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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import herddb.core.PageReplacementPolicy;
import herddb.core.RandomPageReplacementPolicy;
import herddb.index.brin.BlockRangeIndex.Block;
import herddb.utils.Sized;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

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
        index.boot(BlockRangeIndexMetadata.empty());
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
        index.boot(BlockRangeIndexMetadata.empty());

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
        index.boot(BlockRangeIndexMetadata.empty());
        index.boot(BlockRangeIndexMetadata.empty());
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.delete(Sized.valueOf(1), Sized.valueOf("a"));
        List<Sized<String>> searchResult = index.search(Sized.valueOf(1));
        assertTrue(searchResult.isEmpty());
    }

    @Test
    public void testSimpleSplitSameKey() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(400, new RandomPageReplacementPolicy(10));
        index.boot(BlockRangeIndexMetadata.empty());
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
        index.boot(BlockRangeIndexMetadata.empty());
        index.put(Sized.valueOf(1), Sized.valueOf("a"));
        index.put(Sized.valueOf(2), Sized.valueOf("b"));
        index.put(Sized.valueOf(3), Sized.valueOf("c"));
        assertEquals(3, index.search(Sized.valueOf(1), null).size());
        assertEquals(2, index.search(null, Sized.valueOf(2)).size());

    }

    @Test
    public void lookupVeryFirstEntry() {
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, new RandomPageReplacementPolicy(10));
        index.boot(BlockRangeIndexMetadata.empty());

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

        List<Sized<String>> searchResult2 = index.search(Sized.valueOf(1), Sized.valueOf(4));
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
        index.boot(BlockRangeIndexMetadata.empty());
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
        index.boot(BlockRangeIndexMetadata.empty());
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
        index.boot(BlockRangeIndexMetadata.empty());

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
        index.boot(BlockRangeIndexMetadata.empty());

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
            if (index.getBlocks().lastEntry().getValue().getSize() == 0) {
                elements = i;
                break;
            }
        }

        /* Now checkpoint to remove empty block */
        BlockRangeIndexMetadata<Sized<Integer>> metadata = index.checkpoint();

        /* Deletes unreferenced pages from memory store */
        storage.getPages().retainAll(metadata.getBlocksMetadata().stream().map(m -> m.pageId).collect(Collectors.
                toList()));

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
        index.boot(BlockRangeIndexMetadata.empty());
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
        List<Sized<String>> range = index.search(Sized.valueOf(3), Sized.valueOf(5));
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
        index.boot(BlockRangeIndexMetadata.empty());
        for (int i = 0; i < 20; i++) {
            index.put(Sized.valueOf(i), Sized.valueOf("test_" + i));
        }
        List<Sized<String>> result = index.search(Sized.valueOf(2), Sized.valueOf(10));
        System.out.println("result_" + result);
        for (int i = 2; i <= 10; i++) {
            assertTrue(result.contains(Sized.valueOf("test_" + i)));
        }
        assertEquals(9, result.size());
    }

    private void dumpIndex(BlockRangeIndex<?, ?> index) {
        for (BlockRangeIndex.Block<?, ?> b : index.getBlocks().values()) {
            System.out.println("BLOCK " + b);
        }
    }

    /**
     * This test was generating a block self relationship
     *
     * @see GitHub: https://github.com/diennea/herddb/pull/443
     */
    @Test
    public void testSelfLoop() throws IOException {
        Random random = new Random(2431);
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(400, new RandomPageReplacementPolicy(10, random));
        index.boot(BlockRangeIndexMetadata.empty());

        int numCheckpoints = 0;
        int i = 0;
        try {
            for (i = 0; i < 60; i++) {
                byte[] s = new byte[10];
                random.nextBytes(s);
                index.put(Sized.valueOf(random.nextInt(200)), Sized.valueOf(new String(s, StandardCharsets.US_ASCII)));
                if (random.nextInt(10) == 3) {
                    index.checkpoint();
                    numCheckpoints++;
                }
                if (i % 1000 == 0) {
                    System.out.println("DONE " + i + " - " + numCheckpoints + " checkpoints");
                }
            }
        } finally {
            System.out.println("DONE " + i + " - " + numCheckpoints + " checkpoints");
        }
    }

    /**
     * Test that after split order imposed by BRIN outer tree {@link BlockRangeIndex#blocks} is the same order
     * imposed by {@link BlockRangeIndex.Block#next} relationship.
     *
     * @see GitHub: https://github.com/diennea/herddb/pull/443
     */
    @Test
    public void testSplitBlockStartKeySorting() throws IOException {

        /**
         * Expected BRIN tree
         * <pre>
         *        +-----------------+
         *        | MinKey: 1       |
         * ... -> | BlockId: 1      | -> ...
         *        | Keys: 1, 2, 2   |
         *        | Occupancy: 100% |
         *        +-----------------+
         *
         * Adding key 2 with next block id 2: create a new block (2,-2)
         *
         *        +-----------------+    +-----------------+
         *        | MinKey: 1       |    | MinKey: 2       |
         * ... -> | BlockId: 1      | -> | BlockId: -2     | -> ...
         *        | Keys: 1, 2      |    | Keys: 2, 2      |
         *        | Occupancy: 66%  |    | Occupancy: 66%  |
         *        +-----------------+    +-----------------+
         *
         * Adding key 2 with next block id 3: add to block (2,-2)
         *
         *        +-----------------+    +-----------------+
         *        | MinKey: 1       |    | MinKey: 2       |
         * ... -> | BlockId: 1      | -> | BlockId: -2     | -> ...
         *        | Keys: 1, 2      |    | Keys: 2, 2, 2   |
         *        | Occupancy: 66%  |    | Occupancy: 100% |
         *        +-----------------+    +-----------------+
         *
         * Adding key 2 with next block id 3: create a new block (2,3)
         *
         *        +-----------------+    +-----------------+    +-----------------+
         *        | MinKey: 1       |    | MinKey: 2       |    | MinKey: 2       |
         * ... -> | BlockId: 1      | -> | BlockId: -2     | -> | BlockId: 3      | -> ...
         *        | Keys: 1, 2      |    | Keys: 2, 2      |    | Keys: 2, 2      |
         *        | Occupancy: 66%  |    | Occupancy: 66%  |    | Occupancy: 66%  |
         *        +-----------------+    +-----------------+    +-----------------+
         * </pre>
         */

        BlockRangeIndex<Sized<Integer>, Sized<Integer>> index =
                new BlockRangeIndex<>(450, new RandomPageReplacementPolicy(1000));

        index.boot(BlockRangeIndexMetadata.empty());

        Sized<Integer> s1 = Sized.valueOf(1);
        Sized<Integer> s2 = Sized.valueOf(2);

        index.put(s1, s1);
        index.put(s2, s2);
        index.put(s2, s2);

        assertEquals(1, index.getNumBlocks());

        Block<Sized<Integer>, Sized<Integer>> block1 = index.getBlocks().firstEntry().getValue();

        block1.ensureBlockLoaded();

        assertTrue(block1.values.containsKey(s1));
        assertTrue(block1.values.containsKey(s2));

        assertEquals(Arrays.asList(s1), block1.values.get(s1));
        assertEquals(Arrays.asList(s2, s2), block1.values.get(s2));


        index.put(s2, s2);

        assertEquals(2, index.getNumBlocks());

        Block<Sized<Integer>, Sized<Integer>> block2 = index.getBlocks().lastEntry().getValue();

        assertSame(block2, block1.next);

        assertTrue(block2.key.blockId < 0);
        assertTrue(block1.key.blockId > block2.key.blockId);
        assertTrue(block1.key.compareTo(block2.key) < 0);

        block1.ensureBlockLoaded();

        assertTrue(block1.values.containsKey(s1));
        assertTrue(block1.values.containsKey(s2));

        assertEquals(Arrays.asList(s1), block1.values.get(s1));
        assertEquals(Arrays.asList(s2), block1.values.get(s2));

        block2.ensureBlockLoaded();

        assertFalse(block2.values.containsKey(s1));
        assertTrue(block2.values.containsKey(s2));

        assertEquals(Arrays.asList(s2, s2), block2.values.get(s2));


        index.put(s2, s2);

        assertEquals(2, index.getNumBlocks());

        block2.ensureBlockLoaded();

        assertTrue(block2.values.containsKey(s2));

        assertEquals(Arrays.asList(s2, s2, s2), block2.values.get(s2));


        index.put(s2, s2);

        assertEquals(3, index.getNumBlocks());

        Block<Sized<Integer>, Sized<Integer>> block3 = index.getBlocks().lastEntry().getValue();

        assertSame(block3, block2.next);

        assertTrue(block3.key.blockId > 0);
        assertTrue(block2.key.blockId < block3.key.blockId);
        assertTrue(block2.key.compareTo(block3.key) < 0);

        block2.ensureBlockLoaded();

        assertFalse(block2.values.containsKey(s1));
        assertTrue(block2.values.containsKey(s2));

        assertEquals(Arrays.asList(s2, s2), block2.values.get(s2));

        block3.ensureBlockLoaded();

        assertFalse(block3.values.containsKey(s1));
        assertTrue(block3.values.containsKey(s2));

        assertEquals(Arrays.asList(s2, s2), block3.values.get(s2));

    }
}
