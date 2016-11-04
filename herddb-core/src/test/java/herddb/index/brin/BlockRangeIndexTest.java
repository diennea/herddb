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

import java.util.List;

import org.junit.Test;

/**
 * Unit tests for BlockRangeIndex
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexTest {

    @Test
    public void testSimpleSplit() {
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(2);
        index.put(1, "a");
        index.put(2, "b");
        index.put(3, "c");
        dumpIndex(index);
        assertEquals("a", index.search(1).get(0));
        assertEquals("b", index.search(2).get(0));
        assertEquals("c", index.search(3).get(0));
        assertEquals(2, index.getNumBlocks());
    }

    @Test
    public void testRemoveHead() {
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(2);
        index.put(1, "a");
        index.delete(1, "a");
        List<String> searchResult = index.search(1);
        assertTrue(searchResult.isEmpty());
    }

    @Test
    public void testSimpleSplitSameKey() {
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(2);
        index.put(1, "a");
        index.put(1, "b");
        index.put(1, "c");
        dumpIndex(index);
        List<String> searchResult = index.search(1);
        System.out.println("searchResult:" + searchResult);
        assertEquals(3, searchResult.size());
        assertEquals("a", searchResult.get(0));
        assertEquals("b", searchResult.get(1));
        assertEquals("c", searchResult.get(2));
        assertEquals(2, index.getNumBlocks());
    }

    @Test
    public void testUnboundedSearch() {
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(2);
        index.put(1, "a");
        index.put(2, "b");
        index.put(3, "c");
        assertEquals(3, index.lookUpRange(1, null).size());
        assertEquals(2, index.lookUpRange(null, 2).size());

    }

    @Test
    public void lookupVeryFirstEntry() {
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(2);
        index.put(1, "a");
        index.put(2, "b");
        index.put(3, "c");
        index.put(4, "d");
        index.put(5, "e");
        index.put(6, "f");
        dumpIndex(index);
        List<String> searchResult = index.search(1);
        System.out.println("searchResult:" + searchResult);
        assertEquals(1, searchResult.size());

        List<String> searchResult2 = index.lookUpRange(1, 4);
        System.out.println("searchResult:" + searchResult2);
        assertEquals(4, searchResult2.size());
        assertEquals("a", searchResult2.get(0));
        assertEquals("b", searchResult2.get(1));
        assertEquals("c", searchResult2.get(2));
        assertEquals("d", searchResult2.get(3));
    }

    @Test
    public void testSimpleSplitInverse() {
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(2);
        index.put(3, "c");
        index.put(2, "b");
        index.put(1, "a");
        dumpIndex(index);

        assertEquals("a", index.search(1).get(0));
        assertEquals("b", index.search(2).get(0));
        assertEquals("c", index.search(3).get(0));
        assertEquals(2, index.getNumBlocks());
    }

    @Test
    public void testDelete() {
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(2);
        index.put(3, "c");
        index.put(2, "b");
        index.put(1, "a");
        index.delete(1, "a");
        assertTrue(index.search(1).isEmpty());
        assertEquals("b", index.search(2).get(0));
        assertEquals("c", index.search(3).get(0));

        index.delete(2, "b");
        assertTrue(index.search(2).isEmpty());
        assertEquals("c", index.search(3).get(0));
        index.delete(3, "c");
        assertTrue(index.search(3).isEmpty());
    }

    @Test
    public void testMultiple() {
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(10);
        for (int i = 0; i < 10; i++) {
            index.put(i, "test_" + i);
        }
        for (int i = 0; i < 10; i++) {
            List<String> result = index.search(i);
            assertEquals(1, result.size());
            assertEquals("test_" + i, result.get(0));
        }
        for (int i = 0; i < 10; i++) {
            index.put(i, "test_" + i);
        }
        for (int i = 0; i < 10; i++) {
            if (i == 6) {
                System.out.println("QUI");
            }
            List<String> result = index.search(i);

            System.out.println("result for " + i + " :" + result);
            assertEquals(2, result.size());
            assertEquals("test_" + i, result.get(0));
            assertEquals("test_" + i, result.get(1));
        }
        List<String> range = index.lookUpRange(3, 5);
        assertEquals(6, range.size());

        for (int i = 0; i < 10; i++) {
            index.delete(i, "test_" + i);
            index.delete(i, "test_" + i);
        }
        for (int i = 0; i < 10; i++) {
            List<String> result = index.search(i);
            assertEquals(0, result.size());
        }
    }

    @Test
    public void testManySegments() {
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(10);
        for (int i = 0; i < 20; i++) {
            index.put(i, "test_" + i);
        }
        List<String> result = index.lookUpRange(2, 10);
        System.out.println("result_" + result);
        for (int i = 2; i <= 10; i++) {
            assertTrue(result.contains("test_" + i));
        }
        assertEquals(9, result.size());
    }

    private void dumpIndex(BlockRangeIndex<?, ?> index) {
        for (BlockRangeIndex.Block b : index.getBlocks().values()) {
            System.out.println("BLOCK " + b);
        }
    }

}
