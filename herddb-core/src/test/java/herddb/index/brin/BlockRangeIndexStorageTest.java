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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexStorageTest {

    @Test
    public void testSimple() throws Exception {

        IndexDataStorage<Integer, String> storage = new IndexDataStorage<Integer, String>() {

            AtomicLong newPageId = new AtomicLong();

            private ConcurrentHashMap<Long, List<Map.Entry<Integer, String>>> pages = new ConcurrentHashMap<>();

            @Override
            public List<Map.Entry<Integer, String>> loadDataPage(long pageId) throws IOException {
                return pages.get(pageId);
            }

            @Override
            public long createDataPage(List<Map.Entry<Integer, String>> values) throws IOException {
                long newid = newPageId.incrementAndGet();
                pages.put(newid, values);
                return newid;
            }

        };

        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(2, Integer.MAX_VALUE, storage);

        index.put(1, "a");
        index.put(2, "b");
        index.put(3, "c");
        BlockRangeIndexMetadata<Integer> metadata = index.checkpoint();
        assertEquals(index.getNumBlocks(), metadata.getBlocksMetadata().size());

        index.unloadAllBlocks();

        assertEquals("a", index.search(1).get(0));
        assertEquals("b", index.search(2).get(0));
        assertEquals("c", index.search(3).get(0));
        assertEquals(2, index.getNumBlocks());

        BlockRangeIndex<Integer, String> indexAfterBoot = new BlockRangeIndex<>(2, Integer.MAX_VALUE, storage);
        indexAfterBoot.boot(metadata);
        assertEquals("a", indexAfterBoot.search(1).get(0));
        assertEquals("b", indexAfterBoot.search(2).get(0));
        assertEquals("c", indexAfterBoot.search(3).get(0));
        assertEquals(2, indexAfterBoot.getNumBlocks());

    }

    @Test
    public void testUnload() throws Exception {

        IndexDataStorage<Integer, String> storage = new IndexDataStorage<Integer, String>() {

            AtomicLong newPageId = new AtomicLong();

            private ConcurrentHashMap<Long, List<Map.Entry<Integer, String>>> pages = new ConcurrentHashMap<>();

            @Override
            public List<Map.Entry<Integer, String>> loadDataPage(long pageId) throws IOException {
                return pages.get(pageId);
            }

            @Override
            public long createDataPage(List<Map.Entry<Integer, String>> values) throws IOException {
                long newid = newPageId.incrementAndGet();
                pages.put(newid, values);
                return newid;
            }

        };

        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(2, 10, storage);
        for (int i = 0; i < 100; i++) {
            index.put(i, "a");
        }
        BlockRangeIndexMetadata<Integer> metadata = index.checkpoint();
        assertEquals(index.getNumBlocks(), metadata.getBlocksMetadata().size());
        assertEquals(10,index.getLoadedBlocksCount());

        for (int i = 0; i < 100; i++) {
            assertEquals("a", index.search(i).get(0));
        }
        assertEquals(10,index.getLoadedBlocksCount());
        index.unloadAllBlocks();
        assertEquals(0,index.getLoadedBlocksCount());

        BlockRangeIndex<Integer, String> indexAfterBoot = new BlockRangeIndex<>(2, 10, storage);
        indexAfterBoot.boot(metadata);
        assertEquals(metadata.getBlocksMetadata().size(), indexAfterBoot.getNumBlocks());
        assertEquals(0,indexAfterBoot.getLoadedBlocksCount());
        for (int i = 0; i < 100; i++) {
            assertEquals("a", indexAfterBoot.search(i).get(0));
        }
        assertEquals(10,indexAfterBoot.getLoadedBlocksCount());
        assertEquals(metadata.getBlocksMetadata().size(), indexAfterBoot.getNumBlocks());
        

    }
}
