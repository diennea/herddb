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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import herddb.core.PageReplacementPolicy;
import herddb.core.RandomPageReplacementPolicy;
import herddb.utils.Sized;

/**
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexStorageTest {

    @Test
    public void testSimple() throws Exception {

        IndexDataStorage<Sized<Integer>, Sized<String>> storage =
                new IndexDataStorage<Sized<Integer>, Sized<String>>() {

            AtomicLong newPageId = new AtomicLong();

            private ConcurrentHashMap<Long, List<Map.Entry<Sized<Integer>, Sized<String>>>> pages =
                    new ConcurrentHashMap<>();

            @Override
            public List<Map.Entry<Sized<Integer>, Sized<String>>> loadDataPage(long pageId) throws IOException {
                return pages.get(pageId);
            }

            @Override
            public long createDataPage(List<Map.Entry<Sized<Integer>, Sized<String>>> values) throws IOException {
                long newid = newPageId.incrementAndGet();
                pages.put(newid, values);
                return newid;
            }

        };

        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(400, new RandomPageReplacementPolicy(10), storage);

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
    public void testUnload() throws Exception {

        IndexDataStorage<Sized<Integer>, Sized<String>> storage = new IndexDataStorage<Sized<Integer>, Sized<String>>() {

            AtomicLong newPageId = new AtomicLong();

            private ConcurrentHashMap<Long, List<Map.Entry<Sized<Integer>, Sized<String>>>> pages = new ConcurrentHashMap<>();

            @Override
            public List<Map.Entry<Sized<Integer>, Sized<String>>> loadDataPage(long pageId) throws IOException {
                return pages.get(pageId);
            }

            @Override
            public long createDataPage(List<Map.Entry<Sized<Integer>, Sized<String>>> values) throws IOException {
                long newid = newPageId.incrementAndGet();
                pages.put(newid, values);
                return newid;
            }

        };

        PageReplacementPolicy policy = new RandomPageReplacementPolicy(10);
        BlockRangeIndex<Sized<Integer>, Sized<String>> index =
                new BlockRangeIndex<>(1024, policy, storage);
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
