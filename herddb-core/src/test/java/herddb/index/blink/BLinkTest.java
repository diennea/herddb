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
package herddb.index.blink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import javax.xml.ws.Holder;

import org.junit.Test;

import herddb.core.PageReplacementPolicy;
import herddb.core.RandomPageReplacementPolicy;

/**
 * Simpler tests for {@link BLink}
 *
 * @author diego.salvi
 */
public class BLinkTest {


    private static final class DummyBLinkIndexDataStorage<K extends Comparable<K>> implements BLinkIndexDataStorage<K> {
        AtomicLong newPageId = new AtomicLong();

        private ConcurrentHashMap<Long, Element<K>> pages = new ConcurrentHashMap<>();
        private ConcurrentHashMap<Long, BLinkMetadata<K>> metadatas = new ConcurrentHashMap<>();

        @Override
        public Element<K> loadPage(long pageId) throws IOException {
            return pages.get(pageId);
        }

        @Override
        public long createDataPage(Element<K> root) throws IOException {
            long id = newPageId.incrementAndGet();
            pages.put(id, root);
            return id;
        }

        @Override
        public long createMetadataPage(BLinkMetadata<K> metadata) throws IOException {
            long id = newPageId.incrementAndGet();
            metadatas.put(id, metadata);
            return id;
        }
    }

    @Test
    public void testCheckpointAndRestore() throws Exception {

        BLinkIndexDataStorage<String> storage = new DummyBLinkIndexDataStorage<>();

        BLink<String> blink = new BLink<>(4, 4, storage, new RandomPageReplacementPolicy(3));

        blink.insert("a", 1L);
        blink.insert("b", 2L);
        blink.insert("c", 3L);

        assertEquals(3, blink.size());

        BLinkMetadata<String> metadata = blink.checkpoint();


        BLink<String> blinkFromMeta = new BLink<>(metadata, storage, new RandomPageReplacementPolicy(3));

        assertEquals(1L, blinkFromMeta.search("a"));
        assertEquals(2L, blinkFromMeta.search("b"));
        assertEquals(3L, blinkFromMeta.search("c"));

        assertEquals(3, blinkFromMeta.size());

    }

    @Test
    public void testSearch() throws Exception {

        BLinkIndexDataStorage<Long> storage = new DummyBLinkIndexDataStorage<>();

        BLink<Long> blink = new BLink<>(4, 4, storage, new RandomPageReplacementPolicy(10));

        for (long l = 0; l < 100;l++) {
            blink.insert(l, l);
        }

        for (long l = 0; l < 100; l++) {
            assertEquals(l,blink.search(l));
        }
    }

    @Test
    public void testScan() throws Exception {

        BLinkIndexDataStorage<Long> storage = new DummyBLinkIndexDataStorage<>();

        BLink<Long> blink = new BLink<>(4, 4, storage, new RandomPageReplacementPolicy(10));

        final long insertions = 100;

        for (long l = 0; l < insertions;l++) {
            blink.insert(l, l);
        }

        long offset = 10;

        for (long l = 0; l < insertions - offset; l++) {

            Stream<Entry<Long,Long>> stream = blink.scan(l, l + offset);


            Holder<Long> h = new Holder<>(l);
            Holder<Long> count = new Holder<>(0L);

            StringBuilder builder = new StringBuilder();

            /* Check each value */
            stream.forEach(entry -> {
                assertEquals(h.value,entry.getValue());
                h.value++;
                count.value++;

                builder.append(entry.getValue()).append(", ");
            });

            builder.setLength(builder.length() -2);
            System.out.println("start " + l + " end " + (l + offset) + " -> " + builder);

            assertEquals(offset,(long) count.value);

        }
    }

    @Test
    public void testUnload() throws Exception {

        final int pages = 5;
        BLinkIndexDataStorage<Long> storage = new DummyBLinkIndexDataStorage<>();

        PageReplacementPolicy policy = new RandomPageReplacementPolicy(pages);

        BLink<Long> blink = new BLink<>(4, 4, storage, policy);

        for (long l = 0; l < 100;l++) {
            blink.insert(l, l);
        }

        assertEquals(pages, policy.size());

        assertNotEquals(0, blink.size());

        blink.close();

        /* No pages should remain in memory after unload!! */
        assertEquals(0, policy.size());

        assertEquals(0, blink.size());

    }
}
