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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import javax.xml.ws.Holder;

import org.junit.Test;

import herddb.core.PageReplacementPolicy;
import herddb.core.RandomPageReplacementPolicy;
import herddb.index.blink.BLink;
import herddb.index.blink.BLinkIndexDataStorage;
import herddb.index.blink.BLinkMetadata;
import herddb.index.blink.BLink.SizeEvaluator;
import herddb.utils.SizeAwareObject;
import herddb.utils.Sized;

/**
 * Simpler tests for {@link BLink}
 *
 * @author diego.salvi
 */
public class BLinkTest {


    private static final class DummyBLinkIndexDataStorage<K extends Comparable<K> & SizeAwareObject, V> implements BLinkIndexDataStorage<K,V> {
        AtomicLong newPageId = new AtomicLong();

        private ConcurrentHashMap<Long, Object> datas = new ConcurrentHashMap<>();


        @Override
        public Map<Comparable<K>, Long> loadNodePage(long pageId) throws IOException {
            return (Map<Comparable<K>, Long>) datas.get(pageId);
        }

        @Override
        public Map<Comparable<K>, V> loadLeafPage(long pageId) throws IOException {
            return (Map<Comparable<K>, V>) datas.get(pageId);
        }

        @Override
        public long createNodePage(Map<Comparable<K>, Long> data) throws IOException {
            long id = newPageId.incrementAndGet();
            datas.put(id, new HashMap<>(data));
            return id;
        }

        @Override
        public long createLeafPage(Map<Comparable<K>, V> data) throws IOException {
            long id = newPageId.incrementAndGet();
            datas.put(id, new HashMap<>(data));
            return id;
        }

        @Override
        public void overwriteNodePage(long pageId, Map<Comparable<K>, Long> data) throws IOException {
            datas.put(pageId, new HashMap<>(data));
        }

        @Override
        public void overwriteLeafPage(long pageId, Map<Comparable<K>, V> data) throws IOException {
            datas.put(pageId, new HashMap<>(data));
        }
    }

    private static final class StringSizeEvaluator implements SizeEvaluator<Sized<String>,Long> {
        @Override
        public long evaluateKey(Sized<String> key) {
            return key.getEstimatedSize();
        }

        @Override
        public long evaluateValue(Long value) {
            return 24L;
        }

        @Override
        public long evaluateAll(Sized<String> key, Long value) {
            return evaluateKey(key) + evaluateValue(value);
        }
    }

    private static final class LongSizeEvaluator implements SizeEvaluator<Sized<Long>,Long> {
        @Override
        public long evaluateKey(Sized<Long> key) {
            return key.getEstimatedSize();
        }

        @Override
        public long evaluateValue(Long value) {
            return 24L;
        }

        @Override
        public long evaluateAll(Sized<Long> key, Long value) {
            return evaluateKey(key) + evaluateValue(value);
        }
    }


    @Test
    public void testCheckpointAndRestore() throws Exception {

        String[] data = new String[] {
                "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z",
                "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"
        };

        BLinkIndexDataStorage<Sized<String>,Long> storage = new DummyBLinkIndexDataStorage<>();

        BLinkMetadata<Sized<String>> metadata;
        try (BLink<Sized<String>,Long> blink = new BLink<>(2048L, new StringSizeEvaluator(), new RandomPageReplacementPolicy(3), storage)) {

            for (int i = 0; i < data.length; ++i) {
                blink.insert(Sized.valueOf(data[i]), i + 1L);
            }

            assertEquals(data.length, blink.size());

            metadata = blink.checkpoint();
        }

        try (BLink<Sized<String>,Long> blinkFromMeta = new BLink<>(2048L, new StringSizeEvaluator(), new RandomPageReplacementPolicy(3), storage, metadata)) {

            /* Require at least two nodes! */
            assertNotEquals(1,metadata.nodes.size());

            for (int i = 0; i < data.length; ++i) {
                assertEquals(i + 1L, (long) blinkFromMeta.search(Sized.valueOf(data[i])));
            }

            assertEquals(data.length, blinkFromMeta.size());
        }

    }

    @Test
    public void testSearch() throws Exception {

        final long inserts = 100;

        BLinkIndexDataStorage<Sized<Long>,Long> storage = new DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>,Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            for (long l = 0; l < inserts;l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            for (long l = 0; l < inserts; l++) {
                assertEquals(l,(long) blink.search(Sized.valueOf(l)));
            }
        }
    }

    @Test
    public void testDelete() throws Exception {

        final long inserts = 100000;

        BLinkIndexDataStorage<Sized<Long>,Long> storage = new DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>,Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            for (long l = 0; l < inserts;l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            BLinkMetadata<Sized<Long>> metadata = blink.checkpoint();

            /* Require at least two nodes! */
            assertNotEquals(1,metadata.nodes.size());

            for (long l = 0; l < inserts; l++) {
                assertEquals(l,(long) blink.delete(Sized.valueOf(l)));
            }

            for (long l = 0; l < inserts; l++) {
                assertEquals(null,blink.search(Sized.valueOf(l)));
            }
        }
    }

    @Test
    public void testScan() throws Exception {

        BLinkIndexDataStorage<Sized<Long>,Long> storage = new DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>,Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            final long inserts = 100;

            for (long l = 0; l < inserts;l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            BLinkMetadata<Sized<Long>> metadata = blink.checkpoint();

            /* Require at least two nodes! */
            assertNotEquals(1,metadata.nodes.size());

            long offset = 10;

            for (long l = 0; l < inserts - offset; l++) {

                Stream<Entry<Sized<Long>,Long>> stream = blink.scan(Sized.valueOf(l),Sized.valueOf(l + offset));


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
    }

    @Test
    public void testUnload() throws Exception {

        final int pages = 5;
        final int inserts = 100;
        BLinkIndexDataStorage<Sized<Long>,Long> storage = new DummyBLinkIndexDataStorage<>();

        PageReplacementPolicy policy = new RandomPageReplacementPolicy(pages);

        try (BLink<Sized<Long>,Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), policy, storage)) {

            for (long l = 0; l < inserts;l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            /* Must fill the polocy */
            assertEquals(pages, policy.size());

            assertEquals(inserts, blink.size());

            blink.close();

            /* No pages should remain in memory after unload!! */
            assertEquals(0, policy.size());

            assertEquals(0, blink.size());

        }

    }
}
