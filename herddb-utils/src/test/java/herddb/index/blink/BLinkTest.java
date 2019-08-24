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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.PageReplacementPolicy;
import herddb.core.RandomPageReplacementPolicy;
import herddb.index.blink.BLink.SizeEvaluator;
import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;
import herddb.utils.Holder;
import herddb.utils.Sized;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.Test;

/**
 * Simpler tests for {@link BLink}
 *
 * @author diego.salvi
 */
public class BLinkTest {

    static final class DummyBLinkIndexDataStorage<K extends Comparable<K>, V> implements BLinkIndexDataStorage<K, V> {

        AtomicLong newPageId = new AtomicLong();
        AtomicLong swapIn = new AtomicLong();
        private ConcurrentHashMap<Long, Object> datas = new ConcurrentHashMap<>();

        @Override
        public void loadNodePage(long pageId, Map<K, Long> data) throws IOException {
            swapIn.incrementAndGet();
            @SuppressWarnings("unchecked")
            Map<K, Long> res = (Map<K, Long>) datas.get(pageId);
            if (res != null) {
                data.putAll(res);
            }
        }

        @Override
        public void loadLeafPage(long pageId, Map<K, V> data) throws IOException {
            swapIn.incrementAndGet();
            @SuppressWarnings("unchecked")
            Map<K, V> res = (Map<K, V>) datas.get(pageId);
            if (res != null) {
                data.putAll(res);
            }
        }

        @Override
        public long createNodePage(Map<K, Long> data) throws IOException {
            long id = newPageId.incrementAndGet();
            datas.put(id, new HashMap<>(data));
            return id;
        }

        @Override
        public long createLeafPage(Map<K, V> data) throws IOException {
            long id = newPageId.incrementAndGet();
            datas.put(id, new HashMap<>(data));
            return id;
        }

        @Override
        public void overwriteNodePage(long pageId, Map<K, Long> data) throws IOException {
            datas.put(pageId, new HashMap<>(data));
        }

        @Override
        public void overwriteLeafPage(long pageId, Map<K, V> data) throws IOException {
            datas.put(pageId, new HashMap<>(data));
        }
    }

    static final class StringSizeEvaluator implements SizeEvaluator<Sized<String>, Long> {

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

        private static final Sized<String> THE_BIGGEST_STRING = Sized.valueOf("{{{{{{{{{{{{{{{{{");

        @Override
        public Sized<String> getPosiviveInfinityKey() {
            return THE_BIGGEST_STRING;
        }

    }

    static final class LongSizeEvaluator implements SizeEvaluator<Sized<Long>, Long> {

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

        private static final Sized<Long> THE_BIGGEST_LONG = Sized.valueOf(Long.MAX_VALUE);

        @Override
        public Sized<Long> getPosiviveInfinityKey() {
            return THE_BIGGEST_LONG;
        }

    }

    @Test
    public void testCheckpointAndRestore() throws Exception {

        String[] data = new String[]{
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
                "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
        };

        BLinkIndexDataStorage<Sized<String>, Long> storage = new DummyBLinkIndexDataStorage<>();

        BLinkMetadata<Sized<String>> metadata;
        try (BLink<Sized<String>, Long> blink = new BLink<>(2048L, new StringSizeEvaluator(), new RandomPageReplacementPolicy(3), storage)) {

            for (int i = 0; i < data.length; ++i) {
                blink.insert(Sized.valueOf(data[i]), i + 1L);
            }

            assertEquals(data.length, blink.size());

            metadata = blink.checkpoint();
        }

        try (BLink<Sized<String>, Long> blinkFromMeta = new BLink<>(2048L, new StringSizeEvaluator(), new RandomPageReplacementPolicy(3), storage, metadata)) {

            /* Require at least two nodes! */
            assertNotEquals(1, metadata.nodes.size());

            for (int i = 0; i < data.length; ++i) {
                assertEquals(i + 1L, (long) blinkFromMeta.search(Sized.valueOf(data[i])));
            }

            assertEquals(data.length, blinkFromMeta.size());
        }

    }

    @Test
    public void testUnknownSizeAndRestore() throws Exception {

        String[] data = new String[]{
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
                "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
        };
        StringSizeEvaluator evaluator = new StringSizeEvaluator();
        for (String d : data) {
            assertTrue(Sized.valueOf(d).compareTo(evaluator.getPosiviveInfinityKey()) < 0);
        }

        BLinkIndexDataStorage<Sized<String>, Long> storage = new DummyBLinkIndexDataStorage<>();

        BLinkMetadata<Sized<String>> metadata;
        try (BLink<Sized<String>, Long> blink = new BLink<>(2048L, new StringSizeEvaluator(), new RandomPageReplacementPolicy(3), storage)) {

            for (int i = 0; i < data.length; ++i) {
                blink.insert(Sized.valueOf(data[i]), i + 1L);
            }

            assertEquals(data.length, blink.size());

            metadata = blink.checkpoint();
        }

        /* Reset each node size to unknown */
        List<BLinkNodeMetadata<Sized<String>>> unknownSizeNodes = new ArrayList<>(metadata.nodes.size());
        for (BLinkNodeMetadata<Sized<String>> node : metadata.nodes) {
            unknownSizeNodes.add(new BLinkNodeMetadata<>(
                    node.leaf,
                    node.id,
                    node.storeId,
                    node.keys,
                    BLink.UNKNOWN_SIZE,
                    node.outlink,
                    node.rightlink,
                    node.rightsep));

        }

        BLinkMetadata<Sized<String>> unknownSizeMetadata = new BLinkMetadata<>(
                metadata.nextID,
                metadata.fast,
                metadata.fastheight,
                metadata.top,
                metadata.topheight,
                metadata.first,
                metadata.values,
                unknownSizeNodes);

        /* Checks that node size has been changed for each node */
        for (int i = 0; i < metadata.nodes.size(); ++i) {
            BLinkNodeMetadata<Sized<String>> node = metadata.nodes.get(i);
            BLinkNodeMetadata<Sized<String>> unknownSizeNode = unknownSizeMetadata.nodes.get(i);

            assertNotNull(unknownSizeNode);
            assertNotEquals(node.bytes, unknownSizeNode.bytes);
        }

        BLinkMetadata<Sized<String>> rebuildMetadata;
        try (BLink<Sized<String>, Long> blinkFromMeta = new BLink<>(2048L, new StringSizeEvaluator(), new RandomPageReplacementPolicy(3), storage, unknownSizeMetadata)) {

            /* Require at least two nodes! */
            assertNotEquals(1, metadata.nodes.size());

            for (int i = 0; i < data.length; ++i) {
                assertEquals(i + 1L, (long) blinkFromMeta.search(Sized.valueOf(data[i])));
            }

            assertEquals(data.length, blinkFromMeta.size());

            rebuildMetadata = blinkFromMeta.checkpoint();
        }

        /* Checks that node size has been restored for each node */
        for (int i = 0; i < metadata.nodes.size(); ++i) {
            BLinkNodeMetadata<Sized<String>> node = metadata.nodes.get(i);
            BLinkNodeMetadata<Sized<String>> rebuildNode = rebuildMetadata.nodes.get(i);

            assertNotNull(rebuildNode);
            assertEquals(node.bytes, rebuildNode.bytes);
        }

    }

    @Test
    public void testSearch() throws Exception {
        List<Long> l = new ArrayList<>();
        for (long i = 0; i < 50; i++) {
            l.add(i);
        }
        testDataSet(l);
    }

    @Test
    public void testSearchInsertReverse() throws Exception {
        List<Long> l = new ArrayList<>();
        for (long i = 50 - 1; i >= 0; i--) {
            l.add(i);
        }
        testDataSet(l);
    }

    @Test
    public void testConstants() throws Exception {
        List<Long> l = new ArrayList<>();
        for (long i = 100 - 1; i >= 0; i--) {
            l.add(19L);
        }
        testDataSet(l);
    }

    private void testDataSet(List<Long> data) throws Exception {
        int[] maxSizes = {1024, 2048};
        for (int i : maxSizes) {
            testDataSet(data, i);
        }
    }

    private void testDataSet(List<Long> data, int maxSize) throws Exception {
        System.out.println("testDataSet " + data.size() + " maxSize:" + maxSize + ": " + data);
        BLinkIndexDataStorage<Sized<Long>, Long> storage = new DummyBLinkIndexDataStorage<>();
        BLinkMetadata<Sized<Long>> metadata;
        try (BLink<Sized<Long>, Long> blink = new BLink<>(maxSize, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            for (long l : data) {
                blink.insert(Sized.valueOf(l), l);
            }

            for (long l : data) {
                assertEquals(l, (long) blink.search(Sized.valueOf(l)));
            }
            metadata = blink.checkpoint();
            System.out.println("metadata:" + metadata);
            metadata.nodes.forEach(n -> {
                System.out.println("node:" + n + " rightsep:" + n.rightsep);
            });
        }
        // reboot
        try (BLink<Sized<Long>, Long> blink = new BLink<>(maxSize, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage, metadata)) {
            for (long l : data) {
                assertEquals(l, (long) blink.search(Sized.valueOf(l)));
            }
        }
    }

    @Test
    public void testConstructTree() throws Exception {
        testDataSet(Arrays.asList(1L), 512);
        testDataSet(Arrays.asList(1L, 2L, 3L), 512);
        testDataSet(Arrays.asList(1L, 2L, 3L, 4L), 512);
        testDataSet(Arrays.asList(4L, 3L, 2L, 1L), 512);
        testDataSet(Arrays.asList(4L, 3L, 2L, 2L, 1L), 512);
        testDataSet(Arrays.asList(3L, 2L, 4L, 2L, 1L), 512);
    }

    @Test
    public void testDelete() throws Exception {

        final long inserts = 100000;

        BLinkIndexDataStorage<Sized<Long>, Long> storage = new DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>, Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            for (long l = 0; l < inserts; l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            BLinkMetadata<Sized<Long>> metadata = blink.checkpoint();

            /* Require at least two nodes! */
            assertNotEquals(1, metadata.nodes.size());

            for (long l = 0; l < inserts; l++) {
                assertEquals(l, (long) blink.delete(Sized.valueOf(l)));
            }

            for (long l = 0; l < inserts; l++) {
                assertEquals(null, blink.search(Sized.valueOf(l)));
            }
        }
    }

    @Test
    public void testScan() throws Exception {

        BLinkIndexDataStorage<Sized<Long>, Long> storage = new DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>, Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            final long inserts = 100;

            for (long l = 0; l < inserts; l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            BLinkMetadata<Sized<Long>> metadata = blink.checkpoint();

            /* Require at least two nodes! */
            assertNotEquals(1, metadata.nodes.size());

            long offset = 10;

            for (long l = 0; l < inserts - offset; l++) {

                Stream<Entry<Sized<Long>, Long>> stream = blink.scan(Sized.valueOf(l), Sized.valueOf(l + offset));

                Holder<Long> h = new Holder<>(l);
                Holder<Long> count = new Holder<>(0L);

                StringBuilder builder = new StringBuilder();

                /* Check each value */
                stream.forEach(entry -> {
                    assertEquals(h.value, entry.getValue());
                    h.value++;
                    count.value++;

                    builder.append(entry.getValue()).append(", ");
                });

                builder.setLength(builder.length() - 2);
                System.out.println("start " + l + " end " + (l + offset) + " -> " + builder);

                assertEquals(offset, (long) count.value);

            }
        }
    }

    @Test
    public void testScanHeadNotExistent() throws Exception {

        BLinkIndexDataStorage<Sized<Long>, Long> storage = new DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>, Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            final long headNonExistent = 100;
            final long inserts = 100;

            for (long l = headNonExistent; l < inserts + headNonExistent; l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            BLinkMetadata<Sized<Long>> metadata = blink.checkpoint();

            /* Require at least two nodes! */
            assertNotEquals(1, metadata.nodes.size());

            long offset = 10;

            for (long l = 0; l < headNonExistent - offset; l++) {

                Stream<Entry<Sized<Long>, Long>> stream = blink.scan(Sized.valueOf(l), Sized.valueOf(l + offset));

                Holder<Long> h = new Holder<>(l);
                Holder<Long> count = new Holder<>(0L);

                StringBuilder builder = new StringBuilder();

                /* Check each value */
                stream.forEach(entry -> {
                    assertEquals(h.value, entry.getValue());
                    h.value++;
                    count.value++;

                    builder.append(entry.getValue()).append(", ");
                });

                assertEquals(0, (long) count.value);

            }
        }
    }

    @Test
    public void testScanTailNotExistent() throws Exception {

        BLinkIndexDataStorage<Sized<Long>, Long> storage = new DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>, Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            final long inserts = 100;
            final long tailNonExistent = 100;

            for (long l = 0; l < inserts; l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            BLinkMetadata<Sized<Long>> metadata = blink.checkpoint();

            /* Require at least two nodes! */
            assertNotEquals(1, metadata.nodes.size());

            long offset = 10;

            for (long l = inserts; l < tailNonExistent - offset; l++) {

                Stream<Entry<Sized<Long>, Long>> stream = blink.scan(Sized.valueOf(l), Sized.valueOf(l + offset));

                Holder<Long> h = new Holder<>(l);
                Holder<Long> count = new Holder<>(0L);

                StringBuilder builder = new StringBuilder();

                /* Check each value */
                stream.forEach(entry -> {
                    assertEquals(h.value, entry.getValue());
                    h.value++;
                    count.value++;

                    builder.append(entry.getValue()).append(", ");
                });

                assertEquals(0, (long) count.value);

            }
        }
    }

    @Test
    public void testScanMiddleNotExistent() throws Exception {

        BLinkIndexDataStorage<Sized<Long>, Long> storage = new DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>, Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            final long headInserts = 100;
            final long nonExistents = 100;
            final long tailInserts = 100;

            for (long l = 0; l < headInserts; l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            for (long l = headInserts + nonExistents; l < headInserts + nonExistents + tailInserts; l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            BLinkMetadata<Sized<Long>> metadata = blink.checkpoint();

            /* Require at least two nodes! */
            assertNotEquals(1, metadata.nodes.size());

            long offset = 10;

            for (long l = headInserts; l < headInserts + nonExistents - offset; l++) {

                Stream<Entry<Sized<Long>, Long>> stream = blink.scan(Sized.valueOf(l), Sized.valueOf(l + offset));

                Holder<Long> h = new Holder<>(l);
                Holder<Long> count = new Holder<>(0L);

                StringBuilder builder = new StringBuilder();

                /* Check each value */
                stream.forEach(entry -> {
                    assertEquals(h.value, entry.getValue());
                    h.value++;
                    count.value++;

                    builder.append(entry.getValue()).append(", ");
                });

                assertEquals(0, (long) count.value);

            }
        }
    }

    @Test
    public void testScanDotNotExistent() throws Exception {

        BLinkIndexDataStorage<Sized<Long>, Long> storage = new DummyBLinkIndexDataStorage<>();

        try (BLink<Sized<Long>, Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), new RandomPageReplacementPolicy(10), storage)) {

            final long headInserts = 100;
            final long nonExistents = 10;
            final long tailInserts = 100;

            for (long l = 0; l < headInserts; l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            for (long l = headInserts + nonExistents; l < headInserts + nonExistents + tailInserts; l++) {
                blink.insert(Sized.valueOf(l), l);
            }

            BLinkMetadata<Sized<Long>> metadata = blink.checkpoint();

            /* Require at least two nodes! */
            assertNotEquals(1, metadata.nodes.size());

            long offset = 100;

            for (long l = nonExistents; l < headInserts + nonExistents - offset; l++) {

                Stream<Entry<Sized<Long>, Long>> stream = blink.scan(Sized.valueOf(l), Sized.valueOf(l + offset));

                Holder<Long> h = new Holder<>(l);
                Holder<Long> count = new Holder<>(0L);

                StringBuilder builder = new StringBuilder();

                /* Check each value */
                stream.forEach(entry -> {
                    assertEquals(h.value, entry.getValue());
                    h.value++;
                    count.value++;

                    builder.append(entry.getValue()).append(", ");
                });

                builder.setLength(builder.length() - 2);
                System.out.println("start " + l + " end " + (l + offset) + " -> " + builder);

                assertEquals(offset - nonExistents, (long) count.value);

            }
        }
    }

    @Test
    public void testUnload() throws Exception {

        final int pages = 5;
        final int inserts = 100;
        BLinkIndexDataStorage<Sized<Long>, Long> storage = new DummyBLinkIndexDataStorage<>();

        PageReplacementPolicy policy = new RandomPageReplacementPolicy(pages);

        try (BLink<Sized<Long>, Long> blink = new BLink<>(2048L, new LongSizeEvaluator(), policy, storage)) {

            for (long l = 0; l < inserts; l++) {
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
