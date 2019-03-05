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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.AbstractIndexManager;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.index.IndexOperation;
import herddb.index.KeyToPageIndex;
import herddb.index.PrimaryIndexPrefixScan;
import herddb.index.PrimaryIndexRangeScan;
import herddb.index.PrimaryIndexSeek;
import herddb.index.blink.BLink.SizeEvaluator;
import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;
import herddb.log.LogSequenceNumber;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.sql.SQLRecordKeyFunction;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.ByteArrayCursor;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;

/**
 * Implementation of {@link KeyToPageIndex} with a backing {@link BLink} paged and stored to {@link DataStorageManager}.
 *
 * @author diego.salvi
 */
public class BLinkKeyToPageIndex implements KeyToPageIndex {

    private static final Logger LOGGER = Logger.getLogger(BLinkKeyToPageIndex.class.getName());

    public static final byte METADATA_PAGE = 0;
    public static final byte INNER_NODE_PAGE = 1;
    public static final byte LEAF_NODE_PAGE = 2;

    private static final byte NODE_PAGE_END_BLOCK = 0;
    private static final byte NODE_PAGE_KEY_VALUE_BLOCK = 1;
    private static final byte NODE_PAGE_INF_BLOCK = 2;

    private static final int METADATA_PAGE_END_BLOCK = 0;
    private static final int METADATA_PAGE_NODE_BLOCK = 1;

    private final String tableSpace;
    private final String indexName;

    private final MemoryManager memoryManager;
    private final DataStorageManager dataStorageManager;

    private final AtomicLong newPageId;

    private final BLinkIndexDataStorage<Bytes, Long> indexDataStorage;

    private volatile BLink<Bytes, Long> tree;

    private final AtomicBoolean closed;

    public static String deriveIndexName(String tableName) {
        return tableName + "_primary";
    }

    public BLinkKeyToPageIndex(String tableSpace, String tableName, MemoryManager memoryManager, DataStorageManager dataStorageManager) {
        super();
        this.tableSpace = tableSpace;
        this.indexName = deriveIndexName(tableName);

        this.memoryManager = memoryManager;
        this.dataStorageManager = dataStorageManager;

        this.newPageId = new AtomicLong(1);
        this.indexDataStorage = new BLinkIndexDataStorageImpl();

        this.closed = new AtomicBoolean(false);
    }

    @Override
    public long size() {
        return getTree().size();
    }

    @Override
    public void put(Bytes key, Long currentPage) {
        getTree().insert(key, currentPage);
    }

    @Override
    public boolean put(Bytes key, Long newPage, Long expectedPage) {
        return getTree().insert(key, newPage, expectedPage);
    }

    @Override
    public boolean containsKey(Bytes key) {
        return getTree().search(key) != null;
    }

    @Override
    public Long get(Bytes key) {
        return getTree().search(key);
    }

    @Override
    public Long remove(Bytes key) {
        return getTree().delete(key);
    }

    @Override
    public boolean isSortedAscending() {
        return true;
    }

    @Override
    public Stream<Entry<Bytes, Long>> scanner(IndexOperation operation, StatementEvaluationContext context,
        TableContext tableContext, AbstractIndexManager index) throws DataStorageManagerException, StatementExecutionException {
        if (operation instanceof PrimaryIndexSeek) {
            PrimaryIndexSeek seek = (PrimaryIndexSeek) operation;
            byte[] seekValue = seek.value.computeNewValue(null, context, tableContext);
            if (seekValue == null) {
                return Stream.empty();
            }
            Bytes key = Bytes.from_array(seekValue);
            Long pageId = getTree().search(key);
            if (pageId == null) {
                return Stream.empty();
            }
            return Stream.of(new AbstractMap.SimpleImmutableEntry<>(key, pageId));
        }

        if (operation instanceof PrimaryIndexPrefixScan) {

            PrimaryIndexPrefixScan scan = (PrimaryIndexPrefixScan) operation;
//            SQLRecordKeyFunction value = sis.value;
            byte[] refvalue = scan.value.computeNewValue(null, context, tableContext);
            Bytes firstKey = Bytes.from_array(refvalue);
            Bytes lastKey = firstKey.next();

            return getTree().scan(firstKey, lastKey);
        }

        // Remember that the IndexOperation can return more records
        // every predicate (WHEREs...) will always be evaluated anyway on every record, in order to guarantee correctness
        if (index != null) {
            return index.recordSetScanner(operation, context, tableContext, this);
        }

        if (operation == null) {
            Stream<Map.Entry<Bytes, Long>> baseStream = getTree().scan(null, null);
            return baseStream;
        } else if (operation instanceof PrimaryIndexRangeScan) {

            Bytes refminvalue;
            PrimaryIndexRangeScan sis = (PrimaryIndexRangeScan) operation;
            SQLRecordKeyFunction minKey = sis.minValue;
            if (minKey != null) {
                refminvalue = Bytes.from_array(minKey.computeNewValue(null, context, tableContext));
            } else {
                refminvalue = null;
            }
            Bytes refmaxvalue;
            SQLRecordKeyFunction maxKey = sis.maxValue;
            if (maxKey != null) {
                refmaxvalue = Bytes.from_array(maxKey.computeNewValue(null, context, tableContext));
            } else {
                refmaxvalue = null;
            }
            return getTree().scan(refminvalue, refmaxvalue, refmaxvalue != null);
        }

        throw new DataStorageManagerException("operation " + operation + " not implemented on " + this.getClass());

    }

    @Override
    public void close() throws DataStorageManagerException {

        if (closed.compareAndSet(false, true)) {
            final BLink<Bytes, Long> tree = this.tree;
            this.tree = null;
            if (tree != null) {
                tree.close();
            }
        } else {
            throw new DataStorageManagerException("Index " + indexName + " already closed");
        }
    }

    @Override
    public void truncate() {
        getTree().truncate();
    }

    @Override
    public long getUsedMemory() {
        return 0;
    }

    @Override
    public boolean requireLoadAtStartup() {
        return false;
    }

    @Override
    public void start(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {

        LOGGER.log(Level.SEVERE, " start index {0}", new Object[]{indexName});

        /* Actually the same size */
        final long pageSize = memoryManager.getMaxLogicalPageSize();

        if (LogSequenceNumber.START_OF_TIME.equals(sequenceNumber)) {
            /* Empty index (booting from the start) */
            tree = new BLink<>(pageSize, SizeEvaluatorImpl.INSTANCE,
                memoryManager.getPKPageReplacementPolicy(), indexDataStorage);
            LOGGER.log(Level.SEVERE, "loaded empty index {0}", new Object[]{indexName});
        } else {
            IndexStatus status = dataStorageManager.getIndexStatus(tableSpace, indexName, sequenceNumber);
            try {
                BLinkMetadata<Bytes> metadata = MetadataSerializer.INSTANCE.read(status.indexData);

                tree = new BLink<>(pageSize, SizeEvaluatorImpl.INSTANCE,
                    memoryManager.getPKPageReplacementPolicy(), indexDataStorage,
                    metadata);
            } catch (IOException e) {
                throw new DataStorageManagerException(e);
            }

            newPageId.set(status.newPageId);
            LOGGER.log(Level.SEVERE, "loaded index {0}: {1} keys", new Object[]{indexName, tree.size()});
        }
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {

        try {

            /* Tree can be null if no data was inserted (tree creation deferred to check evaluate key size) */
            final BLink<Bytes, Long> tree = this.tree;
            if (tree == null) {
                return Collections.emptyList();
            }

            BLinkMetadata<Bytes> metadata = getTree().checkpoint();

            byte[] metaPage = MetadataSerializer.INSTANCE.write(metadata);

            Set<Long> activePages = new HashSet<>();
            metadata.nodes.forEach(node -> activePages.add(node.storeId));

            IndexStatus indexStatus = new IndexStatus(indexName, sequenceNumber, newPageId.get(), activePages, metaPage);
            List<PostCheckpointAction> result = new ArrayList<>();
            result.addAll(dataStorageManager.indexCheckpoint(tableSpace, indexName, indexStatus, pin));

            LOGGER.log(Level.INFO, "checkpoint index {0} finished: logpos {1}, {2} pages",
                new Object[]{indexName, sequenceNumber, Integer.toString(metadata.nodes.size())});
            LOGGER.log(Level.FINE, "checkpoint index {0} finished: logpos {1}, pages {2}",
                new Object[]{indexName, sequenceNumber, activePages.toString()});

            return result;

        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        dataStorageManager.unPinIndexCheckpoint(tableSpace, indexName, sequenceNumber);
    }

    /**
     * Retrieve {@link BLink} tree checking the index status
     */
    private BLink<Bytes, Long> getTree() {
        final BLink<Bytes, Long> tree = this.tree;

        if (tree == null) {
            if (closed.get()) {
                throw new DataStorageManagerException("Index " + indexName + " already closed");
            } else {
                throw new DataStorageManagerException("Index " + indexName + " still not started");
            }
        }

        return tree;
    }

    private static final class SizeEvaluatorImpl implements SizeEvaluator<Bytes, Long> {

        /**
         * Siongleton INSTANCE
         */
        public static final SizeEvaluator<Bytes, Long> INSTANCE = new SizeEvaluatorImpl();

        /**
         * Private constructor, use Singleton instance {@link #INSTANCE}
         */
        private SizeEvaluatorImpl() {
        }

        @Override
        public long evaluateKey(Bytes key) {
            return key.getEstimatedSize();
        }

        @Override
        public long evaluateValue(Long value) {
            /**
             * <pre>
             * java.lang.Long object internals:
             *  OFFSET  SIZE   TYPE DESCRIPTION                               VALUE
             *       0    12        (object header)                           N/A
             *      12     4        (alignment/padding gap)
             *      16     8   long Long.value                                N/A
             * Instance size: 24 bytes
             * Space losses: 4 bytes internal + 0 bytes external = 4 bytes total
             * </pre>
             */
            return 24L;
        }

        @Override
        public long evaluateAll(Bytes key, Long value) {
            return key.getEstimatedSize() + 24L;
        }

        @Override
        public Bytes getPosiviveInfinityKey() {
            return Bytes.POSITIVE_INFINITY;
        }

    }

    public static final class MetadataSerializer {

        public static final MetadataSerializer INSTANCE = new MetadataSerializer();

        /** Original Version */
        private static final long VERSION_0 = 0L;

        /**
         * Added flags options after version in metadata and forced byte size recalculation due to changes
         * on size evaluation algorithm
         *
         * @since 0.6.0
         */
        private static final long VERSION_1 = 1L;

        public static final long CURRENT_VERSION = VERSION_1;

        private static final long NO_FLAGS = 0L;

        public byte[] write(BLinkMetadata<Bytes> metadata) throws IOException {

            final VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            try (ExtendedDataOutputStream edos = new ExtendedDataOutputStream(bos)) {

                /* data version */
                edos.writeVLong(CURRENT_VERSION);

                /* flags for future implementations, actually unused */
                edos.writeVLong(NO_FLAGS);
                edos.writeByte(METADATA_PAGE);

                edos.writeVLong(metadata.nextID);

                edos.writeVLong(metadata.fast);
                edos.writeVInt(metadata.fastheight);

                edos.writeVLong(metadata.top);
                edos.writeVInt(metadata.topheight);

                edos.writeVLong(metadata.first);
                edos.writeVLong(metadata.values);

                for (BLinkNodeMetadata<Bytes> node : metadata.nodes) {

                    edos.writeVInt(METADATA_PAGE_NODE_BLOCK);

                    edos.writeBoolean(node.leaf);

                    edos.writeVLong(node.id);
                    edos.writeVLong(node.storeId);

                    edos.writeBoolean(node.empty);

                    edos.writeVInt(node.keys);
                    edos.writeVLong(node.bytes);

                    edos.writeZLong(node.outlink);
                    edos.writeZLong(node.rightlink);

                    boolean hasInf = node.rightsep == Bytes.POSITIVE_INFINITY;

                    edos.writeBoolean(hasInf);

                    if (!hasInf) {
                        edos.writeArray(node.rightsep.to_array());
                    }
                }

                edos.writeVInt(METADATA_PAGE_END_BLOCK);

            }

            return bos.toByteArray();

        }

        @SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "flags still not used but it must be forcefully read")
        public BLinkMetadata<Bytes> read(byte[] data) throws IOException {

            try (ByteArrayCursor edis = ByteArrayCursor.wrap(data)) {

                long version = edis.readVLong();

                /*
                 * Check if byte size needs to be recalculated (between v.0 and v.1 was changed size evaluation
                 * algorithm so v.0 stored size is meaningless)
                 */
                boolean recalculateSize = version == VERSION_0;

                /* flags for future implementations, actually unused (exists from version 1)*/
                @SuppressWarnings("unused")
                long flags = version > VERSION_0 ? edis.readVLong() : NO_FLAGS;

                byte rtype = edis.readByte();

                if (rtype != METADATA_PAGE) {
                    throw new IOException("Wrong page type " + rtype + " expected " + METADATA_PAGE);
                }

                long nextID = edis.readVLong();

                long fast = edis.readVLong();
                int fastheight = edis.readVInt();

                long top = edis.readVLong();
                int topheight = edis.readVInt();

                long first = edis.readVLong();
                long values = edis.readVLong();

                List<BLinkNodeMetadata<Bytes>> nodes = new LinkedList<>();

                int block;

                while ((block = edis.readVInt()) != METADATA_PAGE_END_BLOCK) {

                    if (block != METADATA_PAGE_NODE_BLOCK) {
                        throw new IOException("Wrong block type " + block);
                    }

                    final boolean leaf = edis.readBoolean();

                    long id = edis.readVLong();
                    long storeId = edis.readVLong();

                    boolean empty = edis.readBoolean();

                    int keys = edis.readVInt();
                    long bytes = edis.readVLong();

                    if (recalculateSize) {
                        /* Set size to unknown to force node size recalculation */
                        bytes = BLink.UNKNOWN_SIZE;
                    }

                    long outlink = edis.readZLong();
                    long rightlink = edis.readZLong();

                    boolean hasInf = edis.readBoolean();

                    Bytes rightsep;
                    if (hasInf) {
                        rightsep = Bytes.POSITIVE_INFINITY;
                    } else {
                        rightsep = edis.readBytesNoCopy();
                    }

                    BLinkNodeMetadata<Bytes> node
                        = new BLinkNodeMetadata<>(leaf, id, storeId, empty, keys, bytes, outlink, rightlink, rightsep);

                    nodes.add(node);
                }

                return new BLinkMetadata<>(nextID, fast, fastheight, top, topheight, first, values, nodes);
            }

        }
    }

    private final class BLinkIndexDataStorageImpl implements BLinkIndexDataStorage<Bytes, Long> {

        @Override
        public void loadNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            loadPage(pageId, INNER_NODE_PAGE, data);
        }

        @Override
        public void loadLeafPage(long pageId, Map<Bytes, Long> data) throws IOException {
            loadPage(pageId, LEAF_NODE_PAGE, data);
        }

        private void loadPage(long pageId, byte type, Map<Bytes, Long> map) throws IOException {

            dataStorageManager.readIndexPage(tableSpace, indexName, pageId, in -> {

                long version = in.readVLong();

                /* flags for future implementations, actually unused */
                long flags = in.readVLong();

                if (version != 1 || flags != 0) {
                    throw new IOException("Corrupted index page " + pageId);
                }

                byte rtype = in.readByte();

                if (rtype != type) {
                    throw new IOException("Wrong page type " + rtype + " expected " + type);
                }

                byte block;
                while ((block = in.readByte()) != NODE_PAGE_END_BLOCK) {

                    switch (block) {

                        case NODE_PAGE_KEY_VALUE_BLOCK:
                            map.put(in.readBytesNoCopy(),
                                in.readVLong());
                            break;

                        case NODE_PAGE_INF_BLOCK:
                            map.put(Bytes.POSITIVE_INFINITY, in.readVLong());
                            break;

                        default:
                            throw new IOException("Wrong node block type " + block);

                    }
                }

                return map;

            });

        }

        @Override
        public long createNodePage(Map<Bytes, Long> data) throws IOException {
            /* Both node ids and leaf values are Long, direct both to a common method */
            return createPage(NEW_PAGE, data, INNER_NODE_PAGE);
        }

        @Override
        public long createLeafPage(Map<Bytes, Long> data) throws IOException {
            /* Both node ids and leaf values are Long, direct both to a common method */
            return createPage(NEW_PAGE, data, LEAF_NODE_PAGE);
        }

        @Override
        public void overwriteNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            /* Both node ids and leaf values are Long, direct both to a common method */
            createPage(pageId, data, INNER_NODE_PAGE);
        }

        @Override
        public void overwriteLeafPage(long pageId, Map<Bytes, Long> data) throws IOException {
            /* Both node ids and leaf values are Long, direct both to a common method */
            createPage(pageId, data, LEAF_NODE_PAGE);
        }

        private long createPage(long pageId, Map<Bytes, Long> data, byte type) throws IOException {
            /* Write/overwrite switch */
            if (pageId == NEW_PAGE) {
                pageId = newPageId.getAndIncrement();
            }

            dataStorageManager.writeIndexPage(tableSpace, indexName, pageId, out -> {

                /* Data version */
                out.writeVLong(1);

                /* flags for future implementations, actually unused */
                out.writeVLong(0);

                out.writeByte(type);

                data.forEach((x, y) -> {
                    try {
                        if (x == Bytes.POSITIVE_INFINITY) {
                            // Handle special case for +inf key
                            out.writeByte(NODE_PAGE_INF_BLOCK);
                            out.writeVLong(y);
                        } else {
                            out.writeByte(NODE_PAGE_KEY_VALUE_BLOCK);
                            out.writeArray(x.to_array());
                            out.writeVLong(y);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException("Unexpected IOException during node page write preparation", e);
                    }
                });

                out.writeByte(NODE_PAGE_END_BLOCK);

            });

            return pageId;
        }
    }
}
