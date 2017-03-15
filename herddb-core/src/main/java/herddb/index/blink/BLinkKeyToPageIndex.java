package herddb.index.blink;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import javax.xml.ws.Holder;

import herddb.core.AbstractIndexManager;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.index.IndexOperation;
import herddb.index.KeyToPageIndex;
import herddb.index.PrimaryIndexPrefixScan;
import herddb.index.PrimaryIndexSeek;
import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;
import herddb.log.LogSequenceNumber;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullIndexScanConsumer;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;

public class BLinkKeyToPageIndex implements KeyToPageIndex {

    private static final Logger LOGGER = Logger.getLogger(BLinkInner.class.getName());
    private static final byte[] NULL_KEY = new byte[0];

    public static final byte METADATA_PAGE = 0;
    public static final byte NODEDATA_PAGE = 1;

    private final String tableSpace;
    private final String indexName;

    private final MemoryManager memoryManager;
    private final DataStorageManager dataStorageManager;

    private final AtomicLong pageIDGenerator;

    private final BLinkIndexDataStorage<Bytes> indexDataStorage;

    public volatile BLink<Bytes> tree;


    private static final class MetadataSerializer {

        public static final MetadataSerializer INSTANCE = new MetadataSerializer();

        public void serialize(BLinkMetadata<Bytes> metadata, ExtendedDataOutputStream stream) throws IOException {

            stream.write(BLinkKeyToPageIndex.METADATA_PAGE);

            stream.writeVLong(metadata.nodeSize);
            stream.writeVLong(metadata.leafSize);
            stream.writeVLong(metadata.root);

            for (BLinkNodeMetadata<Bytes> node : metadata.nodeMetadatas) {

                stream.write(node.type);

                stream.writeVLong(node.nodeId);
                stream.writeVLong(node.storeId);

                if (node.highKey == null) {
                    stream.writeArray(NULL_KEY);
                } else {
                    stream.writeArray(node.highKey.data);
                }

                stream.writeVLong(node.maxKeys);

                stream.writeVLong(node.keys);
                stream.writeZLong(node.right);
            }
        }

        public BLinkMetadata<Bytes> deserialize(ExtendedDataInputStream stream) throws IOException {

                byte type = stream.readByte();

                if (type != METADATA_PAGE) {
                    throw new IOException("Wrong page type " + type);
                }

                long nodeSize = stream.readVLong();
                long leafSize = stream.readVLong();
                long root = stream.readVLong();

                List<BLinkNodeMetadata<Bytes>> nodes = new LinkedList<>();

                while (stream.available() > 0) {
                    final byte ntype = stream.readByte();

                    long nodeId = stream.readVLong();
                    long storeId = stream.readVLong();

                    byte[] hk = stream.readArray();
                    Bytes highKey = hk.length == 0 ? null : Bytes.from_array(hk);;

                    long maxKeys = stream.readVLong();

                    long keys = stream.readVLong();
                    long right = stream.readZLong();

                    BLinkNodeMetadata<Bytes> node = new BLinkNodeMetadata<>(ntype, nodeId, storeId, highKey, maxKeys, keys, right);

                    nodes.add(node);
                }

                BLinkMetadata<Bytes> metadata = new BLinkMetadata<>(nodeSize, leafSize, root, nodes);

                return metadata;
        }
    }

    public BLinkKeyToPageIndex(String tableSpace, String tableName, MemoryManager memoryManager, DataStorageManager dataStorageManager) {
        super();
        this.tableSpace = tableSpace;
        this.indexName = tableName + ".primary";

        this.memoryManager = memoryManager;
        this.dataStorageManager = dataStorageManager;

        this.pageIDGenerator = new AtomicLong(0);
        this.indexDataStorage = new BLinkIndexDataStorageImpl();
    }

    @Override
    public long size() {
        BLink<Bytes> tree = this.tree;

        return tree == null ? 0 : tree.size();
    }

    @Override
    public Long put(Bytes key, Long currentPage) {

        BLink<Bytes> tree = this.tree;
        if ( tree == null ) {
            synchronized (this) {

                tree = this.tree;
                if ( tree == null ) {

                    /* Create a tree using given key for page size estimation */
                    final long estimation = key.getEstimatedSize() + 32;
                    final long pageSize = memoryManager.getMaxLogicalPageSize();
                    final long nodeSize = pageSize / estimation;

                    if (nodeSize < 3) {
                        throw new IllegalArgumentException("Key size too big for current page size! Key " + estimation + " page " + pageSize );
                    }

                    tree = new BLink<>(nodeSize, nodeSize, indexDataStorage, memoryManager.getIndexPageReplacementPolicy());

                    /* Publish */
                    this.tree = tree;
                }
            }
        }

        return tree.insert(key, currentPage);
    }

    @Override
    public boolean containsKey(Bytes key) {
        final BLink<Bytes> tree = this.tree;
        if (tree == null) {
            return false;
        }
        return tree.search(key) != BLink.NO_RESULT;
    }

    @Override
    public Long get(Bytes key) {
        final BLink<Bytes> tree = this.tree;
        if (tree == null) {
            return null;
        }
        final long result = tree.search(key);
        return (result == BLink.NO_RESULT) ? null : result;
    }

    @Override
    public Long remove(Bytes key) {
        final BLink<Bytes> tree = this.tree;
        if (tree == null) {
            return null;
        }
        final long result = tree.delete(key);
        return (result == BLink.NO_RESULT) ? null : result;
    }

    @Override
    public Stream<Entry<Bytes, Long>> scanner(IndexOperation operation, StatementEvaluationContext context,
            TableContext tableContext, AbstractIndexManager index) throws DataStorageManagerException {
        final BLink<Bytes> tree = this.tree;
        if (tree == null) {
            return Stream.empty();
        }

        if (operation instanceof PrimaryIndexSeek) {
            try {
                PrimaryIndexSeek seek = (PrimaryIndexSeek) operation;
                byte[] seekValue = seek.value.computeNewValue(null, context, tableContext);
                if (seekValue == null) {
                    return Stream.empty();
                }
                Bytes key = Bytes.from_array(seekValue);
                long pageId = tree.search(key);
                if (pageId == BLink.NO_RESULT) {
                    return Stream.empty();
                }
                return Stream.of(new AbstractMap.SimpleImmutableEntry<>(key,pageId));
            } catch (StatementExecutionException err) {
                throw new DataStorageManagerException(err);
            }
        }

        if (operation instanceof PrimaryIndexPrefixScan) {

            PrimaryIndexPrefixScan scan = (PrimaryIndexPrefixScan) operation;
//            SQLRecordKeyFunction value = sis.value;
            byte[] refvalue = scan.value.computeNewValue(null, context, tableContext);
            Bytes firstKey = Bytes.from_array(refvalue);
            Bytes lastKey = firstKey.next();

            return tree.scan(firstKey, lastKey);
        }

        // Remember that the IndexOperation can return more records
        // every predicate (WHEREs...) will always be evaluated anyway on every record, in order to guarantee correctness
        if (index != null) {
            try {
                return index.recordSetScanner(operation, context, tableContext, this);
            } catch (StatementExecutionException err) {
                throw new DataStorageManagerException(err);
            }
        }

        if (operation == null) {
            Stream<Map.Entry<Bytes, Long>> baseStream = tree.fullScan();
            return baseStream;
        }

        throw new DataStorageManagerException("operation " + operation + " not implemented on " + this.getClass());

    }

    @Override
    public void close() {
        final BLink<Bytes> tree = this.tree;
        this.tree = null;
        if (tree != null) {
            tree.close();
        }
    }

    @Override
    public void truncate() {
        final BLink<Bytes> tree = this.tree;
        if (tree != null) {
            tree.truncate();
        }
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
    public void start() throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, " start index {0}", new Object[]{indexName});
//        bootSequenceNumber = log.getLastSequenceNumber();
        Holder<byte[]> binaryMetadata = new Holder<>();
        dataStorageManager.fullIndexScan(tableSpace, indexName,
            new FullIndexScanConsumer() {

            @Override
            public void acceptIndexStatus(IndexStatus indexStatus) {
                LOGGER.log(Level.SEVERE, "recovery index {0} at {1}", new Object[]{indexStatus.indexName, indexStatus.sequenceNumber});
//                bootSequenceNumber = indexStatus.sequenceNumber;
            }

            @Override
            public void acceptPage(long pageId, byte[] pagedata) {
                if (pageIDGenerator.get() <= pageId) {
                    pageIDGenerator.set(pageId + 1);
                }
                LOGGER.log(Level.FINEST, "recovery index {0}, acceptPage {1} pagedata: {2} bytes", new Object[]{indexName, pageId,pagedata.length});

                byte type = pagedata[0];

                switch(type) {
                    case METADATA_PAGE:
                        LOGGER.log(Level.INFO, "recovery index {0}, metatadata page {1}", new Object[]{indexName, pageId});
                        binaryMetadata.value = pagedata;
                        break;
                    case NODEDATA_PAGE:
                        LOGGER.log(Level.INFO, "recovery index {0}, data page {1}", new Object[]{indexName, pageId});
                        break;
                    default:
                        LOGGER.log(Level.SEVERE, "recovery index {0}, ignore page type {1}", new Object[]{indexName, type});
                        break;
                }
            }

        });

        if (binaryMetadata.value != null) {

            BLinkMetadata<Bytes> metadata;
            try (SimpleByteArrayInputStream bis = new SimpleByteArrayInputStream(binaryMetadata.value);
                 ExtendedDataInputStream edis = new ExtendedDataInputStream(bis)) {

                metadata = MetadataSerializer.INSTANCE.deserialize(edis);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            tree = new BLink<>(metadata, indexDataStorage, memoryManager.getDataPageReplacementPolicy());

            LOGGER.log(Level.SEVERE, "loaded index {0}: {1} keys", new Object[]{indexName, tree.size()});
        } else {
            /* Empty index */
            LOGGER.log(Level.SEVERE, "loaded empty index {0}", new Object[]{indexName});
        }

    }


    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {

        try {

            /* Tree can be null if no data was inserted (tree creation deferred to check evaluate key size) */
            final BLink<Bytes> tree = this.tree;
            if (tree == null) {
                return Collections.emptyList();
            }

            BLinkMetadata<Bytes> metadata = tree.checkpoint();

            long metaPage = indexDataStorage.createMetadataPage(metadata);

            Set<Long> activePages = new HashSet<>();
            activePages.add(metaPage);
            metadata.nodeMetadatas.forEach(node -> activePages.add(node.storeId));

            IndexStatus indexStatus = new IndexStatus(indexName, sequenceNumber, activePages, null);
            List<PostCheckpointAction> result = new ArrayList<>();
            result.addAll(dataStorageManager.indexCheckpoint(tableSpace, indexName, indexStatus));

            LOGGER.log(Level.SEVERE, "checkpoint index {0} finished, {1} blocks, pages {2}", new Object[]{
                    indexName, Integer.toString(metadata.nodeMetadatas.size()), activePages.toString()});

            return result;

        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    private class BLinkIndexDataStorageImpl implements BLinkIndexDataStorage<Bytes> {

        @Override
        public Element<Bytes> loadPage(long pageId) throws IOException {

            byte[] indexData = dataStorageManager.readIndexPage(tableSpace, indexName, pageId);

            final SimpleByteArrayInputStream bis = new SimpleByteArrayInputStream(indexData);

            try (ExtendedDataInputStream edis = new ExtendedDataInputStream(bis)) {

                byte type = edis.readByte();

                if (type != NODEDATA_PAGE) {
                    throw new IOException("Wrong page type " + type);
                }

                Element<Bytes> root = null;
                Element<Bytes> next = null;
                Element<Bytes> current = null;

                while (edis.available() > 0) {

                    final byte[] data = edis.readArray();
                    final Bytes key = data.length == 0 ? null : Bytes.from_array(data);

                    final long page = edis.readVLong();

                    next = new Element<>(key, page);

                    if (root == null) {
                        root = next;
                    } else {
                        current.next = next;
                    }

                    current = next;
                }

                return root;
            }
        }

        @Override
        public long createDataPage(Element<Bytes> root) throws IOException {

            final VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            try (ExtendedDataOutputStream edos = new ExtendedDataOutputStream(bos)) {

                edos.write(NODEDATA_PAGE);

                Element<Bytes> current = root;

                while(current != null) {

                    /* current.key can be null for last inner node element */
                    if (current.key == null) {
                        edos.writeArray(NULL_KEY);
                    } else {
                        edos.writeArray(current.key.data);
                    }

                    edos.writeVLong(current.page);

                    /* current.next is not needed it will be reconstructed */

                    current = current.next;
                }

            }

            long pageId = pageIDGenerator.incrementAndGet();

            dataStorageManager.writeIndexPage(tableSpace, indexName, pageId, bos.getBuffer(), 0, bos.size());

            return pageId;
        }

        @Override
        public long createMetadataPage(BLinkMetadata<Bytes> metadata) throws IOException {

            final VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            try (ExtendedDataOutputStream edos = new ExtendedDataOutputStream(bos)) {
                MetadataSerializer.INSTANCE.serialize(metadata, edos);
            }

            long pageId = pageIDGenerator.incrementAndGet();

            dataStorageManager.writeIndexPage(tableSpace, indexName, pageId, bos.getBuffer(), 0, bos.size());

            return pageId;
        }
    }
}
