package herddb.index.jvector;

import herddb.codec.RecordSerializer;
import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.PostCheckpointAction;
import herddb.index.IndexOperation;
import herddb.index.SecondaryIndexVectorSimilarityScan;
import herddb.log.CommitLog;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.ExtendedDataOutputStream;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Index based on JVector, only for arrays of floats
 */
public class JVectorIndexManager extends AbstractIndexManager {

    private static final int DIMENSIONS = 5;

    private static final int M = 8;
    private static final int beamWidth = 60;
    private static final float neighborOverflow = 1.2f;
    private static final float alpha = 1.4f;

    private static VectorSimilarityFunction vectorSimilarityFunction = VectorSimilarityFunction.COSINE;
    private static final Logger LOGGER = Logger.getLogger(JVectorIndexManager.class.getName());


    private GraphIndexBuilder<float[]> currentGraphBuilder;

    private GraphSearcher<float[]> graphSearcher;

    LogSequenceNumber bootSequenceNumber;


    private final AtomicLong newPageId = new AtomicLong(1);


    private final RandomAccessVectorValuesImpl nodeToVectorMapping = new RandomAccessVectorValuesImpl();

    public JVectorIndexManager(Index index, AbstractTableManager tableManager,
                               DataStorageManager dataStorageManager, String tableSpaceUUID, CommitLog log,
                               long createdInTransaction, int writeLockTimeout, int readLockTimeout) throws DataStorageManagerException {
        super(index, tableManager, dataStorageManager, tableSpaceUUID, log, createdInTransaction, writeLockTimeout, readLockTimeout);
    }

    @Override
    protected boolean doStart(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        LOGGER.log(Level.FINE, " start BRIN index {0} uuid {1}", new Object[]{index.name, index.uuid});

        dataStorageManager.initIndex(tableSpaceUUID, index.uuid);

        bootSequenceNumber = sequenceNumber;

        if (LogSequenceNumber.START_OF_TIME.equals(sequenceNumber)) {
            /* Empty index (booting from the start) */
            createNewBuilder();
            LOGGER.log(Level.FINE, "loaded empty index {0}", new Object[]{index.name});

            return true;
        } else {

            IndexStatus status;
            try {
                status = dataStorageManager.getIndexStatus(tableSpaceUUID, index.uuid, sequenceNumber);
            } catch (DataStorageManagerException e) {
                LOGGER.log(Level.SEVERE, "cannot load index {0} due to {1}, it will be rebuilt", new Object[]{index.name, e});
                return false;
            }
            newPageId.set(status.newPageId);
            return true;
        }
    }

    private void createNewBuilder() {
        currentGraphBuilder = new GraphIndexBuilder<>(this.nodeToVectorMapping, VectorEncoding.FLOAT32,
                vectorSimilarityFunction, M, beamWidth, neighborOverflow, alpha);
        graphSearcher = new GraphSearcher.Builder<>(currentGraphBuilder.getGraph().getView())
                .withConcurrentUpdates()
                .build();
    }

    @Override
    public void rebuild() throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        LOGGER.log(Level.FINE, "building index {0}", index.name);
        dataStorageManager.initIndex(tableSpaceUUID, index.uuid);
        createNewBuilder();
        Table table = tableManager.getTable();
        AtomicLong count = new AtomicLong();
        tableManager.scanForIndexRebuild(r -> {
            DataAccessor values = r.getDataAccessor(table);
            Bytes key = RecordSerializer.serializeIndexKey(values, table, table.primaryKey);
            Bytes indexKey = RecordSerializer.serializeIndexKey(values, index, index.columnNames);
//            LOGGER.log(Level.SEVERE, "adding " + key + " -> " + values);
            recordInserted(key, indexKey);
            count.incrementAndGet();
        });
        long _stop = System.currentTimeMillis();
        if (count.intValue() > 0) {
            LOGGER.log(Level.INFO, "building index {0} took {1}, scanned {2} records", new Object[]{index.name, (_stop - _start) + " ms", count});
        }
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
        OnHeapGraphIndex<float[]> completed = currentGraphBuilder.build();
        byte[] storedDisk;
        try (ByteArrayOutputStream flush = new ByteArrayOutputStream();
             DataOutputStream dataOutputStream = new ExtendedDataOutputStream(flush)) {
            OnDiskGraphIndex.write(completed, this.nodeToVectorMapping,dataOutputStream );
            dataOutputStream.flush();
            storedDisk = flush.toByteArray();
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        // TODO:

        LOGGER.log(Level.INFO, "Serialized index takes {0} bytes", storedDisk.length);

        return Collections.emptyList();
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {

    }

    @Override
    protected Stream<Bytes> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext) throws StatementExecutionException {
        SecondaryIndexVectorSimilarityScan indexVectorSimilarityScan = (SecondaryIndexVectorSimilarityScan) operation;
        byte[] bytes = indexVectorSimilarityScan.value.computeNewValue(null, context, tableContext);

        float[] targetVector =  Bytes.to_float_array(bytes, 0, bytes.length);
        int topK = indexVectorSimilarityScan.topK;

        NeighborSimilarity.ExactScoreFunction scoreFunction = (i) -> {
            return vectorSimilarityFunction.compare(targetVector,
                    this.nodeToVectorMapping.nodeIdToVector.get(i).to_float_array());
        };
        SearchResult search = graphSearcher.search(scoreFunction, null, topK, null);

        List<Bytes> result = new ArrayList<>();
        for (SearchResult.NodeScore node : search.getNodes()) {
            int nodeId = node.node;
            Bytes primaryKey = this.nodeToVectorMapping.nodeIdToKey.get(nodeId);
            result.add(primaryKey);
        }
        return result.stream();

    }

    @Override
    public void recordUpdated(Bytes key, Bytes indexKeyRemoved, Bytes indexKeyAdded) throws DataStorageManagerException {
        throw new DataStorageManagerException("Update not supported");
    }

    @Override
    public void recordInserted(Bytes key, Bytes indexKey) throws DataStorageManagerException {
        int nodeId = nodeToVectorMapping.registerRecord(key, indexKey);
        float[] floatArray = indexKey.to_float_array();
        LOGGER.log(Level.INFO, "Adding {0} as node id {1}", new Object[]{Arrays.toString(floatArray), nodeId});
        currentGraphBuilder.addGraphNode(nodeId, nodeToVectorMapping);
    }

    @Override
    public void recordDeleted(Bytes key, Bytes indexKey) throws DataStorageManagerException {
        throw new DataStorageManagerException("Delete not supported");
    }

    @Override
    public void truncate() throws DataStorageManagerException {
        throw new DataStorageManagerException("TRUNCATE not supported");
    }

    @Override
    public boolean valueAlreadyMapped(Bytes key, Bytes primaryKey) throws DataStorageManagerException {
        // this method is for UNIQUE indexes
        return false;
    }

    private static class RandomAccessVectorValuesImpl implements RandomAccessVectorValues<float[]> {
        private AtomicInteger nextNodeId = new AtomicInteger(1);

        ConcurrentHashMap<Integer, Bytes> nodeIdToVector = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Bytes> nodeIdToKey = new ConcurrentHashMap<>();

        public int registerRecord(Bytes primaryKey, Bytes vectorValue) {
           Integer newId = nextNodeId.incrementAndGet();
           nodeIdToVector.put(newId, vectorValue);
           nodeIdToKey.put(newId, primaryKey);
           return newId;
        }

        @Override
        public int size() {
            return nodeIdToVector.size();
        }

        @Override
        public int dimension() {
            return DIMENSIONS;
        }

        @Override
        public float[] vectorValue(int i) {
            Bytes bytes =  nodeIdToVector.get(i);
            return bytes != null ? bytes.to_float_array() : null;
        }

        @Override
        public boolean isValueShared() {
            return true;
        }

        @Override
        public RandomAccessVectorValues<float[]> copy() {
            return this;
        }
    }
}
