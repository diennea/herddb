package herddb.index.jvector;

import herddb.core.AbstractTableManager;
import herddb.core.MemoryManager;
import herddb.index.IndexOperation;
import herddb.index.SecondaryIndexVectorSimilarityScan;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.sql.SQLRecordKeyFunction;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static herddb.model.Index.TYPE_JVECTOR;
import static org.junit.Assert.assertEquals;

public class JVectorIndexManagerTest {

    private static final Logger LOGGER = Logger.getLogger(JVectorIndexManagerTest.class.getName());

    @Test
    public void basicBuildAndSearch() {
        String column = "embeddings";
        Table table = Table
                .builder()
                .name("table")
                .column("key", ColumnTypes.INTEGER)
                .column("embeddings", ColumnTypes.FLOATARRAY)
                .primaryKey("key")
                .build();
        Index index = Index
                .builder()
                .onTable(table)
                .type(TYPE_JVECTOR)
                .column(column, ColumnTypes.FLOATARRAY)
                .build();
        AbstractTableManager abstractTableManager = null;
        MemoryDataStorageManager memoryDataStorageManager = new MemoryDataStorageManager();
        JVectorIndexManager indexManager = new JVectorIndexManager(index, abstractTableManager, memoryDataStorageManager, "xxxx", null, -1, 10000, 1000);

        indexManager.start(LogSequenceNumber.START_OF_TIME);
        Bytes vector1 = Bytes.from_float_array(new float[] {1, 2, 3, 4, 5});

        Bytes vectorToSearch = vector1;

        Map<Bytes, Bytes> data = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            Bytes pk = Bytes.from_int(i);
            double angle = i * Math.PI / 100;
            float sin = (float) Math.sin( angle);
            float cos = (float) Math.cos( angle);
            Bytes vector = Bytes.from_float_array(new float[] {0, 0, 0, sin, cos});
            indexManager.recordInserted(pk, vector);

            data.put(pk, vector);
        }
        int topK = 10;
        SQLRecordKeyFunction keyFunction = new DummyConstantValueFunction(table, vectorToSearch);
        IndexOperation indexOperation = new SecondaryIndexVectorSimilarityScan(index.name, column, topK, keyFunction);
        Stream<Bytes> scanner = indexManager.scanner(indexOperation, null, null);
        List<Bytes> collect = scanner.collect(Collectors.toList());
        collect.forEach(k -> {
            Bytes vector = data.get(k);
            float compare = VectorSimilarityFunction.COSINE.compare(vector1.to_float_array(), vector.to_float_array());
            LOGGER.log(Level.INFO, "Found record with key {0} and value {1} compare {2}", new Object[] {k.to_int(), vector.to_float_array(), compare});
        });
        assertEquals(10, collect.size());

    }

    private static class DummyConstantValueFunction extends SQLRecordKeyFunction {
        private final Bytes vectorToSearch;

        public DummyConstantValueFunction(Table table, Bytes vectorToSearch) {
            super(Collections.emptyList(), Collections.emptyList(), table);
            this.vectorToSearch = vectorToSearch;
        }

        @Override
        public byte[] computeNewValue(Record previous,
                                      StatementEvaluationContext context,
                                      TableContext tableContext) throws StatementExecutionException {
            return vectorToSearch.to_array();
        }
    }
}
