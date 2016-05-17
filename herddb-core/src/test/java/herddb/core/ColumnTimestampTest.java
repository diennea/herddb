package herddb.core;

import herddb.codec.RecordSerializer;
import herddb.model.ColumnTypes;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class ColumnTimestampTest extends BaseTestcase {

    @Test
    public void test2() throws Exception {

        String tableName2 = "t2";
        Table table2 = Table
                .builder()
                .tablespace("tblspace1")
                .name(tableName2)
                .tablespace(tableSpace)
                .column("id", ColumnTypes.STRING)
                .column("name", ColumnTypes.STRING)
                .column("ts1", ColumnTypes.TIMESTAMP)
                .primaryKey("id")
                .build();

        CreateTableStatement st2 = new CreateTableStatement(table2);
        manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        java.sql.Timestamp ts1 = new java.sql.Timestamp(System.currentTimeMillis());
        {
            Map<String, Object> bean = new HashMap<>();
            bean.put("id", "key1");
            bean.put("ts1", ts1);
            Record record = RecordSerializer.toRecord(bean, table2);
            InsertStatement st = new InsertStatement(tableSpace, "t2", record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        {
            GetResult result = manager.get(new GetStatement(tableSpace, tableName2, Bytes.from_string("key1"), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
            Map<String, Object> resultbean = RecordSerializer.toBean(result.getRecord(), table2);
            assertEquals(Bytes.from_string("key1"), result.getRecord().key);
            assertEquals(2, resultbean.entrySet().size());
            assertEquals("key1", resultbean.get("id"));
            assertEquals(ts1, resultbean.get("ts1"));
        }

        {
            Map<String, Object> bean = new HashMap<>();
            bean.put("id", "key1");
            bean.put("ts1", null);
            Record record = RecordSerializer.toRecord(bean, table2);
            UpdateStatement st = new UpdateStatement(tableSpace, tableName2, record, null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        {
            GetResult result = manager.get(new GetStatement(tableSpace, tableName2, Bytes.from_string("key1"), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
            Map<String, Object> resultbean = RecordSerializer.toBean(result.getRecord(), table2);
            assertEquals(Bytes.from_string("key1"), result.getRecord().key);
            assertEquals(1, resultbean.entrySet().size());
            assertEquals("key1", resultbean.get("id"));
            assertEquals(null, resultbean.get("ts1"));
        }
    }
}
