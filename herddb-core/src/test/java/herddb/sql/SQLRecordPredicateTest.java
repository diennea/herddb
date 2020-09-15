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

package herddb.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.DBManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.RawString;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

/**
 * @author enrico.olivelli
 */
public class SQLRecordPredicateTest {

    @Test
    public void testEvaluateExpression() throws Exception {
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            assertTrue(manager.waitForTablespace(TableSpace.DEFAULT, 10000));

            Table table = Table
                    .builder()
                    .name("t1")
                    .column("pk", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("pk")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT,
                        "SELECT * "
                                + "FROM t1 "
                                + "where pk >= ?", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate() instanceof SQLRecordPredicate);
                SQLRecordPredicate pred = (SQLRecordPredicate) scan.getPredicate();

                assertTrue(pred.getPrimaryKeyFilter() != null && (pred.getPrimaryKeyFilter() instanceof CompiledSQLExpression));

                assertTrue(pred.getWhere() != null && (pred.getWhere() instanceof CompiledSQLExpression));

                StatementEvaluationContext ctx = new SQLStatementEvaluationContext("the-query", Arrays.asList("my-string"), false, false);

                Record record = RecordSerializer.makeRecord(table, "pk", "test", "name", "myname");
                assertEquals(Boolean.TRUE, pred.evaluate(record, ctx));

                long start = System.currentTimeMillis();
                int size = 20_000_000;
                for (int i = 0; i < size; i++) {
                    pred.evaluate(record, ctx);
                }
                long end = System.currentTimeMillis();
                double speed = (int) (size * 1000d / (end - start));
                System.out.println("speed: " + speed + " eval/s" + " per " + size + " records");

            }
        }
    }

    @Test
    public void testCast() throws Exception {
        testCast(null, null, ColumnTypes.INTEGER);

        testCast(1, 1L, ColumnTypes.INTEGER);
        testCast(1, 1d, ColumnTypes.INTEGER);
        testCast(1, true, ColumnTypes.INTEGER);
        testCast(0, false, ColumnTypes.INTEGER);
        testCast(-1, "-1", ColumnTypes.INTEGER);
        testCast(-1, RawString.of("-1"), ColumnTypes.INTEGER);

        testCast(1L, 1L, ColumnTypes.LONG);
        testCast(1L, 1d, ColumnTypes.LONG);
        testCast(1L, true, ColumnTypes.LONG);
        testCast(0L, false, ColumnTypes.LONG);
        testCast(-1L, "-1", ColumnTypes.LONG);
        testCast(-1L, RawString.of("-1"), ColumnTypes.LONG);

        testCast(1d, 1L, ColumnTypes.DOUBLE);
        testCast(1d, 1d, ColumnTypes.DOUBLE);
        testCast(1d, true, ColumnTypes.DOUBLE);
        testCast(0d, false, ColumnTypes.DOUBLE);
        testCast(-1d, "-1", ColumnTypes.DOUBLE);
        testCast(-1d, RawString.of("-1"), ColumnTypes.DOUBLE);

        testCast(true, 1L, ColumnTypes.BOOLEAN);
        testCast(true, 1d, ColumnTypes.BOOLEAN);
        testCast(true, true, ColumnTypes.BOOLEAN);
        testCast(false, false, ColumnTypes.BOOLEAN);
        testCast(true, "1", ColumnTypes.BOOLEAN);
        testCast(false, "0", ColumnTypes.BOOLEAN);

        testCast("1", 1L, ColumnTypes.STRING);
        testCast("1.0", 1d, ColumnTypes.STRING);
        testCast("true", true, ColumnTypes.STRING);
        testCast("false", false, ColumnTypes.STRING);
        testCast("-1", "-1", ColumnTypes.STRING);
        testCast(RawString.of("-1"), RawString.of("-1"), ColumnTypes.STRING);

        long now = System.currentTimeMillis();
        testCast(new java.sql.Timestamp(1), 1L, ColumnTypes.TIMESTAMP);
        testCast(new java.sql.Timestamp(now), new java.sql.Timestamp(now), ColumnTypes.TIMESTAMP);
        testCast(new java.sql.Timestamp(now), RawString.of(new java.sql.Timestamp(now).toString()), ColumnTypes.TIMESTAMP);
        testCast(new java.sql.Timestamp(now), new java.sql.Timestamp(now).toString(), ColumnTypes.TIMESTAMP);
    }

    private void testCast(Object expected, Object value, int type) {
        assertEquals(expected, SQLRecordPredicate.cast(value, type));
        // test the same type and the 'NOT NULL' type
        assertEquals(expected, SQLRecordPredicate.cast(value, ColumnTypes.getNonNullTypeForPrimitiveType(type)));
    }
}
