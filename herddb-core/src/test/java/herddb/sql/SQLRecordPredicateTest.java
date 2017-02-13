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
import herddb.sql.expressions.InterpretedSQLExpression;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class SQLRecordPredicateTest {

    @Test
    public void testEvaluateExpression() throws Exception {
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null, null);) {
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
                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
                assertTrue(scan.getPredicate() instanceof SQLRecordPredicate);
                SQLRecordPredicate pred = (SQLRecordPredicate) scan.getPredicate();

                assertTrue(pred.getPrimaryKeyFilter() != null && !(pred.getPrimaryKeyFilter() instanceof InterpretedSQLExpression));

                assertTrue(pred.getWhere() != null && !(pred.getWhere() instanceof InterpretedSQLExpression));

                StatementEvaluationContext ctx = new SQLStatementEvaluationContext("the-query", Arrays.asList("my-string"));

                Record record = RecordSerializer.makeRecord(table, "pk", "test", "name", "myname");
                assertEquals(Boolean.TRUE, pred.evaluate(record, ctx));

                long _start = System.currentTimeMillis();
                int SIZE = 20_000_000;
                for (int i = 0; i < SIZE; i++) {
                    pred.evaluate(record, ctx);
                }
                long _end = System.currentTimeMillis();
                double speed = (int) (SIZE * 1000d / (_end - _start));
                System.out.println("speed: " + speed + " eval/s" + " per " + SIZE + " records");

            }
        }
    }

}
