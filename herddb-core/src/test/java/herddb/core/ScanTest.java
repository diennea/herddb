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

package herddb.core;

import static org.junit.Assert.assertEquals;
import herddb.codec.RecordSerializer;
import herddb.model.DataScanner;
import herddb.model.Predicate;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.utils.Bytes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * @author enrico.olivelli
 */
public class ScanTest extends BaseTestcase {

    @Test
    public void test() throws Exception {

        for (int i = 0; i < 100; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("id", "key_" + i);
            data.put("number", i);
            Record record = RecordSerializer.toRecord(data, table);
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        {
            ScanStatement scan = new ScanStatement(tableSpace, table, new Predicate() {
                @Override
                public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
                    int value = (Integer) record.toBean(table).get("number");
                    return value >= 50;
                }
            });
            DataScanner scanner = manager.scan(scan, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            List<?> result = scanner.consumeAndClose();
            assertEquals(50, result.size());
        }

        for (int i = 0; i < 20; i++) {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, Bytes.from_string("key_" + i), null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        {
            ScanStatement scan = new ScanStatement(tableSpace, table, new Predicate() {
                @Override
                public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
                    int value = (Integer) record.toBean(table).get("number");
                    return value < 50;
                }
            });
            DataScanner scanner = manager.scan(scan, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            List<?> result = scanner.consumeAndClose();
            assertEquals(30, result.size());
        }

        {
            ScanStatement scan = new ScanStatement(tableSpace, table, null);
            DataScanner scanner = manager.scan(scan, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            List<?> result = scanner.consumeAndClose();
            assertEquals(80, result.size());
        }
    }

    @Test
    public void testTransaction() throws Exception {

        for (int i = 0; i < 5; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("id", "key_" + i);
            data.put("number", i);
            Record record = RecordSerializer.toRecord(data, table);
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        BeginTransactionStatement begin = new BeginTransactionStatement(tableSpace);
        long tx = ((TransactionResult) manager.executeStatement(begin, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();

        for (int i = 5; i < 10; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("id", "key_" + i);
            data.put("number", i);
            Record record = RecordSerializer.toRecord(data, table);
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }

        {
            ScanStatement scan = new ScanStatement(tableSpace, table, new Predicate() {
                @Override
                public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
                    return true;
                }
            });
            DataScanner scanner = manager.scan(scan, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            List<?> result = scanner.consumeAndClose();
            assertEquals(10, result.size());
        }

        for (int i = 0; i < 10; i++) {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, Bytes.from_string("key_" + i), null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }

        {
            ScanStatement scan = new ScanStatement(tableSpace, table, new Predicate() {
                @Override
                public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
                    return true;
                }
            });
            DataScanner scanner = manager.scan(scan, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            List<?> result = scanner.consumeAndClose();
            assertEquals(0, result.size());
        }

    }
}
