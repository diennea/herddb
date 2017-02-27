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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import herddb.codec.RecordSerializer;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Projection;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.TableDoesNotExistException;
import herddb.model.TransactionContext;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.utils.Bytes;

/**
 *
 *
 * @author enrico.olivelli
 */
public class AutoTransactionTest extends BaseTestcase {

    @Test
    public void testAutoTransactionOnInsert() throws Exception {

        int i = 1;
        Map<String, Object> data = new HashMap<>();
        data.put("id", "key_" + i);
        data.put("number", i);
        Record record = RecordSerializer.toRecord(data, table);
        InsertStatement st = new InsertStatement(tableSpace, tableName, record);
        DMLStatementExecutionResult executeUpdate = manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
        long tx = executeUpdate.transactionId;
        TestUtils.commitTransaction(manager, tableSpace, tx);

    }

    @Test
    public void testAutoTransactionOnFailedInsert() throws Exception {

        int i = 1;
        {
            Map<String, Object> data = new HashMap<>();
            data.put("number", i);
            data.put("id", i);
            Record record = RecordSerializer.toRecord(data, table);
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        }
        try {
            Map<String, Object> data = new HashMap<>();
            data.put("number", i);
            data.put("id", i);
            Record record = RecordSerializer.toRecord(data, table);
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            fail();
        } catch (DuplicatePrimaryKeyException ok) {
        }
        assertTrue(manager.getTableSpaceManager(tableSpace).getOpenTransactions().isEmpty());

    }

    @Test
    public void testAutoTransactionOnGet() throws Exception {

        int i = 1;
        Map<String, Object> data = new HashMap<>();
        Bytes key = Bytes.from_string("key_" + i);
        data.put("id", "key_" + i);
        data.put("number", i);
        Record record = RecordSerializer.toRecord(data, table);
        InsertStatement st = new InsertStatement(tableSpace, tableName, record);
        manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
        long tx = get.transactionId;
        assertTrue(get.found());
        TestUtils.commitTransaction(manager, tableSpace, tx);

    }

    @Test
    public void testAutoTransactionOnGetWithError() throws Exception {

        int i = 1;
        Map<String, Object> data = new HashMap<>();
        Bytes key = Bytes.from_string("key_" + i);
        data.put("id", "key_" + i);
        data.put("number", i);
        Record record = RecordSerializer.toRecord(data, table);
        InsertStatement st = new InsertStatement(tableSpace, tableName, record);
        manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        try {
            GetResult get = manager.get(new GetStatement(tableSpace, "no_table_exists", key, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            fail();
        } catch (TableDoesNotExistException ok) {
        }

        assertTrue(manager.getTableSpaceManager(tableSpace).getOpenTransactions().isEmpty());

    }

    @Test
    public void testAutoTransactionOnFailedGet() throws Exception {
        int i = 1;
        Map<String, Object> data = new HashMap<>();
        Bytes key_bad = Bytes.from_string("key_bad_" + i);
        data.put("id", "key_" + i);
        data.put("number", i);
        Record record = RecordSerializer.toRecord(data, table);
        InsertStatement st = new InsertStatement(tableSpace, tableName, record);
        manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key_bad, null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
        long tx = get.transactionId;
        assertTrue(!get.found());
        TestUtils.commitTransaction(manager, tableSpace, tx);
    }

    @Test
    public void testAutoTransactionOnScan() throws Exception {

        int i = 1;
        Map<String, Object> data = new HashMap<>();
        Bytes key = Bytes.from_string("key_" + i);
        data.put("id", "key_" + i);
        data.put("number", i);
        Record record = RecordSerializer.toRecord(data, table);
        InsertStatement st = new InsertStatement(tableSpace, tableName, record);
        DMLStatementExecutionResult executeUpdate = manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        long tx;
        try (DataScanner scan = manager.scan(new ScanStatement(tableSpace, tableName, Projection.IDENTITY(table.columnNames, table.getColumns()), null, null, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);) {
            tx = scan.transactionId;
            assertEquals(1, scan.consume().size());
        }

        TestUtils.commitTransaction(manager, tableSpace, tx);

    }

    @Test
    public void testAutoTransactionOnErrorScan() throws Exception {

        int i = 1;
        Map<String, Object> data = new HashMap<>();
        Bytes key = Bytes.from_string("key_" + i);
        data.put("id", "key_" + i);
        data.put("number", i);
        Record record = RecordSerializer.toRecord(data, table);
        InsertStatement st = new InsertStatement(tableSpace, tableName, record);
        DMLStatementExecutionResult executeUpdate = manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        long tx;
        try (DataScanner scan = manager.scan(new ScanStatement(tableSpace, "no_table", Projection.IDENTITY(table.columnNames, table.getColumns()), null, null, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);) {
            fail();
        } catch (TableDoesNotExistException ok) {
        }

        assertTrue(manager.getTableSpaceManager(tableSpace).getOpenTransactions().isEmpty());

    }
}
