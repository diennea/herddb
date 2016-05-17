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

import herddb.model.ColumnTypes;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableDoesNotExistException;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Basic transaction tests
 *
 * @author enrico.olivelli
 */
public class SimpleTransactionTest extends BaseTestcase {

    @Test
    public void testCommit() throws Exception {

        long tx = beginTransaction();

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }
        commitTransaction(tx);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue(get.found());
    }

    private void commitTransaction(long tx) throws Exception {
        manager.executeStatement(new CommitTransactionStatement(tableSpace, tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
    }

    private long beginTransaction() throws Exception {
        return ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION)).getTransactionId();
    }

    @Test
    public void testRollbackInsert() throws Exception {

        long tx = beginTransaction();

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }
        rollbackTransaction(tx);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertFalse(get.found());
    }

    @Test
    public void testRollbackDelete1() throws Exception {

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        long tx = beginTransaction();
        DeleteStatement st = new DeleteStatement(tableSpace, tableName, key, null);
        assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        rollbackTransaction(tx);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue(get.found());
    }

    @Test
    public void testRollbackDelete2() throws Exception {

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        long tx = beginTransaction();
        DeleteStatement st = new DeleteStatement(tableSpace, tableName, key, null);
        assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        // inside the transaction the record will not be found any more
        {
            GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            assertFalse(get.found());
        }

        rollbackTransaction(tx);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue(get.found());
    }

    @Test
    public void testRollbackUpdate1() throws Exception {

        Bytes key = Bytes.from_string("key1");

        Record record = new Record(key, Bytes.from_int(0));
        InsertStatement st_insert = new InsertStatement(tableSpace, tableName, record);
        assertEquals(1, manager.executeUpdate(st_insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());

        long tx = beginTransaction();
        Record record2 = new Record(key, Bytes.from_int(1));
        UpdateStatement st_update = new UpdateStatement(tableSpace, tableName, record2, null);
        assertEquals(1, manager.executeUpdate(st_update, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        rollbackTransaction(tx);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue(get.found());
        assertEquals(get.getRecord().value, record.value);
    }

    @Test
    public void testRollbackUpdate2() throws Exception {

        Bytes key = Bytes.from_string("key1");

        Record record = new Record(key, Bytes.from_int(0));
        InsertStatement st_insert = new InsertStatement(tableSpace, tableName, record);
        assertEquals(1, manager.executeUpdate(st_insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());

        long tx = beginTransaction();
        Record record2 = new Record(key, Bytes.from_int(1));
        UpdateStatement st_update = new UpdateStatement(tableSpace, tableName, record2, null);
        assertEquals(1, manager.executeUpdate(st_update, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        GetResult get_before_rollback = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
        assertTrue(get_before_rollback.found());
        assertEquals(get_before_rollback.getRecord().value, record2.value);

        rollbackTransaction(tx);

        GetResult get_after_rollback = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue(get_after_rollback.found());
        assertEquals(get_after_rollback.getRecord().value, record.value);
    }

    @Test
    public void testRollbackInsertUpdateDelete() throws Exception {

        Bytes key = Bytes.from_string("key1");

        Record record = new Record(key, Bytes.from_int(0));
        InsertStatement st_insert = new InsertStatement(tableSpace, tableName, record);
        assertEquals(1, manager.executeUpdate(st_insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());

        long tx = beginTransaction();
        Record record2 = new Record(key, Bytes.from_int(1));

        // UPDATE
        UpdateStatement st_update = new UpdateStatement(tableSpace, tableName, record2, null);
        assertEquals(1, manager.executeUpdate(st_update, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        // DELETE
        DeleteStatement st_delete = new DeleteStatement(tableSpace, tableName, key, null);
        assertEquals(1, manager.executeUpdate(st_delete, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        rollbackTransaction(tx);

        GetResult get_after_rollback = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue(get_after_rollback.found());
        assertEquals(get_after_rollback.getRecord().value, record.value);
    }

    @Test
    public void testRollbackInsertDelete() throws Exception {

        long tx = beginTransaction();
        Bytes key = Bytes.from_string("key1");

        Record record = new Record(key, Bytes.from_int(0));
        InsertStatement st_insert = new InsertStatement(tableSpace, tableName, record);
        assertEquals(1, manager.executeUpdate(st_insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        // DELETE RETURNS update-count = 1 during the transaction
        DeleteStatement st_delete = new DeleteStatement(tableSpace, tableName, key, null);
        assertEquals(1, manager.executeUpdate(st_delete, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        rollbackTransaction(tx);

        GetResult get_after_rollback = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertFalse(get_after_rollback.found());
    }

    private void rollbackTransaction(long tx) throws Exception {
        manager.executeStatement(new RollbackTransactionStatement(tableSpace, tx), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
    }

    @Test
    public void testCommitMultiTable() throws Exception {

        String tableName2 = "t2";
        Table table2 = Table
                .builder()
                .tablespace("tblspace1")
                .name(tableName2)
                .column("id", ColumnTypes.STRING)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

        CreateTableStatement st2 = new CreateTableStatement(table2);
        manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        long tx = beginTransaction();

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }
        {
            Record record = new Record(key, Bytes.from_int(1));
            InsertStatement st = new InsertStatement(tableSpace, tableName2, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }
        commitTransaction(tx);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue(get.found());
        assertEquals(Bytes.from_int(0), get.getRecord().value);

        GetResult get2 = manager.get(new GetStatement(tableSpace, tableName2, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue(get2.found());
        assertEquals(Bytes.from_int(1), get2.getRecord().value);
    }

    @Test
    public void rollbackCreateTable() throws Exception {

        Bytes key = Bytes.from_int(1234);
        Bytes value = Bytes.from_long(8888);

        CreateTableStatement st2 = new CreateTableStatement(table);
        manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        manager.flush();

        Table transacted_table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t2")
                .column("id", ColumnTypes.STRING)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

        long tx = beginTransaction();
        CreateTableStatement st_create = new CreateTableStatement(transacted_table);
        manager.executeStatement(st_create, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));

        InsertStatement insert = new InsertStatement("tblspace1", "t2", new Record(key, value));
        assertEquals(1, manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());

        GetStatement get = new GetStatement("tblspace1", "t2", key, null);
        GetResult result = manager.get(get, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
        assertTrue(result.found());
        assertEquals(key, result.getRecord().key);
        assertEquals(value, result.getRecord().value);

        RollbackTransactionStatement rollback = new RollbackTransactionStatement("tblspace1", tx);
        manager.executeStatement(rollback, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        try {
            manager.get(new GetStatement("tblspace1", "t2", key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            fail();
        } catch (TableDoesNotExistException error) {
        }

    }

    @Test
    public void testInsertDelete() throws Exception {

        Bytes key = Bytes.from_string("key1");

        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        long tx = beginTransaction();
        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, key, null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }
        {
            Record record = new Record(key, Bytes.from_int(1));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }

        commitTransaction(tx);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertTrue(get.found());
        assertEquals(Bytes.from_int(1), get.getRecord().value);

    }

    @Test
    public void testInsertThenDelete() throws Exception {

        Bytes key = Bytes.from_string("key1");

        long tx = beginTransaction();
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }
        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, key, null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount());
        }

        commitTransaction(tx);

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        assertFalse(get.found());

    }

    @Test
    public void testInsertInsert() throws Exception {

        Bytes key = Bytes.from_string("key1");

        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        long tx = beginTransaction();
        {
            Record record = new Record(key, Bytes.from_int(1));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            try {
                manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx)).getUpdateCount();
                fail();
            } catch (DuplicatePrimaryKeyException expected) {
            }
        }

    }

}
