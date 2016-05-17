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

import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Basic transaction tests
 *
 * @author enrico.olivelli
 */
public class SimpleTransactionTest extends BaseTestcase {

    @Test
    public void testCommit() throws Exception {

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record)
                    .setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        manager.executeStatement(new CommitTransactionStatement(tableSpace, tx));

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertTrue(get.found());
    }

    @Test
    public void testRollbackInsert() throws Exception {

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record)
                    .setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        manager.executeStatement(new RollbackTransactionStatement(tableSpace, tx));

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertFalse(get.found());
    }

    @Test
    public void testRollbackDelete1() throws Exception {

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();
        DeleteStatement st = new DeleteStatement(tableSpace, tableName, key, null)
                .setTransactionId(tx);
        assertEquals(1, manager.executeUpdate(st).getUpdateCount());

        manager.executeStatement(new RollbackTransactionStatement(tableSpace, tx));

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertTrue(get.found());
    }

    @Test
    public void testRollbackDelete2() throws Exception {

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();
        DeleteStatement st = new DeleteStatement(tableSpace, tableName, key, null)
                .setTransactionId(tx);
        assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        // inside the transaction the record will not be found any more
        {
            GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null).setTransactionId(tx));
            assertFalse(get.found());
        }

        manager.executeStatement(new RollbackTransactionStatement(tableSpace, tx));

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertTrue(get.found());
    }

    @Test
    public void testRollbackUpdate1() throws Exception {

        Bytes key = Bytes.from_string("key1");

        Record record = new Record(key, Bytes.from_int(0));
        InsertStatement st_insert = new InsertStatement(tableSpace, tableName, record);
        assertEquals(1, manager.executeUpdate(st_insert).getUpdateCount());

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();
        Record record2 = new Record(key, Bytes.from_int(1));
        UpdateStatement st_update = new UpdateStatement(tableSpace, tableName, record2, null)
                .setTransactionId(tx);
        assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());

        manager.executeStatement(new RollbackTransactionStatement(tableSpace, tx));

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertTrue(get.found());
        assertEquals(get.getRecord().value, record.value);
    }

    @Test
    public void testRollbackUpdate2() throws Exception {

        Bytes key = Bytes.from_string("key1");

        Record record = new Record(key, Bytes.from_int(0));
        InsertStatement st_insert = new InsertStatement(tableSpace, tableName, record);
        assertEquals(1, manager.executeUpdate(st_insert).getUpdateCount());

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();
        Record record2 = new Record(key, Bytes.from_int(1));
        UpdateStatement st_update = new UpdateStatement(tableSpace, tableName, record2, null)
                .setTransactionId(tx);
        assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());

        GetResult get_before_rollback = manager.get(new GetStatement(tableSpace, tableName, key, null).setTransactionId(tx));
        assertTrue(get_before_rollback.found());
        assertEquals(get_before_rollback.getRecord().value, record2.value);

        manager.executeStatement(new RollbackTransactionStatement(tableSpace, tx));

        GetResult get_after_rollback = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertTrue(get_after_rollback.found());
        assertEquals(get_after_rollback.getRecord().value, record.value);
    }

    @Test
    public void testRollbackInsertUpdateDelete() throws Exception {

        Bytes key = Bytes.from_string("key1");

        Record record = new Record(key, Bytes.from_int(0));
        InsertStatement st_insert = new InsertStatement(tableSpace, tableName, record);
        assertEquals(1, manager.executeUpdate(st_insert).getUpdateCount());

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();
        Record record2 = new Record(key, Bytes.from_int(1));

        // UPDATE
        UpdateStatement st_update = new UpdateStatement(tableSpace, tableName, record2, null)
                .setTransactionId(tx);
        assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());

        // DELETE
        DeleteStatement st_delete = new DeleteStatement(tableSpace, tableName, key, null)
                .setTransactionId(tx);
        assertEquals(1, manager.executeUpdate(st_delete).getUpdateCount());

        manager.executeStatement(new RollbackTransactionStatement(tableSpace, tx));

        GetResult get_after_rollback = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertTrue(get_after_rollback.found());
        assertEquals(get_after_rollback.getRecord().value, record.value);
    }

    @Test
    public void testRollbackInsertDelete() throws Exception {

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();
        Bytes key = Bytes.from_string("key1");

        Record record = new Record(key, Bytes.from_int(0));
        InsertStatement st_insert = new InsertStatement(tableSpace, tableName, record).setTransactionId(tx);
        assertEquals(1, manager.executeUpdate(st_insert).getUpdateCount());

        // DELETE RETURNS update-count = 1 during the transaction
        DeleteStatement st_delete = new DeleteStatement(tableSpace, tableName, key, null)
                .setTransactionId(tx);
        assertEquals(1, manager.executeUpdate(st_delete).getUpdateCount());

        manager.executeStatement(new RollbackTransactionStatement(tableSpace, tx));

        GetResult get_after_rollback = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertFalse(get_after_rollback.found());
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
        manager.executeStatement(st2);

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();

        Bytes key = Bytes.from_string("key1");
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record)
                    .setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        {
            Record record = new Record(key, Bytes.from_int(1));
            InsertStatement st = new InsertStatement(tableSpace, tableName2, record)
                    .setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        manager.executeStatement(new CommitTransactionStatement(tableSpace, tx));

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertTrue(get.found());
        assertEquals(Bytes.from_int(0), get.getRecord().value);

        GetResult get2 = manager.get(new GetStatement(tableSpace, tableName2, key, null));
        assertTrue(get2.found());
        assertEquals(Bytes.from_int(1), get2.getRecord().value);
    }

    @Test
    public void rollbackCreateTable() throws Exception {

        Bytes key = Bytes.from_int(1234);
        Bytes value = Bytes.from_long(8888);

        CreateTableStatement st2 = new CreateTableStatement(table);
        manager.executeStatement(st2);
        manager.flush();

        Table transacted_table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t2")
                .column("id", ColumnTypes.STRING)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement("tblspace1"))).getTransactionId();
        CreateTableStatement st_create = new CreateTableStatement(transacted_table).setTransactionId(tx);
        manager.executeStatement(st_create);

        InsertStatement insert = new InsertStatement("tblspace1", "t2", new Record(key, value)).setTransactionId(tx);
        assertEquals(1, manager.executeUpdate(insert).getUpdateCount());

        GetStatement get = new GetStatement("tblspace1", "t2", key, null).setTransactionId(tx);
        GetResult result = manager.get(get);
        assertTrue(result.found());
        assertEquals(key, result.getRecord().key);
        assertEquals(value, result.getRecord().value);

        RollbackTransactionStatement rollback = new RollbackTransactionStatement("tblspace1", tx);
        manager.executeStatement(rollback);
        try {
            manager.get(new GetStatement("tblspace1", "t2", key, null));
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
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();
        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, key, null).setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        {
            Record record = new Record(key, Bytes.from_int(1));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record).setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }

        manager.executeStatement(new CommitTransactionStatement(tableSpace, tx));

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertTrue(get.found());
        assertEquals(Bytes.from_int(1), get.getRecord().value);

    }

    @Test
    public void testInsertThenDelete() throws Exception {

        Bytes key = Bytes.from_string("key1");

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();
        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record).setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, key, null).setTransactionId(tx);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }

        manager.executeStatement(new CommitTransactionStatement(tableSpace, tx));

        GetResult get = manager.get(new GetStatement(tableSpace, tableName, key, null));
        assertFalse(get.found());

    }

    @Test
    public void testInsertInsert() throws Exception {

        Bytes key = Bytes.from_string("key1");

        {
            Record record = new Record(key, Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }

        long tx = ((TransactionResult) manager.executeStatement(new BeginTransactionStatement(tableSpace))).getTransactionId();
        {
            Record record = new Record(key, Bytes.from_int(1));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record).setTransactionId(tx);
            try {
                manager.executeUpdate(st).getUpdateCount();
                fail();
            } catch (DuplicatePrimaryKeyException expected) {
            }
        }

    }

}
