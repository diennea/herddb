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

import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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

}
