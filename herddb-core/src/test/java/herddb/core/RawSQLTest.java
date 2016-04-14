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

import herddb.codec.RecordSerializer;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.GetResult;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class RawSQLTest {

    @Test
    public void createTable1() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            CreateTableStatement st2 = (CreateTableStatement) manager.getTranslator().translate("CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            manager.executeStatement(st2);

            InsertStatement st_insert = (InsertStatement) manager.getTranslator().translate("INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234)));
            assertEquals(1, manager.executeUpdate(st_insert).getUpdateCount());

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(1234), finalRecord.get("n1"));
            }

            {
                UpdateStatement st_update = (UpdateStatement) manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ?", Arrays.asList(Integer.valueOf(999), "mykey"));
                assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());
            }

            {
                UpdateStatement st_update = (UpdateStatement) manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ? and n1 = ?", Arrays.asList(Integer.valueOf(100), "mykey", Integer.valueOf(999)));
                assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());
            }

            {
                UpdateStatement st_update = (UpdateStatement) manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ? and (n1 = ? or n1 <> ?)", Arrays.asList(Integer.valueOf(999), "mykey", Integer.valueOf(100), Integer.valueOf(1000)));
                assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());
            }

            {
                UpdateStatement st_update = (UpdateStatement) manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ? and (n1 <> ?)", Arrays.asList(Integer.valueOf(34), "mykey", Integer.valueOf(15)));
                assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());
            }

            {
                UpdateStatement st_update = (UpdateStatement) manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ? and not (n1 <> ?)", Arrays.asList(Integer.valueOf(999), "mykey", Integer.valueOf(34)));
                assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            GetStatement st_get = (GetStatement) manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 = ?", Arrays.asList("mykey"));
            {
                GetResult result = manager.get(st_get);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            GetStatement st_get_with_condition = (GetStatement) manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 999));
            {
                GetResult result = manager.get(st_get_with_condition);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            GetStatement st_get_with_wrong_condition = (GetStatement) manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 9992));
            {
                GetResult result = manager.get(st_get_with_wrong_condition);
                assertFalse(result.found());
            }

            DeleteStatement st_delete_with_wrong_condition = (DeleteStatement) manager.getTranslator().translate("DELETE FROM tblspace1.tsql where k1 = ? and n1 = ?", Arrays.asList("mykey", 123));
            assertEquals(0, manager.executeUpdate(st_delete_with_wrong_condition).getUpdateCount());

            DeleteStatement st_delete = (DeleteStatement) manager.getTranslator().translate("DELETE FROM tblspace1.tsql where k1 = ?", Arrays.asList("mykey"));
            assertEquals(1, manager.executeUpdate(st_delete).getUpdateCount());

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertFalse(result.found());
            }

            InsertStatement st_insert_2 = (InsertStatement) manager.getTranslator().translate("INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234)));
            assertEquals(1, manager.executeUpdate(st_insert_2).getUpdateCount());

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertTrue(result.found());
            }

            DeleteStatement st_delete_with_condition = (DeleteStatement) manager.getTranslator().translate("DELETE FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 1234));
            assertEquals(1, manager.executeUpdate(st_delete_with_condition).getUpdateCount());

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertFalse(result.found());
            }

            InsertStatement st_insert_3 = (InsertStatement) manager.getTranslator().translate("INSERT INTO tblspace1.tsql(k1,n1) values('mykey2',1234)", Collections.emptyList());
            assertEquals(1, manager.executeUpdate(st_insert_3).getUpdateCount());

            {
                UpdateStatement st_update = (UpdateStatement) manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=2135 where k1 = 'mykey2'", Collections.emptyList());
                assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());
            }

            {
                UpdateStatement st_update = (UpdateStatement) manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=2138,s1='foo' where k1 = 'mykey2' and s1 is null", Collections.emptyList());
                assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());
            }
            {
                UpdateStatement st_update = (UpdateStatement) manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and s1 is not null", Collections.emptyList());
                assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());
            }
            {
                UpdateStatement st_update = (UpdateStatement) manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and s1 is null", Collections.emptyList());
                assertEquals(0, manager.executeUpdate(st_update).getUpdateCount());
            }
            {
                DeleteStatement st_update = (DeleteStatement) manager.getTranslator().translate("DELETE FROM  tblspace1.tsql where k1 = 'mykey2' and s1 is not null", Collections.emptyList());
                assertEquals(1, manager.executeUpdate(st_update).getUpdateCount());
            }
            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey2"), null));
                assertFalse(result.found());
            }
            {
                BeginTransactionStatement st_begin_transaction = (BeginTransactionStatement) manager.getTranslator().translate("EXECUTE BEGINTRANSACTION 'tblspace1'", Collections.emptyList());
                TransactionResult result = (TransactionResult) manager.executeStatement(st_begin_transaction);
                long tx = result.getTransactionId();
                CommitTransactionStatement st_commit_transaction = (CommitTransactionStatement) manager.getTranslator().translate("EXECUTE COMMITTRANSACTION 'tblspace1',"+tx, Collections.emptyList());
                manager.executeStatement(st_commit_transaction);                
            }
            {
                BeginTransactionStatement st_begin_transaction = (BeginTransactionStatement) manager.getTranslator().translate("EXECUTE BEGINTRANSACTION 'tblspace1'", Collections.emptyList());
                TransactionResult result = (TransactionResult) manager.executeStatement(st_begin_transaction);
                long tx = result.getTransactionId();
                RollbackTransactionStatement st_rollback_transaction = (RollbackTransactionStatement) manager.getTranslator().translate("EXECUTE ROLLBACKTRANSACTION 'tblspace1',"+tx, Collections.emptyList());
                manager.executeStatement(st_rollback_transaction);                
            }
        }

    }
}
