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
import herddb.model.DataScanner;
import herddb.model.GetResult;
import herddb.model.PrimaryKeyIndexSeekPredicate;
import herddb.model.TransactionResult;
import herddb.model.Tuple;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.UpdateStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    public void cacheStatement() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            CreateTableStatement st2 = (CreateTableStatement) manager.getTranslator().translate("CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList()).statement;
            manager.executeStatement(st2);

            TranslatedQuery translated_insert = manager.getTranslator().translate("INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234)));
            InsertStatement st_insert = (InsertStatement) translated_insert.statement;
            assertEquals(1, manager.executeUpdate(st_insert, translated_insert.context).getUpdateCount());

            ScanStatement scanFirst = null;
            for (int i = 0; i < 100; i++) {
                TranslatedQuery translate = manager.getTranslator().translate("SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 ='mykey'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate.statement;
                assertTrue(scan.getPredicate() instanceof PrimaryKeyIndexSeekPredicate);
                if (scanFirst == null) {
                    scanFirst = scan;
                } else {
                    assertTrue(scan == scanFirst);
                }
            }

        }
    }

    @Test
    public void insertJdbcParametersTest() throws Exception {
        String  nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            CreateTableStatement st2 = (CreateTableStatement) manager.getTranslator().translate("CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList()).statement;
            manager.executeStatement(st2);

            {
                TranslatedQuery tquery = manager.getTranslator().translate("INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234)));
                InsertStatement st_insert = (InsertStatement) tquery.statement;
                assertEquals(1, manager.executeUpdate(st_insert, tquery.context).getUpdateCount());
            }
            {
                TranslatedQuery tquery = manager.getTranslator().translate("INSERT INTO tblspace1.tsql(n1,k1) values(?,?)", Arrays.asList(Integer.valueOf(1234), "mykey2"));
                InsertStatement st_insert = (InsertStatement) tquery.statement;
                assertEquals(1, manager.executeUpdate(st_insert, tquery.context).getUpdateCount());
            }
            {
                TranslatedQuery tquery = manager.getTranslator().translate("INSERT INTO tblspace1.tsql(n1,k1,s1) values(?,?,?)", Arrays.asList(Integer.valueOf(1234), "mykey3","string2"));
                InsertStatement st_insert = (InsertStatement) tquery.statement;
                assertEquals(1, manager.executeUpdate(st_insert, tquery.context).getUpdateCount());
            }
        }
    }

    @Test
    public void indexSeek() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            CreateTableStatement st2 = (CreateTableStatement) manager.getTranslator().translate("CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList()).statement;
            manager.executeStatement(st2);

            TranslatedQuery tquery = manager.getTranslator().translate("INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234)));
            InsertStatement st_insert = (InsertStatement) tquery.statement;
            assertEquals(1, manager.executeUpdate(st_insert, tquery.context).getUpdateCount());

            {
                TranslatedQuery translated = manager.getTranslator().translate("SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 ='mykey'", Collections.emptyList(), true, true);

                ScanStatement scan = (ScanStatement) translated.statement;
                assertTrue(scan.getPredicate() instanceof PrimaryKeyIndexSeekPredicate);
                try (DataScanner scan1 = manager.scan(scan, translated.context);) {
                    List<Tuple> records = scan1.consume();
                    assertEquals(1, records.size());
                    assertEquals(3, records.get(0).fieldNames.length);
                    assertEquals(3, records.get(0).values.length);
                    assertEquals("theKey", records.get(0).fieldNames[0]);
                    assertEquals("mykey", records.get(0).values[0]);
                    assertEquals("theStringConstant", records.get(0).fieldNames[1]);
                    assertEquals("one", records.get(0).values[1]);
                    assertEquals("LongConstant", records.get(0).fieldNames[2]);
                    assertEquals(Long.valueOf(3), records.get(0).values[2]);
                }
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 = 'mykey_no'", Collections.emptyList(), true, true);

                ScanStatement scan = (ScanStatement) translate1.statement;
                assertTrue(scan.getPredicate() instanceof PrimaryKeyIndexSeekPredicate);
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    assertTrue(scan1.consume().isEmpty());
                }
            }
        }
    }

    @Test
    @SuppressWarnings("empty-statement")
    public void createTable1() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            CreateTableStatement st2 = (CreateTableStatement) manager.getTranslator().translate("CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList()).statement;
            manager.executeStatement(st2);

            TranslatedQuery translate = manager.getTranslator().translate("INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234)));
            InsertStatement st_insert = (InsertStatement) translate.statement;
            assertEquals(1, manager.executeUpdate(st_insert, translate.context).getUpdateCount());

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(1234), finalRecord.get("n1"));
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ?", Arrays.asList(Integer.valueOf(999), "mykey"));
                UpdateStatement st_update = (UpdateStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ? and n1 = ?", Arrays.asList(Integer.valueOf(100), "mykey", Integer.valueOf(999)));;
                UpdateStatement st_update = (UpdateStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ? and (n1 = ? or n1 <> ?)", Arrays.asList(Integer.valueOf(999), "mykey", Integer.valueOf(100), Integer.valueOf(1000)));;
                UpdateStatement st_update = (UpdateStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ? and (n1 <> ?)", Arrays.asList(Integer.valueOf(34), "mykey", Integer.valueOf(15)));
                UpdateStatement st_update = (UpdateStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=? where k1 = ? and not (n1 <> ?)", Arrays.asList(Integer.valueOf(999), "mykey", Integer.valueOf(34)));
                UpdateStatement st_update = (UpdateStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 = ?", Arrays.asList("mykey"));
                GetStatement st_get = (GetStatement) translate1.statement;
                GetResult result = manager.get(st_get, translate1.context);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 999));
                GetStatement st_get_with_condition = (GetStatement) translate1.statement;
                GetResult result = manager.get(st_get_with_condition, translate1.context);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 9992));
                GetStatement st_get_with_wrong_condition = (GetStatement) translate1.statement;
                GetResult result = manager.get(st_get_with_wrong_condition, translate1.context);
                assertFalse(result.found());
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("DELETE FROM tblspace1.tsql where k1 = ? and n1 = ?", Arrays.asList("mykey", 123));
                DeleteStatement st_delete_with_wrong_condition = (DeleteStatement) translate1.statement;
                assertEquals(0, manager.executeUpdate(st_delete_with_wrong_condition, translate1.context).getUpdateCount());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("DELETE FROM tblspace1.tsql where k1 = ?", Arrays.asList("mykey"));
                DeleteStatement st_delete = (DeleteStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_delete, translate1.context).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertFalse(result.found());
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234)));;
                InsertStatement st_insert_2 = (InsertStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_insert_2, translate1.context).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertTrue(result.found());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("DELETE FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 1234));;
                DeleteStatement st_delete_with_condition = (DeleteStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_delete_with_condition, translate1.context).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertFalse(result.found());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("INSERT INTO tblspace1.tsql(k1,n1) values('mykey2',1234)", Collections.emptyList());;
                InsertStatement st_insert_3 = (InsertStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_insert_3, translate1.context).getUpdateCount());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=2135 where k1 = 'mykey2'", Collections.emptyList());;
                UpdateStatement st_update = (UpdateStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=2138,s1='foo' where k1 = 'mykey2' and s1 is null", Collections.emptyList());;
                UpdateStatement st_update = (UpdateStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.statement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    assertEquals(1, scan1.consume().size());
                }

            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT k1 FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.statement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    List<Tuple> records = scan1.consume();
                    assertEquals(1, records.size());
                    assertEquals(1, records.get(0).fieldNames.length);
                    assertEquals(1, records.get(0).values.length);
                    assertEquals("k1", records.get(0).fieldNames[0]);
                    assertEquals("mykey2", records.get(0).values[0]);
                }
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT k1 theKey FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.statement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    List<Tuple> records = scan1.consume();
                    assertEquals(1, records.size());
                    assertEquals(1, records.get(0).fieldNames.length);
                    assertEquals(1, records.get(0).values.length);
                    assertEquals("theKey", records.get(0).fieldNames[0]);
                    assertEquals("mykey2", records.get(0).values[0]);
                }
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.statement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    List<Tuple> records = scan1.consume();
                    assertEquals(1, records.size());
                    assertEquals(3, records.get(0).fieldNames.length);
                    assertEquals(3, records.get(0).values.length);
                    assertEquals("theKey", records.get(0).fieldNames[0]);
                    assertEquals("mykey2", records.get(0).values[0]);
                    assertEquals("theStringConstant", records.get(0).fieldNames[1]);
                    assertEquals("one", records.get(0).values[1]);
                    assertEquals("LongConstant", records.get(0).fieldNames[2]);
                    assertEquals(Long.valueOf(3), records.get(0).values[2]);
                }
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 ='mykey2' and s1 is not null", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.statement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    assertEquals(1, scan1.consume().size());
                }

            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and s1 is not null", Collections.emptyList());;
                UpdateStatement st_update = (UpdateStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and s1 is null", Collections.emptyList());;
                UpdateStatement st_update = (UpdateStatement) translate1.statement;
                assertEquals(0, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("DELETE FROM  tblspace1.tsql where k1 = 'mykey2' and s1 is not null", Collections.emptyList());;
                DeleteStatement st_update = (DeleteStatement) translate1.statement;
                assertEquals(1, manager.executeUpdate(st_update, translate1.context).getUpdateCount());
            }
            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey2"), null));
                assertFalse(result.found());
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("EXECUTE BEGINTRANSACTION 'tblspace1'", Collections.emptyList());;
                BeginTransactionStatement st_begin_transaction = (BeginTransactionStatement) translate1.statement;
                TransactionResult result = (TransactionResult) manager.executeStatement(st_begin_transaction);
                long tx = result.getTransactionId();
                TranslatedQuery translate2 = manager.getTranslator().translate("EXECUTE COMMITTRANSACTION 'tblspace1'," + tx, Collections.emptyList());;
                CommitTransactionStatement st_commit_transaction = (CommitTransactionStatement) translate2.statement;
                manager.executeStatement(st_commit_transaction);
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("EXECUTE BEGINTRANSACTION 'tblspace1'", Collections.emptyList());;
                BeginTransactionStatement st_begin_transaction = (BeginTransactionStatement) translate1.statement;
                TransactionResult result = (TransactionResult) manager.executeStatement(st_begin_transaction);
                long tx = result.getTransactionId();
                TranslatedQuery translate2 = manager.getTranslator().translate("EXECUTE ROLLBACKTRANSACTION 'tblspace1'," + tx, Collections.emptyList());;
                RollbackTransactionStatement st_rollback_transaction = (RollbackTransactionStatement) translate2.statement;
                manager.executeStatement(st_rollback_transaction);
            }
        }

    }
}
