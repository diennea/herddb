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
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.GetResult;
import herddb.model.PrimaryKeyIndexSeekPredicate;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionResult;
import herddb.model.Tuple;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.ScanStatement;
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

    private DMLStatementExecutionResult executeUpdate(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters);
        return (DMLStatementExecutionResult) manager.executePlan(translated.plan, translated.context);
    }

    private StatementExecutionResult execute(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters);
        return manager.executePlan(translated.plan, translated.context);
    }

    @Test
    public void cacheStatement() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            ScanStatement scanFirst = null;
            for (int i = 0; i < 100; i++) {
                TranslatedQuery translate = manager.getTranslator().translate("SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 ='mykey'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate.plan.mainStatement;
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
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            }
            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(n1,k1) values(?,?)", Arrays.asList(Integer.valueOf(1234), "mykey2")).getUpdateCount());
            }
            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(n1,k1,s1) values(?,?,?)", Arrays.asList(Integer.valueOf(1234), "mykey3", "string2")).getUpdateCount());
            }
        }
    }

    @Test
    public void orderByTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(3))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT k1 FROM tblspace1.tsql order by n1", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    List<Tuple> result = scan1.consume();
                    assertEquals(4, result.size());
                    assertEquals("mykey", result.get(0).get(0));
                    assertEquals("mykey2", result.get(1).get(0));
                    assertEquals("mykey3", result.get(2).get(0));
                    assertEquals("mykey4", result.get(3).get(0)); // NULLS LAST
                }
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT k1 FROM tblspace1.tsql order by n1 desc", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    List<Tuple> result = scan1.consume();
                    assertEquals(4, result.size());
                    assertEquals("mykey", result.get(3).get(0));
                    assertEquals("mykey2", result.get(2).get(0));
                    assertEquals("mykey3", result.get(1).get(0));
                    assertEquals("mykey4", result.get(0).get(0));
                }
            }
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey5", Integer.valueOf(3))).getUpdateCount());
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT k1 FROM tblspace1.tsql order by n1, k1", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    List<Tuple> result = scan1.consume();
                    assertEquals(5, result.size());
                    assertEquals("mykey", result.get(0).get(0));
                    assertEquals("mykey2", result.get(1).get(0));
                    assertEquals("mykey3", result.get(2).get(0));
                    assertEquals("mykey5", result.get(3).get(0));
                    assertEquals("mykey4", result.get(4).get(0)); // NULLS LAST
                }
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT k1 FROM tblspace1.tsql order by n1, k1 desc", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    List<Tuple> result = scan1.consume();
                    assertEquals(5, result.size());
                    assertEquals("mykey", result.get(0).get(0));
                    assertEquals("mykey2", result.get(1).get(0));
                    assertEquals("mykey5", result.get(2).get(0));
                    assertEquals("mykey3", result.get(3).get(0));                    
                    assertEquals("mykey4", result.get(4).get(0)); // NULLS LAST
                }
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

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            {
                TranslatedQuery translated = manager.getTranslator().translate("SELECT k1 as theKey,'one' as theStringConstant,3  LongConstant FROM tblspace1.tsql where k1 ='mykey'", Collections.emptyList(), true, true);

                ScanStatement scan = (ScanStatement) translated.plan.mainStatement;
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
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
                assertTrue(scan.getPredicate() instanceof PrimaryKeyIndexSeekPredicate);
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    assertTrue(scan1.consume().isEmpty());
                }
            }
        }
    }

    @Test
    @SuppressWarnings("empty-statement")
    public void basicTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(1234), finalRecord.get("n1"));
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ?", Arrays.asList(Integer.valueOf(999), "mykey")).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ? and n1 = ?", Arrays.asList(Integer.valueOf(100), "mykey", Integer.valueOf(999))).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ? and (n1 = ? or n1 <> ?)", Arrays.asList(Integer.valueOf(999), "mykey", Integer.valueOf(100), Integer.valueOf(1000))).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ? and (n1 <> ?)", Arrays.asList(Integer.valueOf(34), "mykey", Integer.valueOf(15))).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1 = ? and not (n1 <> ?)", Arrays.asList(Integer.valueOf(999), "mykey", Integer.valueOf(34))).getUpdateCount());
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
                GetStatement st_get = (GetStatement) translate1.plan.mainStatement;
                GetResult result = manager.get(st_get, translate1.context);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 999));
                GetStatement st_get_with_condition = (GetStatement) translate1.plan.mainStatement;
                GetResult result = manager.get(st_get_with_condition, translate1.context);
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("mykey"));
                Map<String, Object> finalRecord = RecordSerializer.toBean(result.getRecord(), manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable());
                assertEquals("mykey", finalRecord.get("k1"));
                assertEquals(Integer.valueOf(999), finalRecord.get("n1"));
            }

            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 9992));
                GetStatement st_get_with_wrong_condition = (GetStatement) translate1.plan.mainStatement;
                GetResult result = manager.get(st_get_with_wrong_condition, translate1.context);
                assertFalse(result.found());
            }
            {
                assertEquals(0, executeUpdate(manager, "DELETE FROM tblspace1.tsql where k1 = ? and n1 = ?", Arrays.asList("mykey", 123)).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql where k1 = ?", Arrays.asList("mykey")).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertFalse(result.found());
            }
            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertTrue(result.found());
            }

            {
                assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql where k1 = ? and n1=?", Arrays.asList("mykey", 1234)).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey"), null));
                assertFalse(result.found());
            }

            {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values('mykey2',1234)", Collections.emptyList()).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2135 where k1 = 'mykey2'", Collections.emptyList()).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2138,s1='foo' where k1 = 'mykey2' and s1 is null", Collections.emptyList()).getUpdateCount());
            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT * FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    assertEquals(1, scan1.consume().size());
                }

            }
            {
                TranslatedQuery translate1 = manager.getTranslator().translate("SELECT k1 FROM tblspace1.tsql where k1 ='mykey2'", Collections.emptyList(), true, true);
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
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
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
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
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
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
                ScanStatement scan = (ScanStatement) translate1.plan.mainStatement;
                try (DataScanner scan1 = manager.scan(scan, translate1.context);) {
                    assertEquals(1, scan1.consume().size());
                }

            }
            {
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and s1 is not null", Collections.emptyList()).getUpdateCount());
            }
            {
                assertEquals(0, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=2138,s1='bar' where k1 = 'mykey2' and s1 is null", Collections.emptyList()).getUpdateCount());
            }

            {
                assertEquals(1, executeUpdate(manager, "DELETE FROM  tblspace1.tsql where k1 = 'mykey2' and s1 is not null", Collections.emptyList()).getUpdateCount());
                assertEquals(0, executeUpdate(manager, "DELETE FROM  tblspace1.tsql where k1 = 'mykey2' and s1 is not null", Collections.emptyList()).getUpdateCount());
            }
            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("mykey2"), null));
                assertFalse(result.found());
            }

            {
                TransactionResult result = (TransactionResult) execute(manager, "EXECUTE BEGINTRANSACTION 'tblspace1'", Collections.emptyList());
                long tx = result.getTransactionId();
                execute(manager, "EXECUTE COMMITTRANSACTION 'tblspace1'," + tx, Collections.emptyList());;
            }
            {
                TransactionResult result = (TransactionResult) execute(manager, "EXECUTE BEGINTRANSACTION 'tblspace1'", Collections.emptyList());
                long tx = result.getTransactionId();
                execute(manager, "EXECUTE ROLLBACKTRANSACTION 'tblspace1'," + tx, Collections.emptyList());
            }
        }

    }
}
