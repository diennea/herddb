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

import herddb.core.DBManager;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class SimplerPlannerTest {

    @Test
    public void plannerBasicStatementsTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(3, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));
                assertEquals(RawString.of("mykey"), results.get(0).get("k1"));
                assertEquals(1234, results.get(0).get(1));
                assertEquals(1234, results.get(0).get("n1"));
            }
            try (DataScanner scan = scan(manager, "SELECT n1,k1 FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(2, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(1));
                assertEquals(RawString.of("mykey"), results.get(0).get("k1"));
                assertEquals(1234, results.get(0).get(0));
                assertEquals(1234, results.get(0).get("n1"));
            }
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(1235))).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT * "
                    + " FROM tblspace1.tsql"
                    + " ORDER BY k1", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(3, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));
                assertEquals(RawString.of("mykey2"), results.get(1).get(0));
                assertEquals(1234, results.get(0).get(1));
                assertEquals(1235, results.get(1).get(1));

            }
            try (DataScanner scan = scan(manager, "SELECT * "
                    + " FROM tblspace1.tsql"
                    + " ORDER BY k1 desc", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(3, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(1).get(0));
                assertEquals(RawString.of("mykey2"), results.get(0).get(0));
                assertEquals(1234, results.get(1).get(1));
                assertEquals(1235, results.get(0).get(1));

            }
            try (DataScanner scan = scan(manager, "SELECT k1,n1 "
                    + " FROM tblspace1.tsql"
                    + " ORDER BY k1 desc", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(2, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(1).get(0));
                assertEquals(RawString.of("mykey2"), results.get(0).get(0));
                assertEquals(1234, results.get(1).get(1));
                assertEquals(1235, results.get(0).get(1));

            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " ORDER BY n1 desc", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(1).get(0));
                assertEquals(RawString.of("mykey2"), results.get(0).get(0));

            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " ORDER BY n1 desc limit 1", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey2"), results.get(0).get(0));

            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " ORDER BY n1 desc limit 1,1", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));

            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " WHERE k1='mykey'", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));

            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " WHERE k1='mykey' aNd n1>3", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " WHERE k1>='mykey' aNd n1>3 and n1<=1234", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " WHERE k1='mykey' or not n1<3"
                    + " order by k1", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));
                assertEquals(RawString.of("mykey2"), results.get(1).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT count(*) "
                    + " FROM tblspace1.tsql",
                    Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(2L, results.get(0).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT count(*), n1, k1 "
                    + " FROM tblspace1.tsql"
                    + " GROUP by k1, n1"
                    + " ORDER BY k1",
                    Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(3, results.get(0).getFieldNames().length);
                assertEquals(1L, results.get(0).get(0));
                assertEquals(RawString.of("mykey"), results.get(0).get(2));
                assertEquals(1234, results.get(0).get(1));
                assertEquals(1L, results.get(1).get(0));
                assertEquals(RawString.of("mykey2"), results.get(1).get(2));
                assertEquals(1235, results.get(1).get(1));
            }
            try (DataScanner scan = scan(manager, "SELECT n1, count(*) as cc "
                    + " FROM tblspace1.tsql"
                    + " GROUP by n1"
                    + " ORDER BY n1",
                    Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(2, results.get(0).getFieldNames().length);
                assertEquals(1L, results.get(0).get(1));
                assertEquals(1234, results.get(0).get(0));
                assertEquals(1L, results.get(1).get(1));
                assertEquals(1235, results.get(1).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT sum(n1), count(*) as cc, k1 "
                    + " FROM tblspace1.tsql"
                    + " GROUP by k1"
                    + " ORDER BY sum(n1)",
                    Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(3, results.get(0).getFieldNames().length);
                assertEquals(1234L, results.get(0).get(0));
                assertEquals(1L, results.get(0).get(1));
                assertEquals(RawString.of("mykey"), results.get(0).get(2));
                assertEquals(1235L, results.get(1).get(0));
                assertEquals(1L, results.get(1).get(1));
                assertEquals(RawString.of("mykey2"), results.get(1).get(2));

            }
            try (DataScanner scan = scan(manager, "SELECT sum(n1) as ss, min(n1) as mi, max(n1) as ma"
                    + " FROM tblspace1.tsql",
                    Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(3, results.get(0).getFieldNames().length);
                assertEquals(1234L + 1235L, results.get(0).get("SS"));
                assertEquals(1234, results.get(0).get("MI"));
                assertEquals(1235, results.get(0).get("MA"));

            }
            try (DataScanner scan = scan(manager, "SELECT sum(n1), count(*) as cc, k1 "
                    + " FROM tblspace1.tsql"
                    + " GROUP by k1"
                    + " HAVING sum(n1) <= 1234",
                    Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(3, results.get(0).getFieldNames().length);
                assertEquals(1234L, results.get(0).get(0));
                assertEquals(1L, results.get(0).get(1));
                assertEquals(RawString.of("mykey"), results.get(0).get(2));
            }
        }
    }

    @Test
    public void plannerDMLTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql", Collections.emptyList()).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(0, results.size());
            }
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql WHERE k1=?", Arrays.asList("mykey")).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList());) {
                List<DataAccessor> results = scan.consume();
                assertEquals(0, results.size());
            }
        }
    }
}
