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

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.sql.expressions.CompiledFunction;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class SimplerPlannerTest {

    @Test
    public void simpleInsertTests() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1) values('a')", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1) values(?,?)", Arrays.asList("mykey", 1234), TransactionContext.NO_TRANSACTION);
            try (DataScanner scan = scan(manager, "SELECT n1,k1 FROM tblspace1.tsql where k1='mykey'", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            }
            try (DataScanner scan = scan(manager, "SELECT n1,k1 FROM tblspace1.tsql where k1='a'", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            }
            try (DataScanner scan = scan(manager, "SELECT n1,k1 FROM tblspace1.tsql where k1='mykey' and n1=1234", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            }
        }
    }

    @Test
    public void plannerBasicStatementsTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList())) {
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
                    + " ORDER BY k1", Collections.emptyList())) {
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
                    + " ORDER BY k1 desc", Collections.emptyList())) {
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
                    + " ORDER BY k1 desc", Collections.emptyList())) {
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
                    + " ORDER BY n1 desc", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(1).get(0));
                assertEquals(RawString.of("mykey2"), results.get(0).get(0));

            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " ORDER BY n1 desc limit 1", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey2"), results.get(0).get(0));

            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " ORDER BY n1 desc limit 1,1", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));

            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " WHERE k1='mykey'", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));

            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " WHERE k1='mykey' aNd n1>3", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " WHERE k1>='mykey' aNd n1>3 and n1<=1234", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT k1 "
                    + " FROM tblspace1.tsql"
                    + " WHERE k1='mykey' or not n1<3"
                    + " order by k1", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));
                assertEquals(RawString.of("mykey2"), results.get(1).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT count(*) "
                            + " FROM tblspace1.tsql",
                    Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(1, results.get(0).getFieldNames().length);
                assertEquals(2L, results.get(0).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT count(*), n1, k1 "
                            + " FROM tblspace1.tsql"
                            + " GROUP by k1, n1"
                            + " ORDER BY k1",
                    Collections.emptyList())) {
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
                    Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(2, results.get(0).getFieldNames().length);
                assertEquals(1L, results.get(0).get(1));
                assertEquals(1234, results.get(0).get(0));
                assertEquals(1L, results.get(1).get(1));
                assertEquals(1235, results.get(1).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT n1, count(*) as cc "
                            + " FROM tblspace1.tsql"
                            + " GROUP by n1"
                            + " ORDER BY abs(n1)",
                    Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(2, results.get(0).getFieldNames().length);
                assertEquals(1L, results.get(0).get(1));
                assertEquals(1234, results.get(0).get(0));
                assertEquals(1L, results.get(1).get(1));
                assertEquals(1235, results.get(1).get(0));
            }
            try (DataScanner scan = scan(manager, "SELECT n1, count(*) as cc "
                            + " FROM tblspace1.tsql"
                            + " GROUP by n1"
                            + " ORDER BY cosine_similarity(n1, n1)",
                    Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(2, results.get(0).getFieldNames().length);
                assertEquals(1L, results.get(0).get(1));
                assertEquals(1234, results.get(0).get(0));
                assertEquals(1L, results.get(1).get(1));
                assertEquals(1235, results.get(1).get(0));
            }
            if (manager.getPlanner() instanceof CalcitePlanner) {
                try (DataScanner scan = scan(manager, "SELECT sum(n1), count(*) as cc, k1 "
                                + " FROM tblspace1.tsql"
                                + " GROUP by k1"
                                + " ORDER BY sum(n1)",
                        Collections.emptyList())) {
                    List<DataAccessor> results = scan.consume();
                    assertEquals(2, results.size());
                    assertEquals(3, results.get(0).getFieldNames().length);
                    // see https://issues.apache.org/jira/browse/CALCITE-3998
                    // we are summing integers but the final datatype is not bigint
                    // but still INTEGER because we are actually summing only one value per row
                    assertEquals(1234, results.get(0).get(0));
                    assertEquals(1L, results.get(0).get(1));
                    assertEquals(RawString.of("mykey"), results.get(0).get(2));
                    assertEquals(1235, results.get(1).get(0));
                    assertEquals(1L, results.get(1).get(1));
                    assertEquals(RawString.of("mykey2"), results.get(1).get(2));

                }
            }
            try (DataScanner scan = scan(manager, "SELECT sum(n1) as ss, min(n1) as mi, max(n1) as ma"
                            + " FROM tblspace1.tsql",
                    Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(3, results.get(0).getFieldNames().length);
                System.out.println("map:" + results.get(0).toMap());
                assertEquals(1234L + 1235L, results.get(0).get("ss"));
                assertEquals(1234, results.get(0).get("mi"));
                assertEquals(1235, results.get(0).get("ma"));

            }
            if (manager.getPlanner() instanceof CalcitePlanner) {
                try (DataScanner scan = scan(manager, "SELECT sum(n1), count(*) as cc, k1 "
                                + " FROM tblspace1.tsql"
                                + " GROUP by k1"
                                + " HAVING sum(n1) <= 1234",
                        Collections.emptyList())) {
                    List<DataAccessor> results = scan.consume();
                    assertEquals(1, results.size());
                    assertEquals(3, results.get(0).getFieldNames().length);
                    assertEquals(1234, results.get(0).get(0));
                    assertEquals(1L, results.get(0).get(1));
                    assertEquals(RawString.of("mykey"), results.get(0).get(2));
                }
            }
        }
    }

    @Test
    public void plannerDeleteTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql", Collections.emptyList()).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(0, results.size());
            }
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "DELETE FROM tblspace1.tsql WHERE k1=?", Arrays.asList("mykey")).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(0, results.size());
            }
        }
    }

    @Test
    public void plannerUpdateTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=1111", Collections.emptyList()).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql where n1=1111", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
            }
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=1112 where k1='mykey'", Collections.emptyList()).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql where n1=1112", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
            }
            assertEquals(0, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=1112 where k1='no'", Collections.emptyList()).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=n1+1 where k1='mykey'", Collections.emptyList()).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql where n1=1113", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
            }

            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1=?", Arrays.asList(1114, "mykey")).getUpdateCount());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql where n1=1114", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
            }
        }
    }

    @Test
    public void basicVectorTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string, v1 floata)", Collections.emptyList());

            List<Float> vector1AsList = Arrays.asList(0.2f, 0.8f);
            float[] vector3raw = new float[]{0f, 1f};
            float[] vector2raw = new float[]{0.1f, 0.9f};
            float[] vector1raw = new float[]{0.2f, 0.8f};

            float v3v3 = CompiledFunction.cosineSimilarity(vector3raw, vector3raw);
            float v3v2 = CompiledFunction.cosineSimilarity(vector3raw, vector2raw);
            float v3v1 = CompiledFunction.cosineSimilarity(vector3raw, vector1raw);
            System.out.println("v3v3:" + v3v3);
            System.out.println("v3v2:" + v3v2);
            System.out.println("v3v1:" + v3v1);
            assertTrue(v3v2 < v3v3);
            assertTrue(v3v1 < v3v3);
            assertTrue(v3v1 < v3v2);

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,v1) values(?,?,?)", Arrays.asList("mykey", Integer.valueOf(1234),
                    vector1AsList)).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,v1) values(?,?,?)", Arrays.asList("mykey2", Integer.valueOf(1235),
                    vector2raw)).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,v1) values(?,?,?)", Arrays.asList("mykey3", Integer.valueOf(1236),
                    vector3raw)).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql where n1=1234", Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(4, results.get(0).getFieldNames().length);
                assertEquals(RawString.of("mykey"), results.get(0).get(0));
                assertEquals(RawString.of("mykey"), results.get(0).get("k1"));
                assertEquals(1234, results.get(0).get(1));
                assertEquals(1234, results.get(0).get("n1"));
                assertEquals(null, results.get(0).get(2));
                assertEquals(null, results.get(0).get("s1"));
                assertArrayEquals(vector1raw, (float[]) results.get(0).get(3), 0);
                assertArrayEquals(vector1raw, (float[]) results.get(0).get("v1"), 0);
            }

            try (DataScanner scan = scan(manager, "SELECT n1, cosine_similarity(v1, cast(? as FLOAT ARRAY)) as distance, v1 "
                            + " FROM tblspace1.tsql"
                            + " ORDER BY cosine_similarity(v1, cast(? as FLOAT ARRAY)) DESC",
                    Arrays.asList(vector3raw, vector3raw))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(3, results.size());
                assertEquals(1236, results.get(0).get(0));
                assertEquals(1235, results.get(1).get(0));
                assertEquals(1234, results.get(2).get(0));
            }

            try (DataScanner scan = scan(manager, "SELECT n1, dot_product(v1, cast(? as FLOAT ARRAY)) as distance, v1 "
                            + " FROM tblspace1.tsql"
                            + " ORDER BY dot_product(v1, cast(? as FLOAT ARRAY)) DESC",
                    Arrays.asList(vector3raw, vector3raw))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(3, results.size());
                assertEquals(1236, results.get(0).get(0));
                assertEquals(1235, results.get(1).get(0));
                assertEquals(1234, results.get(2).get(0));
            }

            try (DataScanner scan = scan(manager, "SELECT n1, euclidean_distance(v1, cast(? as FLOAT ARRAY)) as distance, v1 "
                            + " FROM tblspace1.tsql"
                            + " ORDER BY euclidean_distance(v1, cast(? as FLOAT ARRAY))",
                    Arrays.asList(vector3raw, vector3raw))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(3, results.size());
                assertEquals(1236, results.get(0).get(0));
                assertEquals(1235, results.get(1).get(0));
                assertEquals(1234, results.get(2).get(0));
            }
        }
    }
}
