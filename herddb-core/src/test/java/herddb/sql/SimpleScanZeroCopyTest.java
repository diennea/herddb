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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.codec.DataAccessorForFullRecord;
import herddb.core.DBManager;
import herddb.core.TestUtils;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.planner.ProjectOp.ZeroCopyProjection;
import herddb.utils.DataAccessor;
import herddb.utils.IntHolder;
import herddb.utils.ProjectedDataAccessor;
import herddb.utils.RawString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * Simple test case to assert the zero-copy
 *
 * @author enrico.olivelli
 */
public class SimpleScanZeroCopyTest {

    @Test
    public void test() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList("a", 1, "b"));
            try (DataScanner scan = TestUtils.scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                // read from the full record
                assertTrue(data.get(0) instanceof DataAccessorForFullRecord);
                String[] fieldNames = scan.getFieldNames();
                for (String fName : fieldNames) {
                    Object value = data.get(0).get(fName);
                    System.out.println("FIELD " + fName + " -> " + value + " (" + value.getClass() + ")");
                }
                IntHolder current = new IntHolder();
                data.get(0).forEach((fName, value) -> {
                    System.out.println("FIELD2 " + fName + " -> " + value + " (" + value.getClass() + ")");
                    assertEquals(fName, fieldNames[current.value++]);
                });
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT k1 FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                // read from the full record, keeping only some field
                assertTrue(data.get(0) instanceof ProjectedDataAccessor
                        || data.get(0) instanceof ZeroCopyProjection.RuntimeProjectedDataAccessor);
                assertEquals(RawString.of("a"), data.get(0).get("k1"));
                assertEquals(RawString.of("a"), data.get(0).get(0));
                String[] fieldNames = scan.getFieldNames();
                for (String fName : fieldNames) {
                    Object value = data.get(0).get(fName);
                    System.out.println("FIELD " + fName + " -> " + value + " (" + value.getClass() + ")");
                }
                IntHolder current = new IntHolder();
                data.get(0).forEach((fName, value) -> {
                    System.out.println("FIELD2 " + fName + " -> " + value + " (" + value.getClass() + ")");
                    assertEquals(fName, fieldNames[current.value++]);
                });
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                assertTrue(data.get(0) instanceof Tuple);
                String[] fieldNames = scan.getFieldNames();
                for (String fName : fieldNames) {
                    Object value = data.get(0).get(fName);
                    System.out.println("FIELD " + fName + " -> " + value + " (" + value.getClass() + ")");
                }
                IntHolder current = new IntHolder();
                data.get(0).forEach((fName, value) -> {
                    System.out.println("FIELD2 " + fName + " -> " + value + " (" + value.getClass() + ")");
                    assertEquals(fName, fieldNames[current.value++]);
                });
            }
        }
    }

    @Test
    public void testPkNotFirst() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (n1 int,k1 string primary key, s1 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList("a", 1, "b"));
            try (DataScanner scan = TestUtils.scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                // read from the full record
                assertTrue(data.get(0) instanceof DataAccessorForFullRecord);

                assertEquals(1, data.get(0).get("n1"));
                assertEquals(1, data.get(0).get(0));
                assertEquals(RawString.of("a"), data.get(0).get("k1"));
                assertEquals(RawString.of("a"), data.get(0).get(1));
                assertEquals(RawString.of("b"), data.get(0).get("s1"));
                assertEquals(RawString.of("b"), data.get(0).get(2));

                String[] fieldNames = scan.getFieldNames();
                for (String fName : fieldNames) {
                    Object value = data.get(0).get(fName);
                    System.out.println("FIELD " + fName + " -> " + value + " (" + value.getClass() + ")");
                }
                IntHolder current = new IntHolder();
                data.get(0).forEach((fName, value) -> {
                    System.out.println("FIELD2 " + fName + " -> " + value + " (" + value.getClass() + ")");
                    assertEquals(fName, fieldNames[current.value++]);
                });

            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT k1 FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                // read from the full record, keeping only some field
                assertTrue(data.get(0) instanceof ProjectedDataAccessor
                        || data.get(0) instanceof ZeroCopyProjection.RuntimeProjectedDataAccessor);
                assertEquals(RawString.of("a"), data.get(0).get("k1"));
                assertEquals(RawString.of("a"), data.get(0).get(0));

                String[] fieldNames = scan.getFieldNames();
                for (String fName : fieldNames) {
                    Object value = data.get(0).get(fName);
                    System.out.println("FIELD " + fName + " -> " + value + " (" + value.getClass() + ")");
                }
                IntHolder current = new IntHolder();
                data.get(0).forEach((fName, value) -> {
                    System.out.println("FIELD2 " + fName + " -> " + value + " (" + value.getClass() + ")");
                    assertEquals(fName, fieldNames[current.value++]);
                });
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                assertTrue(data.get(0) instanceof Tuple);
            }
        }
    }

    @Test
    public void testMultiPkFirst() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (n1 int primary key,k1 string primary key, s1 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList("a", 1, "b"));
            try (DataScanner scan = TestUtils.scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                // read from the full record
                assertTrue(data.get(0) instanceof DataAccessorForFullRecord);

                assertEquals(1, data.get(0).get("n1"));
                assertEquals(1, data.get(0).get(0));
                assertEquals(RawString.of("a"), data.get(0).get("k1"));
                assertEquals(RawString.of("a"), data.get(0).get(1));
                assertEquals(RawString.of("b"), data.get(0).get("s1"));
                assertEquals(RawString.of("b"), data.get(0).get(2));

                String[] fieldNames = scan.getFieldNames();
                for (String fName : fieldNames) {
                    Object value = data.get(0).get(fName);
                    System.out.println("FIELD " + fName + " -> " + value + " (" + value.getClass() + ")");
                }
                IntHolder current = new IntHolder();
                data.get(0).forEach((fName, value) -> {
                    System.out.println("FIELD2 " + fName + " -> " + value + " (" + value.getClass() + ")");
                    assertEquals(fName, fieldNames[current.value++]);
                });

            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT k1 FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                // read from the full record, keeping only some field
                assertTrue(data.get(0) instanceof ProjectedDataAccessor
                        || data.get(0) instanceof ZeroCopyProjection.RuntimeProjectedDataAccessor);
                assertEquals(RawString.of("a"), data.get(0).get("k1"));
                assertEquals(RawString.of("a"), data.get(0).get(0));

                String[] fieldNames = scan.getFieldNames();
                for (String fName : fieldNames) {
                    Object value = data.get(0).get(fName);
                    System.out.println("FIELD " + fName + " -> " + value + " (" + value.getClass() + ")");
                }
                IntHolder current = new IntHolder();
                data.get(0).forEach((fName, value) -> {
                    System.out.println("FIELD2 " + fName + " -> " + value + " (" + value.getClass() + ")");
                    assertEquals(fName, fieldNames[current.value++]);
                });
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                assertTrue(data.get(0) instanceof Tuple);
            }
        }
    }

    @Test
    public void testMultiPkFirstButBadPkOrder() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (n1 int ,k1 string, s1 string,"
                    + "primary key(k1, n1))", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList("a", 1, "b"));
            try (DataScanner scan = TestUtils.scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                // read from the full record
                assertTrue(data.get(0) instanceof DataAccessorForFullRecord);

                assertEquals(1, data.get(0).get("n1"));
                assertEquals(1, data.get(0).get(0));
                assertEquals(RawString.of("a"), data.get(0).get("k1"));
                assertEquals(RawString.of("a"), data.get(0).get(1));
                assertEquals(RawString.of("b"), data.get(0).get("s1"));
                assertEquals(RawString.of("b"), data.get(0).get(2));

                String[] fieldNames = scan.getFieldNames();
                for (String fName : fieldNames) {
                    Object value = data.get(0).get(fName);
                    System.out.println("FIELD " + fName + " -> " + value + " (" + value.getClass() + ")");
                }
                IntHolder current = new IntHolder();
                data.get(0).forEach((fName, value) -> {
                    System.out.println("FIELD2 " + fName + " -> " + value + " (" + value.getClass() + ")");
                    assertEquals(fName, fieldNames[current.value++]);
                });

            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT k1 FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                // read from the full record, keeping only some field
                assertTrue(data.get(0) instanceof ProjectedDataAccessor
                        || data.get(0) instanceof ZeroCopyProjection.RuntimeProjectedDataAccessor);
                assertEquals(RawString.of("a"), data.get(0).get("k1"));
                assertEquals(RawString.of("a"), data.get(0).get(0));

                String[] fieldNames = scan.getFieldNames();
                for (String fName : fieldNames) {
                    Object value = data.get(0).get(fName);
                    System.out.println("FIELD " + fName + " -> " + value + " (" + value.getClass() + ")");
                }
                IntHolder current = new IntHolder();
                data.get(0).forEach((fName, value) -> {
                    System.out.println("FIELD2 " + fName + " -> " + value + " (" + value.getClass() + ")");
                    assertEquals(fName, fieldNames[current.value++]);
                });
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM tblspace1.tsql ", Collections.emptyList())) {
                List<DataAccessor> data = scan.consume();
                assertEquals(1, data.size());
                assertTrue(data.get(0) instanceof Tuple);
            }
        }
    }
}
