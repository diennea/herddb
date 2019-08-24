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

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class ProfileEqualsAllocationsTest {

    @Test
    public void simpleEqualsTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,n2 long, s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey", Integer.valueOf(1), "a", Long.valueOf(3))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey2", Integer.valueOf(2), "a", Long.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey3", Integer.valueOf(5), "b", Long.valueOf(1))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1) values(?)", Arrays.asList("mykey4")).getUpdateCount());

            {
                long _start = System.currentTimeMillis();
                for (int i = 0; i < 1; i++) {

                    try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where k1=?", Arrays.asList("mykey"))) {
                        List<DataAccessor> result = scan1.consume();
                        assertEquals(1, result.size());
                        if (i % 1000 == 0) {
                            long _stop = System.currentTimeMillis();
                            System.out.println(i + " time:" + (_stop - _start) + " ms");
                        }
                    }
                }
                long _stop = System.currentTimeMillis();
                System.out.println("time:" + (_stop - _start) + " ms");
            }
            {
                long _start = System.currentTimeMillis();
                for (int i = 0; i < 1; i++) {

                    try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n1=?", Arrays.asList(5))) {
                        List<DataAccessor> result = scan1.consume();
                        assertEquals(1, result.size());
                        if (i % 1000 == 0) {
                            long _stop = System.currentTimeMillis();
                            System.out.println(i + " time:" + (_stop - _start) + " ms");
                        }
                    }
                }
                long _stop = System.currentTimeMillis();
                System.out.println("time:" + (_stop - _start) + " ms");
            }
        }
    }

    @Test
    public void simpleEqualsIntTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string ,n1 int primary key,n2 long, s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey", Integer.valueOf(1), "a", Long.valueOf(3))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey2", Integer.valueOf(2), "a", Long.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey3", Integer.valueOf(5), "b", Long.valueOf(1))).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n1=?", Arrays.asList(5))) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n1=?", Arrays.asList(5L))) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n1=?", Arrays.asList(5.0))) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n1=5", Arrays.asList())) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
        }

    }


    @Test
    public void simpleEqualsLongTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string ,n1 long primary key,n2 long, s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey", Integer.valueOf(1), "a", Long.valueOf(3))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey2", Integer.valueOf(2), "a", Long.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey3", Integer.valueOf(5), "b", Long.valueOf(1))).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n1=?", Arrays.asList(5))) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n1=?", Arrays.asList(5L))) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n1=?", Arrays.asList(5.0))) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n1=5", Arrays.asList())) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
        }

    }


    @Test
    public void simpleEqualsNonPKTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string ,n1 long primary key,n2 long, s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey", Integer.valueOf(1), "a", Long.valueOf(3))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey2", Integer.valueOf(2), "a", Long.valueOf(2))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1,n2) values(?,?,?,?)", Arrays.asList("mykey3", Integer.valueOf(5), "b", Long.valueOf(1))).getUpdateCount());

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n2=?", Arrays.asList(2))) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n2=?", Arrays.asList(2L))) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n2=?", Arrays.asList(2.0))) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql where n2=2", Arrays.asList())) {
                List<DataAccessor> result = scan1.consume();
                assertEquals(1, result.size());
            }
        }

    }

}
