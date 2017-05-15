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
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import java.util.Arrays;
import static org.junit.Assert.assertFalse;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class AlterTableSQLTest {

    @Test
    public void addColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            Table table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.getColumn("s1").serialPosition);
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1,s1) values('a',1,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            execute(manager, "ALTER TABLE tblspace1.tsql add column k2 string", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1,s1,k2) values('b',1,'b','c')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql WHERE k2='c'", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(4, tuples.get(0).getFieldNames().length);
            }
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.getColumn("s1").serialPosition);
            assertEquals(3, table.getColumn("k2").serialPosition);

        }
    }

    @Test
    public void dropColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            Table table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.getColumn("s1").serialPosition);
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1,s1) values('a',1,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
                assertEquals(RawString.of("b"), tuples.get(0).get("s1"));
            }
            execute(manager, "ALTER TABLE tblspace1.tsql drop column s1", Collections.emptyList());
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.columns.length);

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(2, tuples.get(0).getFieldNames().length);
                assertEquals(null, tuples.get(0).get("s1"));
            }

            execute(manager, "ALTER TABLE tblspace1.tsql add column s1 string", Collections.emptyList());
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(3, table.getColumn("s1").serialPosition);
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
                assertEquals(null, tuples.get(0).get("s1"));
            }

            try {
                execute(manager, "ALTER TABLE tblspace1.tsql drop column k1", Collections.emptyList());
                fail();
            } catch (StatementExecutionException error) {
                assertTrue(error.getMessage().contains("column k1 cannot be dropped because is part of the primary key"));
            }

        }
    }

    @Test
    public void modifyColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 int primary key auto_increment,n1 int,s1 string)", Collections.emptyList());
            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().auto_increment);
            Table table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.getColumn("s1").serialPosition);
            execute(manager, "INSERT INTO tblspace1.tsql (n1,s1) values(1,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql where k1=1", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            execute(manager, "ALTER TABLE tblspace1.tsql modify column k1 int", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1,s1) values(2, 2,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(2, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql where k1=2", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            assertFalse(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().auto_increment);
            execute(manager, "ALTER TABLE tblspace1.tsql modify column k1 int auto_increment", Collections.emptyList());
            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().auto_increment);
            execute(manager, "INSERT INTO tblspace1.tsql (n1,s1) values(1,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(3, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }

            try {
                execute(manager, "ALTER TABLE tblspace1.tsql modify column k1 string", Collections.emptyList());
                fail();
            } catch (StatementExecutionException error) {
                assertTrue(error.getMessage().contains("cannot change datatype"));
            }

            try {
                execute(manager, "ALTER TABLE tblspace1.tsql modify column badcol string", Collections.emptyList());
                fail();
            } catch (StatementExecutionException error) {
                assertTrue(error.getMessage().contains("bad column badcol in table tsql"));
            }

            execute(manager, "ALTER TABLE tblspace1.tsql MODIFY COLUMN k1 int RENAME TO l2", Collections.emptyList());

            assertFalse(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().auto_increment);

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql "
                    + "where l2=1", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(Arrays.toString(tuples.get(0).getFieldNames()), 3, tuples.get(0).getFieldNames().length);
                assertEquals("l2", tuples.get(0).getFieldNames()[0]);
                assertEquals("n1", tuples.get(0).getFieldNames()[1]);
                assertEquals("s1", tuples.get(0).getFieldNames()[2]);
            }

        }
    }

    @Test
    public void renameTable() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 int primary key auto_increment,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (n1,s1) values(1,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql where k1=1", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            execute(manager, "EXECUTE RENAMETABLE 'tblspace1','tsql','tsql2'", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql2 where k1=1", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }

        }
    }

}
