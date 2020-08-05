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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.IllegalDataAccessException;
import herddb.utils.RawString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class AlterTableSQLTest {

    @Test
    public void addColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.TSQL (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            Table table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.getColumn("s1").serialPosition);
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1,s1) values('a',1,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            execute(manager, "ALTER TABLE tblspace1.tsql add column k2 string", Collections.emptyList());
            // uppercase table name !
            execute(manager, "INSERT INTO tblspace1.TSQL (k1,n1,s1,k2) values('b',1,'b','c')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql WHERE k2='c'", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(4, tuples.get(0).getFieldNames().length);
            }
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.getColumn("s1").serialPosition);
            assertEquals(3, table.getColumn("k2").serialPosition);

            // check alter table is case non sensitive about table names
            execute(manager, "ALTER TABLE tblspace1.TSQL add column k10 string", Collections.emptyList());
            execute(manager, "ALTER TABLE tblspace1.tSql add column k11 string", Collections.emptyList());
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(ColumnTypes.STRING, table.getColumn("k11").type);

            assertEquals("Found a record in table tsql that contains a NULL value for column k11 ALTER command is not possible",
                    herddb.utils.TestUtils.expectThrows(StatementExecutionException.class, () -> {
                execute(manager, "ALTER TABLE tblspace1.tSql modify column k11 string not null", Collections.emptyList());
            }).getMessage());
            // no effect
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(ColumnTypes.STRING, table.getColumn("k11").type);

            execute(manager, "TRUNCATE  TABLE tblspace1.tSql", Collections.emptyList());
            execute(manager, "ALTER TABLE tblspace1.tSql modify column k11 string not null", Collections.emptyList());
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(ColumnTypes.NOTNULL_STRING, table.getColumn("k11").type);

        }
    }

    @Test
    public void dropColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
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
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose();
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
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(2, tuples.get(0).getFieldNames().length);
                try {
                    assertEquals(null, tuples.get(0).get("s1"));
                    fail("field does not exist anymore");
                } catch (IllegalDataAccessException ok) {
                }
            }

            execute(manager, "ALTER TABLE tblspace1.tsql add column s1 string", Collections.emptyList());
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(3, table.getColumn("s1").serialPosition);
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose();
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
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
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
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql where k1=1", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            execute(manager, "ALTER TABLE tblspace1.tsql modify column k1 int", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1,s1) values(2, 2,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose();
                assertEquals(2, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql where k1=2", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            assertFalse(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().auto_increment);
            execute(manager, "ALTER TABLE tblspace1.tsql modify column k1 int auto_increment", Collections.emptyList());
            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().auto_increment);
            execute(manager, "INSERT INTO tblspace1.tsql (n1,s1) values(1,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose();
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

            execute(manager, "ALTER TABLE tblspace1.tsql CHANGE k1 l2 int", Collections.emptyList());

            assertFalse(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().auto_increment);

            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql "
                        + "where l2=1", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(Arrays.toString(tuples.get(0).getFieldNames()), 3, tuples.get(0).getFieldNames().length);
                assertEquals("l2", tuples.get(0).getFieldNames()[0]);
                assertEquals("n1", tuples.get(0).getFieldNames()[1]);
                assertEquals("s1", tuples.get(0).getFieldNames()[2]);
            }

        }
    }


    @Test
    public void modifyColumnWithDefaults() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 int primary key auto_increment,n1 int default 1234,s1 string)", Collections.emptyList());
            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().auto_increment);
            Table table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(1234, table.getColumn("n1").defaultValue.to_int());
            assertEquals(2, table.getColumn("s1").serialPosition);

            // keep default
            execute(manager, "ALTER TABLE tblspace1.tsql modify column n1 int", Collections.emptyList());

            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(1234, table.getColumn("n1").defaultValue.to_int());
            assertEquals(2, table.getColumn("s1").serialPosition);

            // alter default
            execute(manager, "ALTER TABLE tblspace1.tsql modify column n1 int default 1255", Collections.emptyList());

            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(1255, table.getColumn("n1").defaultValue.to_int());
            assertEquals(2, table.getColumn("s1").serialPosition);

            // rename, shuold keep default
            execute(manager, "ALTER TABLE tblspace1.tsql change column n1 n2 int", Collections.emptyList());

            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n2").serialPosition);
            assertEquals(1255, table.getColumn("n2").defaultValue.to_int());
            assertEquals(2, table.getColumn("s1").serialPosition);

            // drop default
            execute(manager, "ALTER TABLE tblspace1.tsql modify column n2 int default null", Collections.emptyList());

            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n2").serialPosition);
            assertNull(table.getColumn("n2").defaultValue);
            assertEquals(2, table.getColumn("s1").serialPosition);

            execute(manager, "ALTER TABLE tblspace1.tsql add mydate timestamp default CURRENT_TIMESTAMP", Collections.emptyList());
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n2").serialPosition);
            assertNull(table.getColumn("n2").defaultValue);
            assertEquals(3, table.getColumn("mydate").serialPosition);
            assertEquals(Bytes.from_string("CURRENT_TIMESTAMP"), table.getColumn("mydate").defaultValue);

        }
    }

    @Test
    public void renameTable() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 int primary key auto_increment,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (n1,s1) values(1,'b')", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql where k1=1", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            // rename to UPPERCASE
            execute(manager, "EXECUTE RENAMETABLE 'tblspace1','tsql','TSQL2'", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql2 where k1=1", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.TSQL2 where k1=1", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }
            // rename using different case
            execute(manager, "EXECUTE RENAMETABLE 'tblspace1','tsql2','tsql3'", Collections.emptyList());
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql3 where k1=1", Collections.emptyList()).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
            }

        }
    }

    @Test
    public void alterTableImplitlyCommitsTransaction() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            Table table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.getColumn("s1").serialPosition);
            long tx = TestUtils.beginTransaction(manager, "tblspace1");
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1,s1) values('a',1,'b')", Collections.emptyList(), new TransactionContext(tx));

            {
                // record not visible outside transaction
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList(), TransactionContext.NO_TRANSACTION).consumeAndClose();
                assertEquals(0, tuples.size());
            }

            {
                // record visible inside transaction
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList(), new TransactionContext(tx)).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).getFieldNames().length);
                assertEquals(RawString.of("b"), tuples.get(0).get("s1"));
            }
            execute(manager, "ALTER TABLE tblspace1.tsql drop column s1", Collections.emptyList(), new TransactionContext(tx));
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.columns.length);

            // transaction no more exists (it has been committed)
            assertNull(manager.getTableSpaceManager("tblspace1").getTransaction(tx));

            // record is visible out of the transaction, but with only 2 columns
            {
                List<DataAccessor> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList(), TransactionContext.NO_TRANSACTION).consumeAndClose();
                assertEquals(1, tuples.size());
                assertEquals(2, tuples.get(0).getFieldNames().length);
            }

        }
    }

}
