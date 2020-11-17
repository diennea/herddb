/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.sql;

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.dump;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.scan;
import static herddb.utils.TestUtils.expectThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import herddb.core.DBManager;
import herddb.core.TestUtils;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScannerException;
import herddb.model.ForeignKeyDef;
import herddb.model.ForeignKeyViolationException;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Collections;
import org.junit.Test;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class ForeignKeySQLTest {

    @Test
    public void createTableWithForeignKey() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.parent (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.child (k2 string primary key,n2 int,"
                    + "s2 string, "
                    + "CONSTRAINT fk1 FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1) ON DELETE NO ACTION ON UPDATE NO ACTION)", Collections.emptyList());
            Table parentTable = manager.getTableSpaceManager("tblspace1").getTableManager("parent").getTable();

            Table childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(1, childTable.foreignKeys.length);
            assertEquals("fk1", childTable.foreignKeys[0].name);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2", "n2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1", "n1"}, childTable.foreignKeys[0].parentTableColumns);

            testChildSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1"); // test without transaction

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            long tx = beginTransaction(manager, "tblspace1");
            testChildSideOfForeignKey(manager, tx, "fk1");  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            testServerSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1", "NO ACTION", "NO ACTION"); // test without transaction

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            tx = beginTransaction(manager, "tblspace1");
            testServerSideOfForeignKey(manager, tx, "fk1", "NO ACTION", "NO ACTION");  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

        }
    }

    private void testChildSideOfForeignKey(final DBManager manager, long tx, String fkName) throws DataScannerException, StatementExecutionException {
        ForeignKeyViolationException err = expectThrows(ForeignKeyViolationException.class, () -> {
            execute(manager, "INSERT INTO tblspace1.child(k2,n2,s2) values('a',2,'pvalue')", Collections.emptyList(), new TransactionContext(tx));
        });
        assertEquals(fkName, err.getForeignKeyName());

        execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('a',2,'pvalue')", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "INSERT INTO tblspace1.child(k2,n2,s2) values('c1',2,'a')", Collections.emptyList(), new TransactionContext(tx));

        ForeignKeyViolationException errOnUpdate = expectThrows(ForeignKeyViolationException.class, () -> {
            execute(manager, "UPDATE tblspace1.child set s2='badvalue'", Collections.emptyList(), new TransactionContext(tx));
        });
        assertEquals(fkName, errOnUpdate.getForeignKeyName());

        execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('newvalue',2,'foo')", Collections.emptyList(), new TransactionContext(tx));
        dump(manager, "SELECT * FROM tblspace1.parent", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "UPDATE tblspace1.child set s2='newvalue'", Collections.emptyList(), new TransactionContext(tx));
    }

    private void testServerSideOfForeignKey(final DBManager manager, long tx, String fkName, String updateAction, String deleteAction) throws DataScannerException, StatementExecutionException {
        execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('a',2,'pvalue')", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('newvalue',2,'foo')", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "INSERT INTO tblspace1.child(k2,n2,s2) values('c1',2,'a')", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "INSERT INTO tblspace1.child(k2,n2,s2) values('c2',2,'newvalue')", Collections.emptyList(), new TransactionContext(tx));

        if ("SET NULL".equals(updateAction)) {
            // assert that we are setting null only on the expected record
            execute(manager, "UPDATE tblspace1.parent set n1=983 where k1='a'", Collections.emptyList(), new TransactionContext(tx));
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.child WHERE s2 is NULL and n2 is NULL", Collections.emptyList(), new TransactionContext(tx)).consumeAndClose().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.child WHERE s2='newvalue'", Collections.emptyList(), new TransactionContext(tx)).consumeAndClose().size());
        } else if ("CASCADE".equals(updateAction)) {
            // not implemented
        } else if ("NO ACTION".equals(updateAction)) {
            ForeignKeyViolationException errOnUpdate = expectThrows(ForeignKeyViolationException.class, () -> {
                execute(manager, "UPDATE tblspace1.parent set n1=983", Collections.emptyList(), new TransactionContext(tx));
            });
            assertEquals(fkName, errOnUpdate.getForeignKeyName());
        } else {
            fail();
        }

        if ("CASCADE".equals(deleteAction)) {
            execute(manager, "DELETE FROM tblspace1.parent where k1='a'", Collections.emptyList(), new TransactionContext(tx));
            // assert that we are deleting only the expected record
            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.child WHERE s2='a'", Collections.emptyList(), new TransactionContext(tx)).consumeAndClose().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.child WHERE s2='newvalue'", Collections.emptyList(), new TransactionContext(tx)).consumeAndClose().size());
        } else if ("SET NULL".equals(deleteAction)) {
            execute(manager, "DELETE FROM tblspace1.parent where k1='a'", Collections.emptyList(), new TransactionContext(tx));
            // assert that we are setting null only on the expected record
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.child WHERE s2 is NULL and n2 is NULL", Collections.emptyList(), new TransactionContext(tx)).consumeAndClose().size());
            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.child WHERE s2='newvalue'", Collections.emptyList(), new TransactionContext(tx)).consumeAndClose().size());
        } else if ("NO ACTION".equals(deleteAction)) {
            ForeignKeyViolationException errOnDelete = expectThrows(ForeignKeyViolationException.class, () -> {
                execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList(), new TransactionContext(tx));
            });
            assertEquals(fkName, errOnDelete.getForeignKeyName());

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList(), new TransactionContext(tx));
            execute(manager, "UPDATE tblspace1.parent set n1=983", Collections.emptyList(), new TransactionContext(tx));
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList(), new TransactionContext(tx));
        } else {
            fail();
        }

    }

    @Test
    public void cannotAlterColumnsWithChildTableRefs() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.parent (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.child (k2 string primary key,n2 int,"
                    + "s2 string, "
                    + "CONSTRAINT fk1 FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1) ON DELETE NO ACTION ON UPDATE NO ACTION)", Collections.emptyList());
            Table childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(1, childTable.foreignKeys.length);

            StatementExecutionException errCannotDrop = expectThrows(StatementExecutionException.class, () -> {
                execute(manager, "DROP TABLE tblspace1.parent", Collections.emptyList());
            });
            assertEquals("Cannot drop table tblspace1.parent because it has children tables: child", errCannotDrop.getMessage());

            StatementExecutionException errCannotDropColumn = expectThrows(StatementExecutionException.class, () -> {
                execute(manager, "ALTER TABLE tblspace1.parent DROP COLUMN n1", Collections.emptyList());
            });
            assertEquals("Cannot drop column parent.n1 because of foreign key constraint fk1 on table child", errCannotDropColumn.getMessage());
        }
    }

    @Test
    public void alterTableDropAndForeignKey() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.parent (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.child (k2 string primary key,n2 int,"
                    + "s2 string, "
                    + "CONSTRAINT fk1 FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1) ON DELETE NO ACTION ON UPDATE NO ACTION,"
                    + "CONSTRAINT fk2 FOREIGN KEY (s2) REFERENCES parent(k1) ON DELETE NO ACTION ON UPDATE NO ACTION)", Collections.emptyList());
            Table parentTable = manager.getTableSpaceManager("tblspace1").getTableManager("parent").getTable();
            Table childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(2, childTable.foreignKeys.length);
            assertEquals("fk1", childTable.foreignKeys[0].name);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2", "n2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1", "n1"}, childTable.foreignKeys[0].parentTableColumns);

            assertEquals("fk2", childTable.foreignKeys[1].name);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[1].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[1].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[1].parentTableId);
            assertArrayEquals(new String[]{"s2"}, childTable.foreignKeys[1].columns);
            assertArrayEquals(new String[]{"k1"}, childTable.foreignKeys[1].parentTableColumns);

            // test FK is working
            testChildSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1");
            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            testServerSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1", "NO ACTION", "NO ACTION");

            execute(manager, "ALTER TABLE tblspace1.child DROP CONSTRAINT fk1", Collections.emptyList());
            childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(1, childTable.foreignKeys.length);

            assertEquals("fk2", childTable.foreignKeys[0].name);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1"}, childTable.foreignKeys[0].parentTableColumns);

            // TRUCATE requires a checkpoint lock, we are also testing that the tables are free from global locks
            execute(manager, "TRUNCATE TABLE tblspace1.child", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            execute(manager, "TRUNCATE TABLE tblspace1.parent", Collections.emptyList(), TransactionContext.NO_TRANSACTION);

            execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('a',2,'pvalue')", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            // insert a record that could violate the old FK1 (but not FK2)
            execute(manager, "INSERT INTO tblspace1.child(k2,n2,s2) values('no',10,'a')", Collections.emptyList(), TransactionContext.NO_TRANSACTION);

            execute(manager, "TRUNCATE TABLE tblspace1.child", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            execute(manager, "TRUNCATE TABLE tblspace1.parent", Collections.emptyList(), TransactionContext.NO_TRANSACTION);

            // add the FK again
            execute(manager, "ALTER TABLE tblspace1.`CHILD` Add CONSTRAINT `fk3` FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1) ON DELETE RESTRICT", Collections.emptyList());

            testChildSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk2");
            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(2, childTable.foreignKeys.length);
            assertEquals("fk2", childTable.foreignKeys[0].name);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1"}, childTable.foreignKeys[0].parentTableColumns);

            assertEquals("fk3", childTable.foreignKeys[1].name);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[1].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[1].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[1].parentTableId);
            assertArrayEquals(new String[]{"s2", "n2"}, childTable.foreignKeys[1].columns);
            assertArrayEquals(new String[]{"k1", "n1"}, childTable.foreignKeys[1].parentTableColumns);

            testServerSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk2", "NO ACTION", "NO ACTION");

        }
    }

    @Test
    public void alterTableCannotAddViolatedForeignKey() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.parent (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.child (k2 string primary key,n2 int,s2 string)", Collections.emptyList());


            execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('a',2,'pvalue')", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            // insert a record that could violates the new FK1
            execute(manager, "INSERT INTO tblspace1.child(k2,n2,s2) values('no',10,'a')", Collections.emptyList(), TransactionContext.NO_TRANSACTION);

            // add the FK, it must fail
            ForeignKeyViolationException err = expectThrows(ForeignKeyViolationException.class, () -> {
                execute(manager, "ALTER TABLE tblspace1.`CHILD` Add CONSTRAINT `fk1` FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1) ON DELETE RESTRICT", Collections.emptyList());
            });
            assertEquals("fk1", err.getForeignKeyName());

            // check that the fk is not added
            Table parentTable = manager.getTableSpaceManager("tblspace1").getTableManager("parent").getTable();
            Table childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertNull(childTable.foreignKeys);

            // fix the record, in transaction
            long tx = beginTransaction(manager, "tblspace1");
            execute(manager, "UPDATE tblspace1.child set s2='a',n2=2", Collections.emptyList(), new TransactionContext(tx));

            assertNotNull(manager.getTableSpaceManager("tblspace1").getTransaction(tx));
            execute(manager, "ALTER TABLE tblspace1.`CHILD` Add CONSTRAINT `fk1` FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1) ON DELETE RESTRICT", Collections.emptyList(), new TransactionContext(tx));

            // transaction is committed automatically after an ALTER TABLE operation
            assertNull(manager.getTableSpaceManager("tblspace1").getTransaction(tx));

            // check that the fk is not present
            childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(1, childTable.foreignKeys.length);

            assertEquals("fk1", childTable.foreignKeys[0].name);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2", "n2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1", "n1"}, childTable.foreignKeys[0].parentTableColumns);

        }
    }

    @Test
    public void createTableWithOnDeleteCascade() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.parent (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.child (k2 string primary key,n2 int,"
                    + "s2 string, "
                    + "CONSTRAINT fk1 FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1) ON DELETE CASCADE ON UPDATE NO ACTION)", Collections.emptyList());
            Table parentTable = manager.getTableSpaceManager("tblspace1").getTableManager("parent").getTable();

            Table childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(1, childTable.foreignKeys.length);
            assertEquals("fk1", childTable.foreignKeys[0].name);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_CASCADE, childTable.foreignKeys[0].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2", "n2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1", "n1"}, childTable.foreignKeys[0].parentTableColumns);

            testChildSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1"); // test without transaction

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            long tx = beginTransaction(manager, "tblspace1");
            testChildSideOfForeignKey(manager, tx, "fk1");  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            testServerSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1", "NO ACTION", "CASCADE"); // test without transaction

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            tx = beginTransaction(manager, "tblspace1");
            testServerSideOfForeignKey(manager, tx, "fk1", "NO ACTION", "CASCADE");  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

        }
    }

    @Test
    public void createTableWithOnDeleteSetNull() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.parent (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.child (k2 string primary key,n2 int,"
                    + "s2 string, "
                    + "CONSTRAINT fk1 FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1) ON DELETE SET NULL ON UPDATE NO ACTION)", Collections.emptyList());
            Table parentTable = manager.getTableSpaceManager("tblspace1").getTableManager("parent").getTable();

            Table childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(1, childTable.foreignKeys.length);
            assertEquals("fk1", childTable.foreignKeys[0].name);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_SETNULL, childTable.foreignKeys[0].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2", "n2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1", "n1"}, childTable.foreignKeys[0].parentTableColumns);

            testChildSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1"); // test without transaction

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            long tx = beginTransaction(manager, "tblspace1");
            testChildSideOfForeignKey(manager, tx, "fk1");  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            testServerSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1", "NO ACTION", "SET NULL"); // test without transaction

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            tx = beginTransaction(manager, "tblspace1");
            testServerSideOfForeignKey(manager, tx, "fk1", "NO ACTION", "SET NULL");  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

        }
    }

    @Test
    public void createTableWithOnUpdateSetNull() throws Exception {
        assumeTrue("only jsqlparser 4.x support ON UPDATE set NULL", false);
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.parent (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.child (k2 string primary key,n2 int,"
                    + "s2 string, "
                    + "CONSTRAINT fk1 FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1) ON DELETE NO ACTION ON UPDATE SET NULL)", Collections.emptyList());
            Table parentTable = manager.getTableSpaceManager("tblspace1").getTableManager("parent").getTable();

            Table childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(1, childTable.foreignKeys.length);
            assertEquals("fk1", childTable.foreignKeys[0].name);
            assertEquals(ForeignKeyDef.ACTION_SETNULL, childTable.foreignKeys[0].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2", "n2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1", "n1"}, childTable.foreignKeys[0].parentTableColumns);

            testChildSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1"); // test without transaction

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            long tx = beginTransaction(manager, "tblspace1");
            testChildSideOfForeignKey(manager, tx, "fk1");  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            testServerSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, "fk1", "SET NULL", "NO ACTION"); // test without transaction

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            tx = beginTransaction(manager, "tblspace1");
            testServerSideOfForeignKey(manager, tx, "fk1", "SET NULL", "NO ACTION");  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

        }
    }

     @Test
    public void alterAddUnnamedForeignKey() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.parent (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.child (k2 string primary key,n2 int,"
                    + "s2 string)", Collections.emptyList());
            execute(manager, "ALTER TABLE tblspace1.child ADD FOREIGN KEY (s2,n2) REFERENCES parent(k1,n1)", Collections.emptyList());
            Table parentTable = manager.getTableSpaceManager("tblspace1").getTableManager("parent").getTable();
            Table childTable = manager.getTableSpaceManager("tblspace1").getTableManager("child").getTable();
            assertEquals(1, childTable.foreignKeys.length);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onUpdateAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onDeleteAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2", "n2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1", "n1"}, childTable.foreignKeys[0].parentTableColumns);

            // test FK is working
            testChildSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID, childTable.foreignKeys[0].name);
        }
    }

}
