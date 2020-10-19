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

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.dump;
import static herddb.core.TestUtils.execute;
import static herddb.utils.TestUtils.expectThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onUpdateCascadeAction);
            assertEquals(ForeignKeyDef.ACTION_NO_ACTION, childTable.foreignKeys[0].onDeleteCascadeAction);
            assertEquals(parentTable.uuid, childTable.foreignKeys[0].parentTableId);
            assertArrayEquals(new String[]{"s2", "n2"}, childTable.foreignKeys[0].columns);
            assertArrayEquals(new String[]{"k1", "n1"}, childTable.foreignKeys[0].parentTableColumns);

            testChildSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID); // test without transaction

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            long tx = beginTransaction(manager, "tblspace1");
            testChildSideOfForeignKey(manager, tx);  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            testServerSideOfForeignKey(manager, TransactionContext.NOTRANSACTION_ID); // test without transaction


            execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList());
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList());

            tx = beginTransaction(manager, "tblspace1");
            testServerSideOfForeignKey(manager, tx);  // test with transaction
            TestUtils.commitTransaction(manager, "tblspace1", tx);

        }
    }

    private void testChildSideOfForeignKey(final DBManager manager, long tx) throws DataScannerException, StatementExecutionException {
        ForeignKeyViolationException err = expectThrows(ForeignKeyViolationException.class, () -> {
            execute(manager, "INSERT INTO tblspace1.child(k2,n2,s2) values('a',2,'pvalue')", Collections.emptyList(), new TransactionContext(tx));
        });
        assertEquals("fk1", err.getForeignKeyName());

        execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('a',2,'pvalue')", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "INSERT INTO tblspace1.child(k2,n2,s2) values('c1',2,'a')", Collections.emptyList(), new TransactionContext(tx));

        ForeignKeyViolationException errOnUpdate = expectThrows(ForeignKeyViolationException.class, () -> {
            execute(manager, "UPDATE tblspace1.child set s2='badvalue'", Collections.emptyList(), new TransactionContext(tx));
        });
        assertEquals("fk1", errOnUpdate.getForeignKeyName());

        execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('newvalue',2,'foo')", Collections.emptyList(), new TransactionContext(tx));
        dump(manager, "SELECT * FROM tblspace1.parent", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "UPDATE tblspace1.child set s2='newvalue'", Collections.emptyList(), new TransactionContext(tx));
    }


    private void testServerSideOfForeignKey(final DBManager manager, long tx) throws DataScannerException, StatementExecutionException {
        execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('a',2,'pvalue')", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "INSERT INTO tblspace1.parent(k1,n1,s1) values('newvalue',2,'foo')", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "INSERT INTO tblspace1.child(k2,n2,s2) values('c1',2,'a')", Collections.emptyList(), new TransactionContext(tx));

        ForeignKeyViolationException errOnUpdate = expectThrows(ForeignKeyViolationException.class, () -> {
            execute(manager, "UPDATE tblspace1.parent set n1=983", Collections.emptyList(), new TransactionContext(tx));
        });
        assertEquals("fk1", errOnUpdate.getForeignKeyName());

        ForeignKeyViolationException errOnDelete = expectThrows(ForeignKeyViolationException.class, () -> {
            execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList(), new TransactionContext(tx));
        });
        assertEquals("fk1", errOnDelete.getForeignKeyName());

        execute(manager, "DELETE FROM tblspace1.child", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "UPDATE tblspace1.parent set n1=983", Collections.emptyList(), new TransactionContext(tx));
        execute(manager, "DELETE FROM tblspace1.parent", Collections.emptyList(), new TransactionContext(tx));

    }

}
