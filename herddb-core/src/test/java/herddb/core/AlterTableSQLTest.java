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
import herddb.model.Tuple;
import herddb.model.commands.CreateTableSpaceStatement;

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
                List<Tuple> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).fieldNames.length);
            }
            execute(manager, "ALTER TABLE tblspace1.tsql add column k2 string", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1,s1,k2) values('b',1,'b','c')", Collections.emptyList());
            {
                List<Tuple> tuples = scan(manager, "SELECT * FROM tblspace1.tsql WHERE k2='c'", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(4, tuples.get(0).fieldNames.length);
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
                List<Tuple> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).fieldNames.length);
                assertEquals("b", tuples.get(0).get("s1"));
            }
            execute(manager, "ALTER TABLE tblspace1.tsql drop column s1", Collections.emptyList());
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(2, table.columns.length);

            {
                List<Tuple> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(2, tuples.get(0).fieldNames.length);
                assertEquals(null, tuples.get(0).get("s1"));
            }

            execute(manager, "ALTER TABLE tblspace1.tsql add column s1 string", Collections.emptyList());
            table = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable();
            assertEquals(0, table.getColumn("k1").serialPosition);
            assertEquals(1, table.getColumn("n1").serialPosition);
            assertEquals(3, table.getColumn("s1").serialPosition);
            {
                List<Tuple> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).fieldNames.length);
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

}
