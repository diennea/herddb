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
package herddb.data.consistency;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.core.ReplicatedLogtestcase;
import herddb.core.TestUtils;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataConsistencyStatementResult;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.TableConsistencyCheckStatement;
import herddb.model.commands.TableSpaceConsistencyCheckStatement;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;


/**
 * Consistency check test case
 * @author Hamado.Dene
 */
public class TableDataCheckSumTest extends ReplicatedLogtestcase {

    @Test
    public void consistencyCheckReplicaTest() throws Exception {
        final String tableName = "table1";
        final String tableSpaceName = "t2";

        try (DBManager manager1 = startDBManager("node1")) {
            manager1.executeStatement(new CreateTableSpaceStatement(tableSpaceName, new HashSet<>(Arrays.asList("node1", "node2")), "node1", 2, 0, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager1.waitForTablespace(tableSpaceName, 10000, true);
            try (DBManager manager2 = startDBManager("node2")) {
                manager2.waitForTablespace(tableSpaceName, 10000, false);
                Table table = Table
                    .builder()
                    .tablespace(tableSpaceName)
                    .name(tableName)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.BOOLEAN)
                    .primaryKey("id")
                    .build();

                CreateTableStatement st = new CreateTableStatement(table);
                manager1.executeStatement(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                manager1.waitForTable(tableSpaceName, tableName, 10000, true);
                execute(manager1, "INSERT INTO t2.table1 (id,name) values (?,?)", Arrays.asList("1", true));
                execute(manager1, "INSERT INTO t2.table1 (id,name) values (?,?)", Arrays.asList("2", false));
                manager2.waitForTable(tableSpaceName, tableName, 10000, false);

                TableConsistencyCheckStatement statement = new TableConsistencyCheckStatement("table1", "t2");
                DataConsistencyStatementResult result = manager1.createTableCheckSum(statement, null);
                assertTrue("Consistency check replicated test ", result.getOk());
            }
        }
    }

    @Test
    public void consistencyCheckSyntax() throws Exception {

        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            execute(manager, "CREATE TABLESPACE 'consistence'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE consistence.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE consistence.tsql1 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE consistence.tsql2 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO consistence.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
                execute(manager, "INSERT INTO consistence.tsql1 (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
                execute(manager, "INSERT INTO consistence.tsql2 (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM consistence.tsql", Collections.emptyList())) {
                DataAccessor first = scan.consume().get(0);
                Number count = (Number) first.get(first.getFieldNames()[0]);
                assertEquals(10, count.intValue());
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM consistence.tsql1", Collections.emptyList())) {
                DataAccessor first = scan.consume().get(0);
                Number count = (Number) first.get(first.getFieldNames()[0]);
                assertEquals(10, count.intValue());
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM consistence.tsql2", Collections.emptyList())) {
                DataAccessor first = scan.consume().get(0);
                Number count = (Number) first.get(first.getFieldNames()[0]);
                assertEquals(10, count.intValue());
            }
            execute(manager, "tableconsistencycheck consistence.tsql1", Collections.emptyList());
            execute(manager, "tableconsistencycheck consistence.tsql2", Collections.emptyList());
            execute(manager, "tablespaceconsistencycheck consistence", Collections.emptyList());
            execute(manager, "tablespaceconsistencycheck 'consistence'", Collections.emptyList());

        }
    }


    @Test
    public void emptyTableConsistencyCheck() throws Exception {
        String tableSpaceName = "tblspace1";
        String tableName = "t1";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            Table table = Table
                .builder()
                .tablespace(tableSpaceName)
                .name(tableName)
                .column("k1", ColumnTypes.STRING)
                .column("n1", ColumnTypes.INTEGER)
                .column("s1", ColumnTypes.STRING)
                .primaryKey("k1")
                .build();

            CreateTableStatement st = new CreateTableStatement(table);
            manager.executeStatement(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            //check empty table
            TableConsistencyCheckStatement statement = new TableConsistencyCheckStatement(tableName, tableSpaceName);
            DataConsistencyStatementResult result = manager.createTableCheckSum(statement, null);
            assertTrue("Check empty table ", result.getOk());
        }
    }

    @Test
    public void tableConsistencyCheckWithNullValue() throws Exception {
        String tableSpaceName = "tblspace1";
        String tableName = "t1";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            Table table = Table
                .builder()
                .tablespace(tableSpaceName)
                .name(tableName)
                .column("k1", ColumnTypes.STRING)
                .column("n1", ColumnTypes.INTEGER)
                .column("s1", ColumnTypes.STRING)
                .primaryKey("k1")
                .build();

            CreateTableStatement st = new CreateTableStatement(table);
            manager.executeStatement(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            execute(manager, "INSERT INTO " + tableSpaceName + "." + tableName + " (k1,n1 ,s1) values (?,?,?)", Arrays.asList("1", null, "b"));
            execute(manager, "INSERT INTO " + tableSpaceName + "." + tableName + " (k1,n1 ,s1) values (?,?,?)", Arrays.asList("2", null, "b"));
            TableConsistencyCheckStatement statement = new TableConsistencyCheckStatement(tableName, tableSpaceName);
            DataConsistencyStatementResult result = manager.createTableCheckSum(statement, null);
            assertTrue("Check table with null value ", result.getOk());
        }
    }
    @Test
    public void tableSpaceConsistencyCheck() throws Exception {
        String tableSpaceName = "tblspace1";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            manager.waitForTablespace(tableSpaceName, 10000, false);
            Table table = Table
                .builder()
                .tablespace(tableSpaceName)
                .name("t1")
                .column("id", ColumnTypes.STRING)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

            Table table2 = Table
                .builder()
                .tablespace(tableSpaceName)
                .name("t2")
                .column("id", ColumnTypes.STRING)
                .column("name", ColumnTypes.BOOLEAN)
                .primaryKey("id")
                .build();
            CreateTableStatement st = new CreateTableStatement(table);
            manager.executeStatement(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            CreateTableStatement st2 = new CreateTableStatement(table2);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            execute(manager, "INSERT INTO tblspace1.t1 (id,name) values (?,?)", Arrays.asList("1", true));
            execute(manager, "INSERT INTO tblspace1.t1 (id,name) values (?,?)", Arrays.asList("2", false));
            execute(manager, "INSERT INTO tblspace1.t2 (id,name) values (?,?)", Arrays.asList("1", true));
            execute(manager, "INSERT INTO tblspace1.t2 (id,name) values (?,?)", Arrays.asList("2", false));

            TableSpaceConsistencyCheckStatement statement = new TableSpaceConsistencyCheckStatement(tableSpaceName);
            DataConsistencyStatementResult result = manager.createTableSpaceCheckSum(statement);
            assertTrue("Check tableSpace  ", result.getOk());
        }
    }

    @Test
    public void tableWithNullValueConsistencyCheck() throws Exception {
        String tableSpaceName = "t1";
        String tableName = "nulltable";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            execute(manager, "CREATE TABLESPACE 't1'", Collections.emptyList());
            manager.waitForTablespace(tableSpaceName, 10000);
            Table table = Table
                .builder()
                .tablespace(tableSpaceName)
                .name(tableName)
                .column("k1", ColumnTypes.STRING)
                .column("n1", ColumnTypes.INTEGER)
                .column("s1", ColumnTypes.STRING)
                .primaryKey("k1")
                .build();

            CreateTableStatement st = new CreateTableStatement(table);
            manager.executeStatement(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            execute(manager, "INSERT INTO " + tableSpaceName + "." + tableName + " (k1,n1 ,s1) values (?,?,?)", Arrays.asList("kk", null, null));
            TableConsistencyCheckStatement statement = new TableConsistencyCheckStatement(tableName, tableSpaceName);
            DataConsistencyStatementResult result = manager.createTableCheckSum(statement, null);
            assertTrue("Check table with null value ", result.getOk());
        }
    }
}
