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

import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.sql.TranslatedQuery;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class CreateTableTest {

    @Test
    public void createTable1() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            CreateTableStatement st2IfNotExists = new CreateTableStatement(table, Collections.emptyList(), true);
            manager.executeStatement(st2IfNotExists, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void weThrowExceptionWhenNullableDataTypeDoubleUsedasPk() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.DOUBLE)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        }
    }

    @Test(expected = StatementExecutionException.class)
    public void weThrowExceptionOnInsertingNullInNonNullColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.NOTNULL_STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated1 = manager.getPlanner().translate("tblspace1", "INSERT INTO t1 (id,name) values(?,?)", Arrays.asList("test", "test1"), true, true, false, -1);
            manager.executePlan(translated1.plan, translated1.context, TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated = manager.getPlanner().translate("tblspace1", "INSERT INTO t1 (id,name) values(?,?)", Arrays.asList("test", null), true, true, false, -1);
            manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION);
        }
    }


    @Test
    public void weThrowExceptionOnInsertingNullInNonNullColumnOnAutoIncrementPrimaryKey() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t11")
                    .column("id", ColumnTypes.LONG)
                    .column("firstname", ColumnTypes.STRING)
                    .column("lastname", ColumnTypes.NOTNULL_STRING)
                    .primaryKey("id", true)
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated1 = manager.getPlanner().translate("tblspace1", "INSERT INTO t11 (firstname, lastname) values(?,?)", Arrays.asList("Joe", "cool"), true, true, false, -1);
            manager.executePlan(translated1.plan, translated1.context, TransactionContext.NO_TRANSACTION);

            TranslatedQuery translated = manager.getPlanner().translate("tblspace1", "INSERT INTO t11 (firstname) values(?)", Arrays.asList("test"), true, true, false, -1);
            manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Column 'lastname' has no default value and does not allow NULLs"));
        }
    }

    @Test(expected = StatementExecutionException.class)
    public void weThrowExceptionOnUpdatingNullInNonNullColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.NOTNULL_STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);

            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            //First insert items in the table.
            TranslatedQuery queryInsert = manager.getPlanner().translate("tblspace1", "INSERT INTO t1 (id,name) values(?,?)", Arrays.asList("test", "12345"), true, true, false, -1);
            manager.executePlan(queryInsert.plan, queryInsert.context, TransactionContext.NO_TRANSACTION);

            TranslatedQuery queryUpdate1 = manager.getPlanner().translate("tblspace1", "Update t1 set name=? where id=?", Arrays.asList("54321", "test"), true, true, false, -1);
            manager.executePlan(queryUpdate1.plan, queryUpdate1.context, TransactionContext.NO_TRANSACTION);

            // Try to update a non null string.
            TranslatedQuery queryUpdate = manager.getPlanner().translate("tblspace1", "Update t1 set name=? where id=?", Arrays.asList(null, "test"), true, true, false, -1);
            manager.executePlan(queryUpdate.plan, queryUpdate.context, TransactionContext.NO_TRANSACTION);
        }
    }

}
