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

import herddb.codec.RecordSerializer;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.PrimaryKeyIndexSeekPredicate;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.Tuple;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class AlterTableSQLTest {

    private DMLStatementExecutionResult executeUpdate(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters, true, true);
        return (DMLStatementExecutionResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION);
    }

    private StatementExecutionResult execute(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters, true, true);
        return manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION);
    }

    private DataScanner scan(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters, true, true);
        return ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner;
    }

    @Test
    public void addColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
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

        }
    }

    @Test
    public void dropColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1,s1) values('a',1,'b')", Collections.emptyList());
            {
                List<Tuple> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(3, tuples.get(0).fieldNames.length);
                assertEquals("b", tuples.get(0).get("s1"));
            }
            execute(manager, "ALTER TABLE tblspace1.tsql drop column s1", Collections.emptyList());
            
            {
                List<Tuple> tuples = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consume();
                assertEquals(1, tuples.size());
                assertEquals(2, tuples.get(0).fieldNames.length);
                assertEquals(null, tuples.get(0).get("s1"));
            }
            
            execute(manager, "ALTER TABLE tblspace1.tsql add column s1 string", Collections.emptyList());
            
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
