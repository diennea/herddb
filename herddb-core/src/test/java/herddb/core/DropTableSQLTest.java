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
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableDoesNotExistException;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.sql.CalcitePlanner;
import herddb.sql.DDLSQLPlanner;
import herddb.utils.DataAccessor;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class DropTableSQLTest {

    @Test
    public void dropTableNoTransaction() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='tsql'", Collections.emptyList());) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
            }
            execute(manager, "DROP TABLE tblspace1.tsql", Collections.emptyList());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='tsql'", Collections.emptyList());) {
                assertTrue(scan.consume().isEmpty());
            }
            execute(manager, "CREATE TABLE tsql2 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            try (DataScanner scan = scan(manager, "SELECT * FROM systables where table_name='tsql2'", Collections.emptyList());) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
            }
            execute(manager, "DROP TABLE tsql2", Collections.emptyList());
            try (DataScanner scan = scan(manager, "SELECT * FROM systables where table_name='tsql2'", Collections.emptyList());) {
                assertTrue(scan.consume().isEmpty());
            }

        }

    }

    @Test
    public void dropTableWithTransaction() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            long tx = ((TransactionResult) execute(manager, "EXECUTE begintransaction 'tblspace1'", Collections.emptyList())).getTransactionId();

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList(), new TransactionContext(tx));
            execute(manager, "INSERT INTO tblspace1.tsql (k1) values('a')", Collections.emptyList(), new TransactionContext(tx));

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList());) {
                fail();
            } catch (TableDoesNotExistException ok) {
            }

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='tsql'", Collections.emptyList());) {
                List<DataAccessor> all = scan.consume();
                assertEquals(0, all.size());
            }

            execute(manager, "EXECUTE committransaction 'tblspace1'," + tx, Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList());) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
            }

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='tsql'", Collections.emptyList());) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
            }

            long tx2 = ((TransactionResult) execute(manager, "EXECUTE begintransaction 'tblspace1'", Collections.emptyList())).getTransactionId();

            execute(manager, "DROP TABLE tblspace1.tsql", Collections.emptyList(), new TransactionContext(tx2));

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList());) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
            }

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='tsql'", Collections.emptyList());) {
                List<DataAccessor> all = scan.consume();
                assertEquals(1, all.size());
            }
            execute(manager, "EXECUTE committransaction 'tblspace1'," + tx2, Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where table_name='tsql'", Collections.emptyList());) {
                List<DataAccessor> all = scan.consume();
                assertEquals(0, all.size());
            }

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList());) {
                fail();
            } catch (TableDoesNotExistException ok) {
                assertTrue(manager.getPlanner() instanceof DDLSQLPlanner);
            } catch (StatementExecutionException ok) {
                assertEquals("From line 1, column 15 to line 1, column 28: Object 'TSQL' not found within 'tblspace1'", ok.getMessage());
                assertTrue(manager.getPlanner() instanceof CalcitePlanner);
            }

        }

    }
}
