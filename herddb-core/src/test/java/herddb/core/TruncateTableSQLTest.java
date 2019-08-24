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
import static org.junit.Assert.fail;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableDoesNotExistException;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Collections;
import org.junit.Test;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class TruncateTableSQLTest {

    @Test
    public void truncateTableNoTransaction() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE BRIN INDEX test1 ON tblspace1.tsql (k1)", Collections.emptyList());
            execute(manager, "CREATE HASH INDEX test2 ON tblspace1.tsql (k1)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1) values('a')", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            } catch (TableDoesNotExistException ok) {
            }
            execute(manager, "TRUNCATE TABLE tblspace1.tsql", Collections.emptyList());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList())) {
                assertEquals(0, scan.consume().size());
            } catch (TableDoesNotExistException ok) {
            }

        }

    }

    @Test
    public void truncateTableTransactionTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE BRIN INDEX test1 ON tblspace1.tsql (k1)", Collections.emptyList());
            execute(manager, "CREATE HASH INDEX test2 ON tblspace1.tsql (k1)", Collections.emptyList());
            long tx1 = TestUtils.beginTransaction(manager, "tblspace1");
            try {
                // forbidden, transactions not allowed
                execute(manager, "truncate TABLE tblspace1.tsql", Collections.emptyList(), new TransactionContext(tx1));
                fail();
            } catch (StatementExecutionException ok) {
                assertEquals("TRUNCATE TABLE cannot be executed within the context of a Transaction", ok.getMessage());
            }

            long txId = execute(manager, "INSERT INTO tblspace1.tsql (k1) values('a')",
                    Collections.emptyList(), TransactionContext.AUTOTRANSACTION_TRANSACTION).transactionId;

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList(), new TransactionContext(txId))) {
                assertEquals(1, scan.consume().size());
            }
            try {
                // forbidden, a transaction is running on table
                execute(manager, "TRUNCATE TABLE tblspace1.tsql", Collections.emptyList(), TransactionContext.NO_TRANSACTION);
                fail();
            } catch (StatementExecutionException ok) {
                assertEquals("TRUNCATE TABLE cannot be executed table tsql: at least one transaction is pending on it",
                        ok.getCause().getMessage());
            }
            TestUtils.commitTransaction(manager, "tblspace1", txId);

            execute(manager, "TRUNCATE TABLE tblspace1.tsql", Collections.emptyList(), TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql ", Collections.emptyList())) {
                assertEquals(0, scan.consume().size());
            }

        }

    }

}
