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

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static herddb.model.TransactionContext.NO_TRANSACTION;
import static org.junit.Assert.assertEquals;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

/**
 * @author diego.salvi
 */
public class UpdateTest {

    @Test
    public void updateWithJDBC() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1,s1))", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey2", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey3", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey4", "a", Integer.valueOf(1234))).getUpdateCount());

            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = n1 + ? WHERE k1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = n1 - ? WHERE k1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = ? + n1 WHERE k1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = ? + n1 WHERE k1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey")).getUpdateCount());

            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = ? + n1 WHERE k1 = ? AND s1 = ?", Arrays.asList(Integer.valueOf(1234), "mykey", "a")).getUpdateCount());
        }
    }

    /**
     * Check update then select (#320, not every row update really was run).
     *
     * @author diego.salvi
     */
    @Test
    public void updateThenSelect() throws Exception {

        int runs = 50;
        int inserts = 100;

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            for (int run = 0; run < runs; ++run) {

                execute(manager, "CREATE TABLE tblspace1.tsql (k1 int, n1 int, primary key(k1))", Collections.emptyList());

                for (int i = 0; i < inserts; ++i) {
                    assertEquals(1, executeUpdate(manager,
                            "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)",
                            Arrays.asList(Integer.valueOf(i), Integer.valueOf(1))).getUpdateCount());
                }

                long tx = beginTransaction(manager, "tblspace1");

                TransactionContext ctx = new TransactionContext(tx);

                assertEquals(inserts, executeUpdate(manager,
                        "UPDATE tblspace1.tsql set n1 = 100",
                        Collections.emptyList(), ctx).getUpdateCount());

                for (int i = 0; i < inserts; ++i) {
                    assertEquals(1, scan(manager,
                            "SELECT k1, n1 FROM tblspace1.tsql WHERE k1 = ? AND n1 = 100",
                            Arrays.asList(Integer.valueOf(i)), ctx).consumeAndClose().size());
                }

                for (int i = 0; i < inserts; ++i) {
                    try {
                        assertEquals(1, scan(manager,
                                "SELECT k1, n1 FROM tblspace1.tsql WHERE k1 = ? AND n1 = 100",
                                Arrays.asList(Integer.valueOf(i)), ctx).consumeAndClose().size());
                    } catch (AssertionError e) {

                        throw e;
                    }
                }

                commitTransaction(manager, "tblspace1", tx);

                for (int i = 0; i < inserts; ++i) {
                    assertEquals(1, scan(manager,
                            "SELECT k1, n1 FROM tblspace1.tsql WHERE k1 = ? AND n1 = 100",
                            Arrays.asList(Integer.valueOf(i), ctx)).consumeAndClose().size());
                }

                execute(manager, "DROP TABLE tblspace1.tsql", Collections.emptyList());

            }

        }
    }
}
