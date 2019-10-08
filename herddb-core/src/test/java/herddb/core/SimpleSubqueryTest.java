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
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
 * @author enrico.olivelli
 */
public class SimpleSubqueryTest {

    @Test
    public void tableAliasTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.table1 (k1 string primary key,n1 int)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey4", Integer.valueOf(1234))).getUpdateCount());

            assertEquals(1, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1='mykey2'"
                    + "", Collections.emptyList()).consumeAndClose().size());

            assertEquals(1, scan(manager, "SELECT t1.k1 "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1='mykey2'"
                    + "", Collections.emptyList()).consumeAndClose().size());

            try {
                scan(manager, "SELECT t2.k1 "
                        + "FROM tblspace1.table1 t1", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {
                assertTrue("invalid column name k1 invalid table name t2, expecting t1".equals(ok.getMessage())
                        || ok.getMessage().contains("Table 'T2' not found"));
            }
            try {
                scan(manager, "SELECT k1 "
                        + "FROM tblspace1.table1 t1 "
                        + "WHERE t2.n1=123", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {
                assertTrue("invalid column name n1 invalid table name t2, expecting t1".equals(ok.getMessage())
                        || ok.getMessage().contains("Table 'T2' not found"));
            }
            try {
                scan(manager, "SELECT k1 "
                        + "FROM tblspace1.table1 t1 "
                        + "WHERE t2.k1='aaa'", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {
                assertTrue("invalid column name k1 invalid table name t2, expecting t1".equals(ok.getMessage())
                        || ok.getMessage().contains("Table 'T2' not found"));

            }

            try {
                scan(manager, "SELECT * "
                        + "FROM tblspace1.table1 t2 "
                        + "WHERE t1.k1='mykey2'", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {
                assertTrue("invalid column name k1 invalid table name t1, expecting t2".equals(ok.getMessage())
                        || ok.getMessage().contains("Table 'T1' not found"));

            }

            try {
                scan(manager, "SELECT * "
                        + "FROM tblspace1.table1 t2 "
                        + "ORDER BY t1.n1", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {
                assertTrue("invalid column name n1 invalid table name t1, expecting t2".equals(ok.getMessage())
                        || ok.getMessage().contains("Table 'T1' not found"));

            }

        }
    }

    @Test
    public void subQueryOnWhereTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.table1 (k1 string primary key,n1 int)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.table2 (k2 string primary key,fk string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey4", Integer.valueOf(1234))).getUpdateCount());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table2(k2,fk) values(?,?)", Arrays.asList("subkey1", "mykey2")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table2(k2,fk) values(?,?)", Arrays.asList("subkey2", "mykey2")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table2(k2,fk) values(?,?)", Arrays.asList("subkey3", "mykey3")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table2(k2,fk) values(?,?)", Arrays.asList("subkey4", "mykey4")).getUpdateCount());

            assertEquals(2, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1 in ('mykey','mykey3')"
                    + "", Collections.emptyList()).consumeAndClose().size());

            assertEquals(1, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2='subkey4')"
                    + "", Collections.emptyList()).consumeAndClose().size());

            assertEquals(1, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.n1 = ? and t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2=?)"
                    + "", Arrays.asList(1234, "subkey4")).consumeAndClose().size());

            assertEquals(0, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.n1 = ? and t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2=?)"
                    + "", Arrays.asList(1234, "subkey5")).consumeAndClose().size());

        }
    }

    @Test
    public void deleteWithSubQueryTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.table1 (k1 string primary key,n1 int)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.table2 (k2 string primary key,fk string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey3", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table1(k1,n1) values(?,?)", Arrays.asList("mykey4", Integer.valueOf(1238))).getUpdateCount());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table2(k2,fk) values(?,?)", Arrays.asList("subkey1", "mykey2")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table2(k2,fk) values(?,?)", Arrays.asList("subkey2", "mykey2")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table2(k2,fk) values(?,?)", Arrays.asList("subkey3", "mykey3")).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.table2(k2,fk) values(?,?)", Arrays.asList("subkey4", "mykey4")).getUpdateCount());

            assertEquals(2, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1 in ('mykey','mykey3')"
                    + "", Collections.emptyList()).consumeAndClose().size());

            assertEquals(1, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2='subkey4')"
                    + "", Arrays.asList("mykey4")).consumeAndClose().size());

            assertEquals(1, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.n1=1238 and t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2='subkey4')"
                    + "", Arrays.asList("mykey4")).consumeAndClose().size());

            assertEquals(1, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.n1=? and t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2='subkey4')"
                    + "", Arrays.asList(1238, "mykey4")).consumeAndClose().size());

            assertEquals(0, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.n1=? and t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2='subkey4')"
                    + "", Arrays.asList(124, "mykey4")).consumeAndClose().size());

            assertEquals(1, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2=?)"
                    + "", Arrays.asList("subkey4")).consumeAndClose().size());

            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.table1 set n1=1000"
                    + "WHERE k1 in (SELECT fk FROM tblspace1.table2 WHERE k2=?)"
                    + "", Arrays.asList("subkey4")).getUpdateCount());

            assertEquals(1, executeUpdate(manager, "DELETE  "
                    + "FROM tblspace1.table1 "
                    + "WHERE k1 in (SELECT fk FROM tblspace1.table2 WHERE k2=?)"
                    + "", Arrays.asList("subkey4")).getUpdateCount());

            assertEquals(0, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2=?)"
                    + "", Arrays.asList("subkey4")).consumeAndClose().size());

        }
    }

}
