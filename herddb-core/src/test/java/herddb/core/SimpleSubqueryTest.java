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
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;

/**
 *
 *
 * @author enrico.olivelli
 */
public class SimpleSubqueryTest {

    @Test
    public void tableAliasTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null, null);) {
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
                    + "", Collections.emptyList()).consume().size());

            assertEquals(1, scan(manager, "SELECT t1.k1 "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1='mykey2'"
                    + "", Collections.emptyList()).consume().size());

            try {
                scan(manager, "SELECT t2.k1 "
                        + "FROM tblspace1.table1 t1", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {
                assertEquals("invalid column name k1 invalid table name t2, expecting t1", ok.getMessage());
            }
            try {
                scan(manager, "SELECT k1 "
                        + "FROM tblspace1.table1 t1 "
                        + "WHERE t2.n1=123", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {                
                assertEquals("invalid column name n1 invalid table name t2, expecting t1", ok.getMessage());

            }
            try {
                scan(manager, "SELECT k1 "
                        + "FROM tblspace1.table1 t1 "
                        + "WHERE t2.k1='aaa'", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {
                assertEquals("invalid column name k1 invalid table name t2, expecting t1", ok.getMessage());

            }

            try {
                scan(manager, "SELECT * "
                        + "FROM tblspace1.table1 t2 "
                        + "WHERE t1.k1='mykey2'", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {
                assertEquals("invalid column name k1 invalid table name t1, expecting t2", ok.getMessage());
            }

            try {
                scan(manager, "SELECT * "
                        + "FROM tblspace1.table1 t2 "
                        + "ORDER BY t1.n1", Collections.emptyList());
                fail("query must not work");
            } catch (Exception ok) {
                assertEquals("invalid column name n1 invalid table name t1, expecting t2", ok.getMessage());
            }

        }
    }

    @Test
    public void subQueryOnWhereTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null, null);) {
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
                    + "", Collections.emptyList()).consume().size());

            assertEquals(1, scan(manager, "SELECT * "
                    + "FROM tblspace1.table1 t1 "
                    + "WHERE t1.k1 in (SELECT fk FROM tblspace1.table2 WHERE k2='subkey4')"
                    + "", Collections.emptyList()).consume().size());

        }
    }

}
