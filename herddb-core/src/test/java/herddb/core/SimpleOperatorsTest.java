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
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

/**
 * @author francesco.caliumi
 */
public class SimpleOperatorsTest {

    @Test
    public void simpleArithmeticOperationsTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key, n1 int, l1 long, t1 timestamp, nu string, b1 bool, d1 double)", Collections.emptyList());

            assertEquals(1, executeUpdate(
                    manager, "INSERT INTO tblspace1.tsql(k1,n1,l1,t1,nu,b1,d1) values(?,?,?,?,?,?,?)",
                    Arrays.asList("mykey",
                            Integer.valueOf(1),
                            Long.valueOf(2),
                            new java.sql.Timestamp(System.currentTimeMillis()),
                            null,
                            Boolean.valueOf(true),
                            Double.valueOf(1.5)))
                    .getUpdateCount());

            // Simple constants
            try (DataScanner scan1 = scan(manager, "SELECT 0.5 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(0.5, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT 1 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(1L, ((Number) scan1.consume().get(0).get(0)).longValue());
            }
            try (DataScanner scan1 = scan(manager, "SELECT 'asd' FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals("asd", scan1.consume().get(0).get(0).toString());
            }
            try (DataScanner scan1 = scan(manager, "SELECT true FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(true, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT false FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(false, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT CURRENT_TIMESTAMP FROM tblspace1.tsql", Collections.emptyList())) {
                long instant = ((java.sql.Timestamp) scan1.consume().get(0).get(0)).getTime();
                assertTrue(Math.abs(System.currentTimeMillis() - instant) < 200);
            }

            // Simple column access
            try (DataScanner scan1 = scan(manager, "SELECT k1 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals("mykey", scan1.consume().get(0).get(0).toString());
            }
            try (DataScanner scan1 = scan(manager, "SELECT n1 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(1, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT l1 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(2L, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT t1 FROM tblspace1.tsql", Collections.emptyList())) {
                final Timestamp currentTs = (java.sql.Timestamp) scan1.consume().get(0).get(0);
                System.out.println("currentTs:" + currentTs);
                long instant = currentTs.getTime();
                long delta = System.currentTimeMillis() - instant;
                System.out.println("delta:" + delta);
                assertTrue("too slow ? " + delta, delta < 60000);
            }
            try (DataScanner scan1 = scan(manager, "SELECT nu FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(null, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT b1 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(true, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT d1 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(1.5, scan1.consume().get(0).get(0));
            }

            // Simple expressions
            try (DataScanner scan1 = scan(manager, "SELECT 4+3+2 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(9L, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT 7-3-2 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(2L, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT 1/2/2 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(0.25, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT 4*3*2 FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(24L, scan1.consume().get(0).get(0));
            }

            // Functions
            try (DataScanner scan1 = scan(manager, "SELECT lower('CiAo') FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals("ciao", scan1.consume().get(0).get(0).toString());
            }
            try (DataScanner scan1 = scan(manager, "SELECT upper('CiAo') FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals("CIAO", scan1.consume().get(0).get(0).toString());
            }

            try (DataScanner scan1 = scan(manager, "SELECT abs(-123) FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(123L, ((Number) scan1.consume().get(0).get(0)).longValue());
            }
            try (DataScanner scan1 = scan(manager, "SELECT abs(123) FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(123L, ((Number) scan1.consume().get(0).get(0)).longValue());
            }
            try (DataScanner scan1 = scan(manager, "SELECT abs(-123.5) FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(123.5, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT abs(123.5) FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(123.5, scan1.consume().get(0).get(0));
            }

            try (DataScanner scan1 = scan(manager, "SELECT round(98765.98765) FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(98766.0, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT round(98765.98765, 2) FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(98765.99, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT round(98765.98765, -2) FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(98800.0, scan1.consume().get(0).get(0));
            }

            // Simple comparisons
            // Warning: jSQLParser doesn't handle this kind of expressions in select clause
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1<2", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 2<1", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1<1", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 2>1", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1>2", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1>1", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1<=2", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 2<=1", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1<=1", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 2>=1", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1>=2", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1>=1", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }

            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 2=1", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1=1", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1<>2", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }

            // Logic expressions
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE (1>2) or (1>0)", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE (1>2) or not (1>0)", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE (1>2) and (1>0)", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE not (1>2) and (1>0)", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }

            // Null exprssion
            // Warning: Parser doesn't handle this kind of expressions in select clause
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE null is null", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE null is not null", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1 is null", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1 is not null", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }

            // Case expressions
            try (DataScanner scan1 = scan(manager, "SELECT CASE "
                    + " WHEN k1='mykey' THEN 1 "
                    + " WHEN k1='mykeys' THEN 2 "
                    + " ELSE 3 "
                    + "END as mycase "
                    + "FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(1L, ((Number) scan1.consume().get(0).get(0)).longValue());
            }
            try (DataScanner scan1 = scan(manager, "SELECT CASE "
                    + " WHEN k1='mykeys' THEN 1 "
                    + " WHEN k1='mykey' THEN 2 "
                    + " ELSE 3 "
                    + "END as mycase "
                    + "FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(2L, ((Number) scan1.consume().get(0).get(0)).longValue());
            }
            try (DataScanner scan1 = scan(manager, "SELECT CASE "
                    + " WHEN k1='mykeys' THEN 1 "
                    + " WHEN k1='mykeyb' THEN 2 "
                    + " ELSE 3 "
                    + "END as mycase "
                    + "FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(3L, ((Number) scan1.consume().get(0).get(0)).longValue());
            }

            // Like expressions
            // Warning: Parser doesn't handle this kind of expressions in select clause
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 'AbBbCc' LIKE '_b____'", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 'AbBbCc' LIKE '_B____'", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 'AbBbCc' LIKE '_b%'", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 'AbBbCc' LIKE '_d%'", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 'AbBbCc' LIKE 'AbBbCc'", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 'AbBbCc' LIKE '%AbBbCc%'", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }


            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 'AbBbCc' LIKE ?", Arrays.asList("%AbBbCc%"))) {
                assertEquals(1, scan1.consume().size());
            }

            // In expressions
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1 in (1,2,3)", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 'b' in ('a','c')", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }

            // Between expressions
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 3 BETWEEN 1 AND 5", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 1 BETWEEN 1 AND 5", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 5 BETWEEN 1 AND 5", Collections.emptyList())) {
                assertEquals(1, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 0 BETWEEN 1 AND 5", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT * FROM tblspace1.tsql WHERE 6 BETWEEN 1 AND 5", Collections.emptyList())) {
                assertEquals(0, scan1.consume().size());
            }
            try (DataScanner scan1 = scan(manager, "SELECT ((4+(3+2)-1)*2) FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(16L, scan1.consume().get(0).get(0));
            }
            try (DataScanner scan1 = scan(manager, "SELECT ((3/2)*3+(1/2)) FROM tblspace1.tsql", Collections.emptyList())) {
                assertEquals(5.0, scan1.consume().get(0).get(0));
            }

        }
    }


    @Test
    public void simpleNullComparisonsTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key, n1 int, l1 long, t1 timestamp, nu string, nu2 string not null, b1 bool, d1 double)", Collections.emptyList());

            assertEquals(1, executeUpdate(
                    manager, "INSERT INTO tblspace1.tsql(k1,n1,l1,t1,nu,nu2, b1,d1) values(?,?,?,?,?,?,?,?)",
                    Arrays.asList("mykey",
                            Integer.valueOf(1),
                            Long.valueOf(2),
                            new java.sql.Timestamp(System.currentTimeMillis()),
                            "mystringnullable",
                            "mystringnotnull",
                            Boolean.valueOf(true),
                            Double.valueOf(1.5)))
                    .getUpdateCount());

            testScan(manager, "SELECT * FROM tblspace1.tsql where k1 is not null", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where k1 is not null and k1 <> ''", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where k1 <> ''", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu is not null", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu is not null and nu <> ''", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu <> ''", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu2 is not null", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu2 is not null and nu2 <> ''", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu2 <> ''", 1);

            execute(manager, "DELETE FROM  tblspace1.tsql", Collections.emptyList());


             assertEquals(1, executeUpdate(
                    manager, "INSERT INTO tblspace1.tsql(k1,n1,l1,t1,nu,nu2, b1,d1) values(?,?,?,?,?,?,?,?)",
                    Arrays.asList("mykey",
                            null,
                            null,
                            null,
                            null,
                            "mystringnotnull",
                            null,
                            null))
                    .getUpdateCount());


            testScan(manager, "SELECT * FROM tblspace1.tsql where k1 is not null", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where k1 is not null and k1 <> ''", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where k1 <> ''", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu is not null", 0);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu <> ''", 0);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu is not null and nu <> ''", 0);

            testScan(manager, "SELECT * FROM tblspace1.tsql where nu2 is not null", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu2 is not null and nu2 <> ''", 1);
            testScan(manager, "SELECT * FROM tblspace1.tsql where nu2 <> ''", 1);
            }
    }

    private void testScan(final DBManager manager, String query, int expectedCount) throws DataScannerException {
        try (DataScanner scan1 = scan(manager, query, Collections.emptyList())) {
            assertEquals(expectedCount, scan1.consume().size());
        }
    }
}
