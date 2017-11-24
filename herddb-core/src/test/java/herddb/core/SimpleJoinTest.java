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

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import herddb.utils.MapUtils;

/**
 * Tests on basic JOIN queries
 *
 * @author enrico.olivelli
 */
public class SimpleJoinTest {

    @Test
    public void testSimpleJoinNoWhere() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.table1 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.table2 (k2 string primary key,n2 int,s2 string)", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.table1 (k1,n1,s1) values('a',1,'A')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table1 (k1,n1,s1) values('b',2,'B')", Collections.emptyList());

            execute(manager, "INSERT INTO tblspace1.table2 (k2,n2,s2) values('c',3,'A')", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.table2 (k2,n2,s2) values('d',4,'A')", Collections.emptyList());

//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
//                    + " tblspace1.table1 t1"
//                    + " NATURAL JOIN tblspace1.table2 t2"
//                    + " WHERE t1.n1 > 0"
//                    + "   and t2.n2 >= 1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//                    System.out.println("t:" + t);
//                    assertEquals(6, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("n1", t.getFieldNames()[1]);
//                    assertEquals("s1", t.getFieldNames()[2]);
//                    assertEquals("k2", t.getFieldNames()[3]);
//                    assertEquals("n2", t.getFieldNames()[4]);
//                    assertEquals("s2", t.getFieldNames()[5]);
//                }
//                assertEquals(4, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A",
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B",
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2 FROM"
//                    + " tblspace1.table1 t1 "
//                    + " NATURAL JOIN tblspace1.table2 t2 "
//                    + " WHERE t1.n1 > 0"
//                    + "   and t2.n2 >= 1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(2, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("k2", t.getFieldNames()[1]);
//                }
//                assertEquals(4, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a",
//                    "k2", "c"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a",
//                    "k2", "d"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b",
//                    "k2", "c"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b",
//                    "k2", "d"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2 FROM"
//                    + " tblspace1.table1 t1 "
//                    + " NATURAL JOIN tblspace1.table2 t2 "
//                    + " WHERE t1.n1 >= 2"
//                    + "   and t2.n2 >= 4", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(2, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("k2", t.getFieldNames()[1]);
//                }
//                assertEquals(1, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b",
//                    "k2", "d"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t1.*,t2.* FROM"
//                    + " tblspace1.table1 t1"
//                    + " NATURAL JOIN tblspace1.table2 t2"
//                    + " WHERE t1.n1 > 0"
//                    + "   and t2.n2 >= 1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(6, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("n1", t.getFieldNames()[1]);
//                    assertEquals("s1", t.getFieldNames()[2]);
//                    assertEquals("k2", t.getFieldNames()[3]);
//                    assertEquals("n2", t.getFieldNames()[4]);
//                    assertEquals("s2", t.getFieldNames()[5]);
//                }
//                assertEquals(4, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A",
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B",
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t1.* FROM"
//                    + " tblspace1.table1 t1"
//                    + " NATURAL JOIN tblspace1.table2 t2"
//                    + " WHERE t1.n1 > 0"
//                    + "   and t2.n2 >= 1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(3, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("n1", t.getFieldNames()[1]);
//                    assertEquals("s1", t.getFieldNames()[2]);
//                }
//                assertEquals(4, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t2.* FROM"
//                    + " tblspace1.table1 t1"
//                    + " NATURAL JOIN tblspace1.table2 t2"
//                    + " WHERE t1.n1 > 0"
//                    + "   and t2.n2 >= 1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(3, t.getFieldNames().length);
//                    assertEquals("k2", t.getFieldNames()[0]);
//                    assertEquals("n2", t.getFieldNames()[1]);
//                    assertEquals("s2", t.getFieldNames()[2]);
//                }
//                assertEquals(4, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t2.s2 FROM"
//                    + " tblspace1.table1 t1"
//                    + " NATURAL JOIN tblspace1.table2 t2"
//                    + " WHERE t1.n1 > 0"
//                    + "   and t2.n2 >= 1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(1, t.getFieldNames().length);
//                    assertEquals("s2", t.getFieldNames()[0]);
//                }
//                assertEquals(4, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "s2", "A"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
//                    + " tblspace1.table1 t1"
//                    + " NATURAL JOIN tblspace1.table2 t2"
//                    + " WHERE t1.n1 > 0"
//                    + "   and t2.n2 >= 4", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(6, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("n1", t.getFieldNames()[1]);
//                    assertEquals("s1", t.getFieldNames()[2]);
//                    assertEquals("k2", t.getFieldNames()[3]);
//                    assertEquals("n2", t.getFieldNames()[4]);
//                    assertEquals("s2", t.getFieldNames()[5]);
//                }
//                assertEquals(2, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A",
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B",
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
//                    + " tblspace1.table1 t1"
//                    + " NATURAL JOIN tblspace1.table2 t2"
//                    + " WHERE t1.n1 <= t2.n2", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(6, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("n1", t.getFieldNames()[1]);
//                    assertEquals("s1", t.getFieldNames()[2]);
//                    assertEquals("k2", t.getFieldNames()[3]);
//                    assertEquals("n2", t.getFieldNames()[4]);
//                    assertEquals("s2", t.getFieldNames()[5]);
//                }
//                assertEquals(4, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A",
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B",
//                    "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT * FROM "
//                    + " tblspace1.table1 t1 "
//                    + " NATURAL JOIN tblspace1.table2 t2 "
//                    + " WHERE t1.n1 <= t2.n2 "
//                    + "and t2.n2 <= 3", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(6, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("n1", t.getFieldNames()[1]);
//                    assertEquals("s1", t.getFieldNames()[2]);
//                    assertEquals("k2", t.getFieldNames()[3]);
//                    assertEquals("n2", t.getFieldNames()[4]);
//                    assertEquals("s2", t.getFieldNames()[5]);
//                }
//                assertEquals(2, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT * FROM "
//                    + " tblspace1.table1 t1 "
//                    + " JOIN tblspace1.table2 t2 ON t1.n1 <= t2.n2 "
//                    + " and t2.n2 <= 3", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(6, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("n1", t.getFieldNames()[1]);
//                    assertEquals("s1", t.getFieldNames()[2]);
//                    assertEquals("k2", t.getFieldNames()[3]);
//                    assertEquals("n2", t.getFieldNames()[4]);
//                    assertEquals("s2", t.getFieldNames()[5]);
//                }
//                assertEquals(2, tuples.size());
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "a", "n1", 1, "s1", "A",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                    tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                    "k1", "b", "n1", 2, "s1", "B",
//                    "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//            }
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2 FROM"
//                        + " tblspace1.table1 t1 "
//                        + " NATURAL JOIN tblspace1.table2 t2 "
//                        + " WHERE t1.n1 + 3 = t2.n2", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//                    System.out.println("tuple -: " + t.toMap());
//                    assertEquals(2, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("k2", t.getFieldNames()[1]);
//                }
//                assertEquals(1, tuples.size());
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "k1", "a",
//                        "k2", "d"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t2.n2, t1.s1, t2.k2 FROM"
//                        + " tblspace1.table1 t1"
//                        + " NATURAL JOIN tblspace1.table2 t2"
//                        + " WHERE t1.n1 > 0"
//                        + "   and t2.n2 >= 1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(3, t.getFieldNames().length);
//                    assertEquals("n2", t.getFieldNames()[0]);
//                    assertEquals("s1", t.getFieldNames()[1]);
//                    assertEquals("k2", t.getFieldNames()[2]);
//                }
//                assertEquals(4, tuples.size());
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "s1", "A",
//                        "k2", "c", "n2", 3
//                ))));
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "s1", "A",
//                        "k2", "d", "n2", 4
//                ))));
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "s1", "B",
//                        "k2", "c", "n2", 3
//                ))));
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "s1", "B",
//                        "k2", "d", "n2", 4
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t2.*, t1.* FROM"
//                        + " tblspace1.table1 t1"
//                        + " NATURAL JOIN tblspace1.table2 t2"
//                        + " WHERE t1.n1 > 0"
//                        + "   and t2.n2 >= 1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(6, t.getFieldNames().length);
//                    assertEquals("k2", t.getFieldNames()[0]);
//                    assertEquals("n2", t.getFieldNames()[1]);
//                    assertEquals("s2", t.getFieldNames()[2]);
//                    assertEquals("k1", t.getFieldNames()[3]);
//                    assertEquals("n1", t.getFieldNames()[4]);
//                    assertEquals("s1", t.getFieldNames()[5]);
//                }
//                assertEquals(4, tuples.size());
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "k1", "a", "n1", 1, "s1", "A",
//                        "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "k1", "a", "n1", 1, "s1", "A",
//                        "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "k1", "b", "n1", 2, "s1", "B",
//                        "k2", "c", "n2", 3, "s2", "A"
//                ))));
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "k1", "b", "n1", 2, "s1", "B",
//                        "k2", "d", "n2", 4, "s2", "A"
//                ))));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
//                        + " tblspace1.table1 t1"
//                        + " NATURAL JOIN tblspace1.table2 t2 "
//                        + " ORDER BY n2,n1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(6, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("n1", t.getFieldNames()[1]);
//                    assertEquals("s1", t.getFieldNames()[2]);
//                    assertEquals("k2", t.getFieldNames()[3]);
//                    assertEquals("n2", t.getFieldNames()[4]);
//                    assertEquals("s2", t.getFieldNames()[5]);
//                }
//                assertEquals(4, tuples.size());
//
//                int i = 0;
//                assertTrue(
//                        tuples.get(i++).toMap().equals(MapUtils.map(
//                                "k1", "a", "n1", 1, "s1", "A",
//                                "k2", "c", "n2", 3, "s2", "A"
//                        )));
//
//                assertTrue(
//                        tuples.get(i++).toMap().equals(MapUtils.map(
//                                "k1", "b", "n1", 2, "s1", "B",
//                                "k2", "c", "n2", 3, "s2", "A"
//                        )));
//
//                assertTrue(
//                        tuples.get(i++).toMap().equals(MapUtils.map(
//                                "k1", "a", "n1", 1, "s1", "A",
//                                "k2", "d", "n2", 4, "s2", "A"
//                        )));
//
//                assertTrue(
//                        tuples.get(i++).toMap().equals(MapUtils.map(
//                                "k1", "b", "n1", 2, "s1", "B",
//                                "k2", "d", "n2", 4, "s2", "A"
//                        )));
//
//            }
//
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT * FROM"
//                        + " tblspace1.table1 t1"
//                        + " NATURAL JOIN tblspace1.table2 t2 "
//                        + " ORDER BY n2 desc,n1", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//
//                    assertEquals(6, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("n1", t.getFieldNames()[1]);
//                    assertEquals("s1", t.getFieldNames()[2]);
//                    assertEquals("k2", t.getFieldNames()[3]);
//                    assertEquals("n2", t.getFieldNames()[4]);
//                    assertEquals("s2", t.getFieldNames()[5]);
//                }
//                assertEquals(4, tuples.size());
//
//                int i = 0;
//
//                assertTrue(
//                        tuples.get(i++).toMap().equals(MapUtils.map(
//                                "k1", "a", "n1", 1, "s1", "A",
//                                "k2", "d", "n2", 4, "s2", "A"
//                        )));
//
//                assertTrue(
//                        tuples.get(i++).toMap().equals(MapUtils.map(
//                                "k1", "b", "n1", 2, "s1", "B",
//                                "k2", "d", "n2", 4, "s2", "A"
//                        )));
//
//                assertTrue(
//                        tuples.get(i++).toMap().equals(MapUtils.map(
//                                "k1", "a", "n1", 1, "s1", "A",
//                                "k2", "c", "n2", 3, "s2", "A"
//                        )));
//
//                assertTrue(
//                        tuples.get(i++).toMap().equals(MapUtils.map(
//                                "k1", "b", "n1", 2, "s1", "B",
//                                "k2", "c", "n2", 3, "s2", "A"
//                        )));
//
//            }
//            {
//                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2 FROM"
//                        + " tblspace1.table1 t1 "
//                        + " NATURAL JOIN tblspace1.table2 t2 "
//                        + " WHERE t1.n1 + 3 <= t2.n2", Collections.emptyList()).consume();
//                for (DataAccessor t : tuples) {
//                    System.out.println("tuple -: " + t.toMap());
//                    assertEquals(2, t.getFieldNames().length);
//                    assertEquals("k1", t.getFieldNames()[0]);
//                    assertEquals("k2", t.getFieldNames()[1]);
//                }
//                assertEquals(1, tuples.size());
//
//                assertTrue(
//                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
//                        "k1", "a",
//                        "k2", "d"
//                ))));
//
//            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2, t1.s1, t2.s2 FROM"
                        + " tblspace1.table1 t1 "
                        + " LEFT JOIN tblspace1.table2 t2 "
                        + " ON t1.s1 = t2.s2"
                        + " ", Collections.emptyList()).consume();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(4, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("k2", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("s2", t.getFieldNames()[3]);
                }
                assertEquals(3, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "k2", "c", "s1", "A", "s2", "A"
                ))));
                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "k2", "d", "s1", "A", "s2", "A"
                ))));
                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "b", "k2", null, "s1", "B", "s2", null
                ))));

            }
            {
                List<DataAccessor> tuples = scan(manager, "SELECT t1.k1, t2.k2, t1.s1, t2.s2 FROM"
                        + " tblspace1.table1 t1 "
                        + " RIGHT JOIN tblspace1.table2 t2 "
                        + " ON t1.s1 = t2.s2"
                        + " ", Collections.emptyList()).consume();
                for (DataAccessor t : tuples) {
                    System.out.println("tuple -: " + t.toMap());
                    assertEquals(4, t.getFieldNames().length);
                    assertEquals("k1", t.getFieldNames()[0]);
                    assertEquals("k2", t.getFieldNames()[1]);
                    assertEquals("s1", t.getFieldNames()[2]);
                    assertEquals("s2", t.getFieldNames()[3]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "k2", "c", "s1", "A", "s2", "A"
                ))));
                assertTrue(
                        tuples.stream().anyMatch(t -> t.toMap().equals(MapUtils.map(
                        "k1", "a", "k2", "d", "s1", "A", "s2", "A"
                ))));

            }
        }
    }

}
