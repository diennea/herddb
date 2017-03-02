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
import java.util.Arrays;

/**
 * Tests on basic JOIN queries
 *
 * @author enrico.olivelli
 */
public class SystemTablesJoinTest {

    @Test
    public void testSimpleJoinNoWhere() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            {
                List<DataAccessor> tuples = scan(manager, "SELECT systablespaces.leader,"
                    + "systablespaces.tablespace_name,"
                    + "sysnodes.address FROM"
                    + " systablespaces "
                    + " JOIN sysnodes ON systablespaces.leader = sysnodes.nodeid"
                    + " ORDER BY tablespace_name", Collections.emptyList()).consume();
                for (DataAccessor t : tuples) {
                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("leader", t.getFieldNames()[0]);
                    assertEquals("tablespace_name", t.getFieldNames()[1]);
                    assertEquals("address", t.getFieldNames()[2]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                    tuples.get(0).toMap().equals(MapUtils.map(
                        "leader", "localhost", "address", "localhost:7000", "tablespace_name", "default"
                    )));
                assertTrue(
                    tuples.get(1).toMap().equals(MapUtils.map(
                        "leader", "localhost", "address", "localhost:7000", "tablespace_name", "tblspace1"
                    )));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT systablespaces.leader,"
                    + "systablespaces.tablespace_name,"
                    + "sysnodes.address FROM"
                    + " systablespaces "
                    + " JOIN sysnodes ON systablespaces.leader = sysnodes.nodeid"
                    + " ORDER BY tablespace_name DESC", Collections.emptyList()).consume();
                for (DataAccessor t : tuples) {
                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("leader", t.getFieldNames()[0]);
                    assertEquals("tablespace_name", t.getFieldNames()[1]);
                    assertEquals("address", t.getFieldNames()[2]);
                }
                assertEquals(2, tuples.size());

                assertTrue(
                    tuples.get(0).toMap().equals(MapUtils.map(
                        "leader", "localhost", "address", "localhost:7000", "tablespace_name", "tblspace1"
                    )));
                assertTrue(
                    tuples.get(1).toMap().equals(MapUtils.map(
                        "leader", "localhost", "address", "localhost:7000", "tablespace_name", "default"
                    )));

            }

            {
                List<DataAccessor> tuples = scan(manager, "SELECT systablespaces.leader,"
                    + "systablespaces.tablespace_name,"
                    + "sysnodes.address FROM"
                    + " systablespaces "
                    + " JOIN sysnodes ON systablespaces.leader = sysnodes.nodeid AND systablespaces.tablespace_name <> ?"
                    + " ORDER BY tablespace_name DESC", Arrays.asList("default")).consume();
                for (DataAccessor t : tuples) {
                    assertEquals(3, t.getFieldNames().length);
                    assertEquals("leader", t.getFieldNames()[0]);
                    assertEquals("tablespace_name", t.getFieldNames()[1]);
                    assertEquals("address", t.getFieldNames()[2]);
                }
                assertEquals(1, tuples.size());

                assertTrue(
                    tuples.get(0).toMap().equals(MapUtils.map(
                        "leader", "localhost", "address", "localhost:7000", "tablespace_name", "tblspace1"
                    )));

            }

        }
    }

}
