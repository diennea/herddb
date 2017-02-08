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
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.RawString;
import static org.junit.Assert.assertFalse;

/**
 *
 *
 * @author enrico.olivelli
 */
public class SystemTablesTest {

    @Test
    public void testSysTables() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key auto_increment,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.tsql2 (k1 string primary key,n1 long,s1 timestamp, b1 blob)", Collections.emptyList());
            execute(manager, "CREATE BRIN INDEX index1 on tblspace1.tsql2 (s1,b1)", Collections.emptyList());
            execute(manager, "CREATE HASH INDEX index2 on tblspace1.tsql2 (b1)", Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables", Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql")).findAny().isPresent());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql2")).findAny().isPresent());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("systables")).findAny().isPresent());
            }
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where systemtable=false", Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertEquals(2, records.size());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql")).findAny().isPresent());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql2")).findAny().isPresent());
            }
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where systemtable='false'", Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertEquals(2, records.size());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql")).findAny().isPresent());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql2")).findAny().isPresent());
            }
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where systemtable=true", Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertFalse(records.stream().filter(t -> t.get("table_name").equals("tsql")).findAny().isPresent());
                assertFalse(records.stream().filter(t -> t.get("table_name").equals("tsql2")).findAny().isPresent());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("systables")).findAny().isPresent());
            }
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables where systemtable='true'", Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertFalse(records.stream().filter(t -> t.get("table_name").equals("tsql")).findAny().isPresent());
                assertFalse(records.stream().filter(t -> t.get("table_name").equals("tsql2")).findAny().isPresent());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("systables")).findAny().isPresent());
            }

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systablestats", Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql")).findAny().isPresent());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql2")).findAny().isPresent());
            }

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.sysindexes order by index_name",
                Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertEquals(2, records.size());
                Tuple index1 = records.get(0);
                assertEquals(RawString.of("tblspace1"), index1.get("tablespace"));
                assertEquals(RawString.of("brin"), index1.get("index_type"));
                assertEquals(RawString.of("index1"), index1.get("index_name"));
                assertEquals(RawString.of("tsql2"), index1.get("table_name"));

                Tuple index2 = records.get(1);
                assertEquals(RawString.of("tblspace1"), index2.get("tablespace"));
                assertEquals(RawString.of("hash"), index2.get("index_type"));
                assertEquals(RawString.of("index2"), index2.get("index_name"));
                assertEquals(RawString.of("tsql2"), index2.get("table_name"));
            }

            execute(manager, "BEGIN TRANSACTION 'tblspace1'", Collections.emptyList());
            long txid;
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systransactions order by txid",
                Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertEquals(1, records.size());
                System.out.println("records:" + records);
                txid = (Long) records.get(0).get("txid");
            }
            execute(manager, "COMMIT TRANSACTION 'tblspace1'," + txid, Collections.emptyList());
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systransactions order by txid",
                Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertEquals(0, records.size());
            }

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.syscolumns", Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                records.forEach(r -> {
                    System.out.println("found " + r.toMap());
                });
                assertTrue(records.stream()
                    .filter(t
                        -> t.get("table_name").equals("tsql")
                    && t.get("column_name").equals("k1")
                    && t.get("data_type").equals("string")
                    && t.get("auto_increment").equals(1)
                    ).findAny().isPresent());
                assertTrue(records.stream()
                    .filter(t
                        -> t.get("table_name").equals("tsql")
                    && t.get("column_name").equals("n1")
                    && t.get("data_type").equals("integer")
                    && t.get("auto_increment").equals(0)
                    ).findAny().isPresent());
                assertTrue(records.stream()
                    .filter(t
                        -> t.get("table_name").equals("tsql2")
                    && t.get("column_name").equals("s1")
                    && t.get("data_type").equals("timestamp")
                    ).findAny().isPresent());
                assertTrue(records.stream()
                    .filter(t
                        -> t.get("table_name").equals("tsql2")
                    && t.get("column_name").equals("b1")
                    && t.get("data_type").equals("bytearray")
                    ).findAny().isPresent());
                assertTrue(records.stream()
                    .filter(t
                        -> t.get("table_name").equals("tsql2")
                    && t.get("column_name").equals("n1")
                    && t.get("data_type").equals("long")
                    ).findAny().isPresent());
            }

        }
    }

}
