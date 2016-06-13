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
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 *
 *
 * @author enrico.olivelli
 */
public class SystemTablesTest {

    @Test
    public void testSysTables() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key auto_increment,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.tsql2 (k1 string primary key,n1 long,s1 timestamp, b1 blob)", Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.systables", Collections.emptyList());) {
                List<Tuple> records = scan.consume();
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql")).findAny().isPresent());
                assertTrue(records.stream().filter(t -> t.get("table_name").equals("tsql2")).findAny().isPresent());
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
