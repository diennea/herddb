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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import herddb.codec.RecordSerializer;
import herddb.core.stats.TableManagerStats;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.FullTableScanPredicate;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;

/**
 * Test on memory usage over a bigtable
 *
 * @author enrico.olivelli
 */
public class BigTableScanTest {

    @Test
    public void bigTableScan() throws Exception {
        int testSize = 5000;
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(),null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            for (int i = 0; i < testSize; i++) {
                InsertStatement insert = new InsertStatement(table.tablespace, table.name, RecordSerializer.makeRecord(table, "id", "k" + i, "name", "testname" + i));
                assertEquals(1, manager.executeUpdate(insert, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
            }

            TableManagerStats stats = manager.getTableSpaceManager(table.tablespace).getTableManager(table.name).getStats();
            assertEquals(testSize, stats.getTablesize());
            assertEquals(0, stats.getLoadedpages());

            manager.checkpoint();

            try (DataScanner scan = manager.scan(new ScanStatement(table.tablespace, table, new FullTableScanPredicate()), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);) {
                AtomicInteger count = new AtomicInteger();
                scan.forEach(tuple -> {
                    count.incrementAndGet();
                });
                assertEquals(testSize, count.get());
            }
            assertEquals(testSize, stats.getTablesize());
            assertTrue(stats.getMaxloadedpages() >= stats.getLoadedpages());
        }
    }
}
