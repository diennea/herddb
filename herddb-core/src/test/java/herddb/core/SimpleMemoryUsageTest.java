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
import herddb.core.stats.TableManagerStats;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class SimpleMemoryUsageTest {

    @Test
    public void memoryCountersNotEmpty() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
            new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string,t1 timestamp)", Collections.emptyList());

            java.sql.Timestamp tt1 = new java.sql.Timestamp(System.currentTimeMillis());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,?)",
                Arrays.asList("mykey", Integer.valueOf(1234), tt1)).getUpdateCount());

            TableManagerStats stats = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getStats();
            assertTrue(stats.getBuffersUsedMemory() > 0);
            assertTrue(stats.getKeysUsedMemory() > 0);

        }
    }

    @Test
    public void memoryWatcherReleaseMemory() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
            new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null, null);) {
            manager.start();

            int numTablespaces = 10;
            for (int i = 0; i < numTablespaces; i++) {
                manager.executeStatement(new CreateTableSpaceStatement("tblspace" + i,
                    Collections.singleton(nodeId), nodeId, 1, 60000, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                execute(manager, "CREATE TABLE tblspace" + i + ".tsql (k1 string primary key,n1 int,s1 string,t1 timestamp)", Collections.emptyList());
            }

            java.sql.Timestamp tt1 = new java.sql.Timestamp(System.currentTimeMillis());

            for (int i = 0; i < 1_000_000; i++) {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,t1) values(?,?,?)",
                    Arrays.asList("mykey_" + i, Integer.valueOf(1234), tt1)).getUpdateCount());
            }
            manager.checkpoint();
            TableManagerStats stats = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getStats();
            assertEquals(1, stats.getLoadedpages());
            assertEquals(0, stats.getDirtypages());

            MemoryWatcher memoryWatcher = new MemoryWatcher(
                1024, // very very low value, we want to unload everything !
                1, 95);
            memoryWatcher.run(manager);

            assertEquals(0, stats.getLoadedpages());
            assertEquals(0, stats.getDirtypages());
        }
    }
}
