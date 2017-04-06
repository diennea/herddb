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
 * Tests on memory usage
 *
 * @author enrico.olivelli
 */
public class MemoryCountersTest {

//    @Before
//    public void setupLogger() throws Exception {
//        Level level = Level.FINEST;
//        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//
//            @Override
//            public void uncaughtException(Thread t, Throwable e) {
//                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
//                e.printStackTrace();
//            }
//        });
//        java.util.logging.LogManager.getLogManager().reset();
//        ConsoleHandler ch = new ConsoleHandler();
//        ch.setLevel(level);
//        SimpleFormatter f = new SimpleFormatter();
//        ch.setFormatter(f);
//        java.util.logging.Logger.getLogger("").setLevel(level);
//        java.util.logging.Logger.getLogger("").addHandler(ch);
//    }

    @Test
    public void memoryCountersNotEmpty() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
            new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
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

}
