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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.file.FileCommitLog;
import herddb.file.FileDataStorageManager;
import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManager;
import herddb.utils.Bytes;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 *
 * @author enrico.olivelli
 */
public class FlushFileTest extends BaseTestcase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private static ExecutorService threadPool;

    @BeforeClass
    public static void startThreadPool() {
        threadPool = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void stopThreadPool() throws Exception {
        if (threadPool != null) {
            threadPool.shutdown();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Override
    protected CommitLogManager makeCommitLogManager() {
        return new CommitLogManager() {
            @Override
            public CommitLog createCommitLog(String tableSpace, String name, String nodeId) {
                try {
                    return new FileCommitLog(folder.newFolder(tableSpace).toPath(),
                            name, 1024 * 1024, threadPool, new NullStatsLogger(),
                            l -> {
                            }, ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT,
                            ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT,
                            ServerConfiguration.PROPERTY_MAX_SYNC_TIME_DEFAULT, false /* require fsync */
                    );
                } catch (IOException err) {
                    throw new RuntimeException(err);
                }
            }
        };
    }

    @Override
    protected DataStorageManager makeDataStorageManager() {
        try {
            return new FileDataStorageManager(folder.newFolder("data").toPath());
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    @Test
    public void test() throws Exception {

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("0"));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        String tableUuid = manager.getTableSpaceManager(tableSpace).getTableManager(tableName).getTable().uuid;
        assertEquals(0, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));
        manager.checkpoint();

        assertNotNull(dataStorageManager.readPage(tableSpaceUUID, tableUuid, 1L));
        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));

        {
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, Bytes.from_string("key1"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

        manager.checkpoint();
        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("5"));
            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        // a new page must be allocated, but the first will be dropped, as it does not contain any useful record
        manager.checkpoint();
        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("6"));
            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }
        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("7"));
            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }
        // only a new page must be allocated, not two more, but the prev page will be dropped, as it does not contain any useful record
        manager.checkpoint();
        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));

        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, Bytes.from_string("key1"), null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, Bytes.from_string("key1"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result.found());
        }

        // a delete does not trigger new pages in this case, no more record will lead to no more active page
        manager.checkpoint();
        assertEquals(0, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));

        {
            assertEquals(1, manager.executeUpdate(new InsertStatement(tableSpace, tableName, new Record(Bytes.from_string("key2"), Bytes.from_string("50"))), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
            assertEquals(1, manager.executeUpdate(new InsertStatement(tableSpace, tableName, new Record(Bytes.from_string("key3"), Bytes.from_string("60"))), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        manager.checkpoint();
        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));
        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, Bytes.from_string("key2"), null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }
        // a new page, containg the key3 record is needed
        manager.checkpoint();

        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));

    }
}
