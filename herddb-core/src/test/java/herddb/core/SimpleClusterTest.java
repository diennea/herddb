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
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.file.FileDataStorageManager;
import herddb.log.CommitLogManager;
import herddb.metadata.MetadataStorageManager;
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
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import herddb.utils.Holder;
import herddb.utils.ZKTestEnv;
import java.util.List;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author enrico.olivelli
 */
public class SimpleClusterTest extends BaseTestcase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private ZKTestEnv testEnv;

    @Override
    protected void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookie();
    }

    @Override
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Override
    protected MetadataStorageManager makeMetadataStorageManager() throws Exception {
        return new ZookeeperMetadataStorageManager(testEnv.getAddress(), testEnv.getTimeout(), testEnv.getPath());
    }

    @Override
    protected CommitLogManager makeCommitLogManager() throws Exception {
        return new BookkeeperCommitLogManager((ZookeeperMetadataStorageManager) metadataStorageManager, new ServerConfiguration(), new NullStatsLogger());
    }

    @Override
    protected DataStorageManager makeDataStorageManager() throws Exception {
        return new FileDataStorageManager(folder.newFolder().toPath());
    }

    @Test
    public void test() throws Exception {

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("0"));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }
        assertEquals(0, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableName));
        manager.checkpoint();
        String tableUuid = manager.getTableSpaceManager(tableSpace).getTableManager(tableName).getTable().uuid;
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

        // a new page must be allocated
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
        // only a new page must be allocated, not two more
        manager.checkpoint();
        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));

        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, Bytes.from_string("key1"), null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, Bytes.from_string("key1"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result.found());
        }

        // a delete does not trigger new pages in this case
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

        Holder<TableStatus> _tableStatus = new Holder<>();
        dataStorageManager.fullTableScan(tableSpaceUUID, tableUuid, new FullTableScanConsumer() {
            @Override
            public void acceptTableStatus(TableStatus tableStatus) {
                _tableStatus.value = tableStatus;
            }

            @Override
            public void startPage(long pageId) {

            }

            @Override
            public void acceptRecord(Record record) {

            }

            @Override
            public void endPage() {
            }

            @Override
            public void endTable() {
            }

        });
        for (long pageId : _tableStatus.value.activePages.keySet()) {
            List<Record> records = dataStorageManager.readPage(tableSpaceUUID, tableUuid, pageId);
            System.out.println("PAGE #" + pageId + " records :" + records);
        }

        assertEquals(1, _tableStatus.value.activePages.size());

    }
}
