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
import static herddb.model.TransactionContext.NO_TRANSACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.InsertStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import java.util.Collections;
import org.junit.Test;

/**
 * Tests on <i>new</i> pages behaviours.
 *
 * @author diego.salvi
 */
public class NewPageTest {

    /**
     * Attempt to unload an empty new page from memory
     */
    @Test
    public void unloadEmptyNewPage() throws Exception {
        String nodeId = "localhost";

        ServerConfiguration config1 = new ServerConfiguration();

        /* Smaller pages to avoid too many record creations */
        config1.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 10 * 1024);

        try (DBManager manager = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(),
                null, null, config1,
                null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string, S1 string, primary key(k1))",
                    Collections.emptyList());

            manager.checkpoint();

            final TableManager tableManager = (TableManager) manager.getTableSpaceManager("tblspace1")
                    .getTableManager("tsql");

            final Table table = tableManager.getTable();

            int records = 0;

            /*
             * Fill table until there are some loaded page (not only kept in memory and unknown to page
             * replacement policy).
             */
            while (tableManager.getLoadedPages().size() < 2) {
                Bytes key = Bytes.from_string("ins_key_" + records++);
                assertEquals(1,
                        manager.executeUpdate(new InsertStatement(table.tablespace, table.name, new Record(key, key)),
                                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                                TransactionContext.NO_TRANSACTION).getUpdateCount());
            }

            /* Checks that every page is still writable and filled */
            for (DataPage page : tableManager.getLoadedPages()) {
                assertTrue(page.writable);
                assertFalse(page.immutable);

                assertFalse(page.isEmpty());
                assertNotEquals(0, page.getUsedMemory());
            }

            /* Fully remove all the data, existing (still mutable) pages should be empty now */
            for (int i = 0; i < records; ++i) {
                Bytes key = Bytes.from_string("ins_key_" + i);
                assertEquals(1,
                        manager.executeUpdate(new DeleteStatement(table.tablespace, table.name, key, null),
                                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                                TransactionContext.NO_TRANSACTION).getUpdateCount());
            }

            /* Checks that every page is still writable and empty */
            for (DataPage page : tableManager.getLoadedPages()) {
                assertTrue(page.writable);
                assertFalse(page.immutable);

                assertTrue(page.isEmpty());
                assertEquals(0, page.getUsedMemory());
            }

            /* Expect that every empty new page remains */
            assertEquals(2, tableManager.getLoadedPages().size());

            /* Attempt to unload new pages known to page replacement policy */
            boolean unloadedAny = false;
            for (DataPage page : tableManager.getLoadedPages()) {
                if (manager.getMemoryManager().getDataPageReplacementPolicy().remove(page)) {
                    unloadedAny = true;
                    tableManager.unload(page.pageId);
                }
            }

            /* Ensure that some page was unloaded */
            assertTrue(unloadedAny);
        }
    }


    /**
     * Attempt to executre a checkpoint with empty new pages
     */
    @Test
    public void checkpointEmptyNewPage() throws Exception {
        String nodeId = "localhost";

        ServerConfiguration config1 = new ServerConfiguration();

        /* Smaller pages to avoid too many record creations */
        config1.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 10 * 1024);

        try (DBManager manager = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(),
                null, null, config1,
                null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string, S1 string, primary key(k1))",
                    Collections.emptyList());

            manager.checkpoint();

            final TableManager tableManager = (TableManager) manager.getTableSpaceManager("tblspace1")
                    .getTableManager("tsql");

            final Table table = tableManager.getTable();

            int records = 0;

            /*
             * Fill table until there are some loaded page (not only kept in memory and unknown to page
             * replacement policy).
             */
            while (tableManager.getLoadedPages().size() < 2) {
                Bytes key = Bytes.from_string("ins_key_" + records++);
                assertEquals(1,
                        manager.executeUpdate(new InsertStatement(table.tablespace, table.name, new Record(key, key)),
                                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                                TransactionContext.NO_TRANSACTION).getUpdateCount());
            }

            /* Checks that every page is still writable and filled */
            for (DataPage page : tableManager.getLoadedPages()) {
                assertTrue(page.writable);
                assertFalse(page.immutable);

                assertFalse(page.isEmpty());
                assertNotEquals(0, page.getUsedMemory());
            }

            /* Fully remove all the data, existing (still mutable) pages should be empty now */
            for (int i = 0; i < records; ++i) {
                Bytes key = Bytes.from_string("ins_key_" + i);
                assertEquals(1,
                        manager.executeUpdate(new DeleteStatement(table.tablespace, table.name, key, null),
                                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                                TransactionContext.NO_TRANSACTION).getUpdateCount());
            }

            /* Checks that every page is still writable and empty */
            for (DataPage page : tableManager.getLoadedPages()) {
                assertTrue(page.writable);
                assertFalse(page.immutable);

                assertTrue(page.isEmpty());
                assertEquals(0, page.getUsedMemory());
            }

            manager.checkpoint();

            /* Expect just one (empty) new page remaining */
            assertEquals(1, tableManager.getLoadedPages().size());
        }
    }
}
