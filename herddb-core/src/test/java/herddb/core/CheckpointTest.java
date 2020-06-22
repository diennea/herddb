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
import static herddb.core.TestUtils.scan;
import static herddb.model.TransactionContext.NO_TRANSACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.stats.TableManagerStats;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import herddb.utils.RandomString;
import herddb.utils.RawString;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests on checkpoints
 *
 * @author diego.salvi
 */
public class CheckpointTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Keep dirty pages even after checkpoint
     */
    @Test
    public void keepDirtyTest() throws Exception {
        String nodeId = "localhost";

        ServerConfiguration config1 = new ServerConfiguration();

        /* Disable page compaction (avoid compaction of dirty page) */
        config1.set(ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD, 0.0D);

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

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1))",
                    Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey2", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey3", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey4", "a", Integer.valueOf(1234))).getUpdateCount());

            manager.checkpoint();

            /* Dirty a page with few data */
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set s1=? where k1=?",
                    Arrays.asList("b", "mykey4")).getUpdateCount());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey5", "a", Integer.valueOf(1234))).getUpdateCount());

            manager.checkpoint();

            /* The page is still dirty */
            assertEquals(1, manager.getTableSpaceManager("tblspace1")
                    .getTableManager("tsql").getStats().getDirtypages());

        }
    }

    /**
     * Reload dirty pages on restart
     */
    @Test
    public void restartTest() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();

        String nodeId = "localhost";

        ServerConfiguration config1 = new ServerConfiguration();

        /* Disable page compaction (avoid compaction of dirty page) */
        config1.set(ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD, 0.0D);

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config1, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1))",
                    Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey2", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey3", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey4", "a", Integer.valueOf(1234))).getUpdateCount());

            manager.checkpoint();

            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set s1=? where k1=?",
                    Arrays.asList("b", "mykey4")).getUpdateCount());

            manager.checkpoint();

            assertEquals(3, manager.getTableSpaceManager("tblspace1")
                    .getTableManager("tsql").getStats().getLoadedpages());

            assertEquals(1, manager.getTableSpaceManager("tblspace1")
                    .getTableManager("tsql").getStats().getDirtypages());

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config1, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            assertEquals(1, manager.getTableSpaceManager("tblspace1")
                    .getTableManager("tsql").getStats().getLoadedpages());

            assertEquals(1, manager.getTableSpaceManager("tblspace1")
                    .getTableManager("tsql").getStats().getDirtypages());

        }

    }

    /**
     * Rebuild all small pages
     */
    @Test
    public void rebuildSmallPages() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();
        String nodeId = "localhost";

        ServerConfiguration config1 = new ServerConfiguration();
        config1.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 1024L);

        int records = 1000;
        int keylen = 25;
        int strlen = 50;

        int originalPages;
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config1, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string, s1 string, n1 int, primary key(k1))",
                    Collections.emptyList());

            for (int i = 0; i < records; ++i) {
                executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                        Arrays.asList(
                                RandomString.getInstance().nextString(keylen),
                                RandomString.getInstance().nextString(strlen),
                                Integer.valueOf(i)));
            }

            manager.checkpoint();

            String uuid = manager.getMetadataStorageManager().describeTableSpace("tblspace1").uuid;
            String tableUuid = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().uuid;
            originalPages = manager.getDataStorageManager().getActualNumberOfPages(uuid, tableUuid);
            assertTrue(originalPages > 10);

        }

        ServerConfiguration config2 = new ServerConfiguration();

        config2.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 2048L);
        config2.set(ServerConfiguration.PROPERTY_COMPACTION_DURATION, -1L);

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config2, null)) {
            manager.start();

            assertTrue(manager.waitForTablespace("tblspace1", 20000));

            manager.checkpoint();

            String tableUuid = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getTable().uuid;
            String uuid = manager.getMetadataStorageManager().describeTableSpace("tblspace1").uuid;
            int pages = manager.getDataStorageManager().getActualNumberOfPages(uuid, tableUuid);

            /* There are at least half pages! */
            assertTrue(pages <= (originalPages / 2) + (originalPages % 2));
        }

    }

    /**
     * Rebuild dirty pages but don't keep rebuilt pages in memory after checkpoint
     */
    @Test
    public void doNotKeepDirtyRebuiltInMemory() throws Exception {
        String nodeId = "localhost";

        ServerConfiguration config = new ServerConfiguration();

        /* Disable page compaction (avoid compaction of dirty page) */
        config.set(ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD, 0.0D);

        /* Force dirty rebuilt */
        config.set(ServerConfiguration.PROPERTY_DIRTY_PAGE_THRESHOLD, 0.0D);

        final long pageSize = 350;
        config.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, pageSize);


        try (DBManager manager = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(),
                null, null, config,
                null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1))",
                    Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey2", "a", Integer.valueOf(1234))).getUpdateCount());

            manager.checkpoint();

            TableManager table = (TableManager) manager.getTableSpaceManager("tblspace1").getTableManager("tsql");

            /* Get only current and loaded page (+1 empty page for new records... we need the other one) */
            Collection<DataPage> pages = table.getLoadedPages();
            assertEquals(2, pages.size());
            DataPage page = pages.stream().filter(dpage -> !dpage.writable).findFirst().get();
            assertNotNull(page);

            /* 2 records in page */
            assertEquals(2, page.size());

            /* No more space for other records */
            long avgRecordSize = page.getUsedMemory() / page.size();
            assertTrue(page.getUsedMemory() + avgRecordSize > pageSize);

            /* Dirty a page with few data */
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set s1=? where k1=?",
                    Arrays.asList("b", "mykey")).getUpdateCount());

            /*
             * Add new record to fill working page (2 record per page, new "updated" and current insert. We
             * expect remaining dirty record on a new page unloaded)
             */
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey3", "a", Integer.valueOf(1234))).getUpdateCount());

            /* Force page unload after dirty it */

            /* Remove from page replacement policy */
            manager.getMemoryManager().getDataPageReplacementPolicy().remove(page);

            /* Fully unload the page */
            table.unload(page.pageId);

            /* Now do a checkpoint with an unloaded dirty page */
            manager.checkpoint();

            /* New rebuilt page souldn't be in memory */
            pages = table.getLoadedPages();

            /* 1 page sould be empty */
            assertEquals(1, pages.stream().filter(DataPage::isEmpty).count());

            /* 2 pages, one empty and one contains only moved record and the new record */
            assertEquals(2, pages.size());

        }
    }

    /**
     * Rebuild dirty pages but keep rebuilt pages in memory after checkpoint
     */
    @Test
    public void keepDirtyRebuiltInMemory() throws Exception {
        String nodeId = "localhost";

        ServerConfiguration config = new ServerConfiguration();

        /* Disable page compaction (avoid compaction of dirty page) */
        config.set(ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD, 0.0D);

        /* Force dirty rebuilt */
        config.set(ServerConfiguration.PROPERTY_DIRTY_PAGE_THRESHOLD, 0.0D);

        final long pageSize = 350;
        config.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, pageSize);


        try (DBManager manager = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(),
                null, null, config,
                null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1))",
                    Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey2", "a", Integer.valueOf(1234))).getUpdateCount());

            manager.checkpoint();

            TableManager table = (TableManager) manager.getTableSpaceManager("tblspace1").getTableManager("tsql");

            /* Get only current and loaded page (+1 empty page for new records... we need the other one) */
            Collection<DataPage> pages = table.getLoadedPages();
            assertEquals(2, pages.size());
            DataPage page = pages.stream().filter(dpage -> !dpage.writable).findFirst().get();
            assertNotNull(page);

            /* 2 records in page */
            assertEquals(2, page.size());

            /* No more space for other records */
            long avgRecordSize = page.getUsedMemory() / page.size();
            assertTrue(page.getUsedMemory() + avgRecordSize > pageSize);

            /* Dirty a page with few data */
            assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set s1=? where k1=?",
                    Arrays.asList("b", "mykey")).getUpdateCount());

            /*
             * Add new record to fill working page (2 record per page, new "updated" and current insert. We
             * expect remaining dirty record on a new page unloaded)
             */
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey3", "a", Integer.valueOf(1234))).getUpdateCount());

            /* Do not unload dirty page */

            /* Now do a checkpoint with an unloaded dirty page */
            manager.checkpoint();

            /* New rebuilt page souldn't be in memory */
            pages = table.getLoadedPages();

            /* 1 page sould be empty */
            assertEquals(1, pages.stream().filter(DataPage::isEmpty).count());

            /*
             * 3 pages, one empty, one contains only moved record and the new record, and one containing only
             * one record remaining from dirty page unpack
             */
            assertEquals(3, pages.size());

        }
    }

    /**
     * Rebuild small pages but don't keep rebuilt pages in memory after checkpoint
     */
    @Test
    public void doNotKeepSmallRebuiltInMemory() throws Exception {
        String nodeId = "localhost";

        ServerConfiguration config = new ServerConfiguration();

        /* Force page compaction */
        final double minFillThreashod = 100.0D;
        config.set(ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD, minFillThreashod);

        /* Disable dirty rebuilt */
        config.set(ServerConfiguration.PROPERTY_DIRTY_PAGE_THRESHOLD, 100.0D);

        final long pageSize = 200;
        config.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, pageSize);


        try (DBManager manager = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(),
                null, null, config,
                null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1))",
                    Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey1", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",

                    Arrays.asList("mykey2", "a", Integer.valueOf(1234))).getUpdateCount());

            manager.checkpoint();

            TableManager table = (TableManager) manager.getTableSpaceManager("tblspace1").getTableManager("tsql");

            /* Get only current and 2 loaded page (+1 empty page for new records... we need the other one) */
            /* We need 2 small pages or small pages compaction will be avoided */
            Collection<DataPage> pages = table.getLoadedPages();
            assertEquals(3, pages.size());
            DataPage page = pages.stream().filter(dpage -> !dpage.writable).findFirst().get();
            assertNotNull(page);

            /* 1 records in page */
            assertEquals(1, page.size());

            /* No more space for other records */
            long avgRecordSize = page.getUsedMemory() / page.size();
            assertTrue(page.getUsedMemory() + avgRecordSize > pageSize);

            /* No more space in page BUT consider the page as small (little hacky) */
            double compactionThresholdSize = minFillThreashod / 100 * pageSize;
            assertTrue(compactionThresholdSize >= page.getUsedMemory());

            /* Force a second checkpoint to attempt to rebuild small page */

            /* Force small pages unload */
            for (DataPage dpage : pages) {
                if (!dpage.writable) {

                    /* Remove from page replacement policy */
                    manager.getMemoryManager().getDataPageReplacementPolicy().remove(dpage);

                    /* Fully unload the page */
                    table.unload(dpage.pageId);
                }
            }


            /* Now do a checkpoint with an unloaded dirty page */
            manager.checkpoint();

            /* New rebuilt page souldn't be in memory */
            pages = table.getLoadedPages();

            /* 1 page should be empty */
            assertEquals(1, pages.stream().filter(DataPage::isEmpty).count());

            /* 1 page empty and nothing else */
            assertEquals(1, pages.size());

        }
    }

    /**
     * Rebuild small pages but keep rebuilt pages in memory after checkpoint
     */
    @Test
    public void keepSmallRebuiltInMemory() throws Exception {
        String nodeId = "localhost";

        ServerConfiguration config = new ServerConfiguration();

        /* Force page compaction */
        final double minFillThreashod = 100.0D;
        config.set(ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD, minFillThreashod);

        /* Disable dirty rebuilt */
        config.set(ServerConfiguration.PROPERTY_DIRTY_PAGE_THRESHOLD, 100.0D);

        final long pageSize = 200;
        config.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, pageSize);


        try (DBManager manager = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(),
                null, null, config,
                null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1))",
                    Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey1", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",

                    Arrays.asList("mykey2", "a", Integer.valueOf(1234))).getUpdateCount());

            manager.checkpoint();

            TableManager table = (TableManager) manager.getTableSpaceManager("tblspace1").getTableManager("tsql");

            /* Get only current and 2 loaded page (+1 empty page for new records... we need the other one) */
            /* We need 2 small pages or small pages compaction will be avoided */
            Collection<DataPage> pages = table.getLoadedPages();
            assertEquals(3, pages.size());
            DataPage page = pages.stream().filter(dpage -> !dpage.writable).findFirst().get();
            assertNotNull(page);

            /* 1 records in page */
            assertEquals(1, page.size());

            /* No more space for other records */
            long avgRecordSize = page.getUsedMemory() / page.size();
            assertTrue(page.getUsedMemory() + avgRecordSize > pageSize);

            /* No more space in page BUT consider the page as small (little hacky) */
            double compactionThresholdSize = minFillThreashod / 100 * pageSize;
            assertTrue(compactionThresholdSize >= page.getUsedMemory());

            /* Force a second checkpoint to attempt to rebuild small page */

            /* Do not unload dirty page */

            /* Now do a checkpoint with an unloaded dirty page */
            manager.checkpoint();

            /* New rebuilt page souldn't be in memory */
            pages = table.getLoadedPages();

            /* 1 page should be empty */
            assertEquals(1, pages.stream().filter(DataPage::isEmpty).count());

            /* 3 pages, one empty, 2 containing old moved records */
            assertEquals(3, pages.size());

        }
    }

    @Test
    public void manyUpdates() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmpDir").toPath();
        String nodeId = "localhost";

        ServerConfiguration config1 = new ServerConfiguration();
        config1.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 1024 * 1024L);
        config1.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 2 * 1024 * 1024L);
        config1.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 2 * 1024 * 1024L);
        config1.set(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, 10 * 1000L);

        int records = 100;
        int iterations = 1_0000;
        int keylen = 25;
        int strlen = 50;

        Map<String, String> expectedValues = new HashMap<>();

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config1, null)) {

            // we want frequent checkpoints
            manager.setCheckpointPeriod(10 * 1000L);

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string, n1 string, primary key(k1))",
                    Collections.emptyList());

            Random random = new Random();
            List<String> keys = new ArrayList<>();
            long tx = TestUtils.beginTransaction(manager, "tblspace1");
            for (int i = 0; i < records; ++i) {
                String key;
                while (true) {
                    key = RandomString.getInstance().nextString(keylen);
                    if (!keys.contains(key)) {
                        keys.add(key);
                        break;
                    }
                }
                String value = RandomString.getInstance().nextString(strlen);
                executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)",
                        Arrays.asList(
                                key,
                                value),
                        new TransactionContext(tx));
                expectedValues.put(key, value);
                if (i % 1000 == 0) {
                    System.out.println("commit at " + i);
                    TestUtils.commitTransaction(manager, "tblspace1", tx);
                    tx = TestUtils.beginTransaction(manager, "tblspace1");
                }
            }
            TestUtils.commitTransaction(manager, "tblspace1", tx);
            System.out.println("database created");
            manager.checkpoint();

            TableManagerStats stats = manager.getTableSpaceManager("tblspace1").getTableManager("tsql").getStats();

            tx = TestUtils.beginTransaction(manager, "tblspace1");
            for (int i = 0; i < iterations; i++) {
                String key = keys.get(random.nextInt(keys.size()));
                String value = RandomString.getInstance().nextString(strlen);
                assertEquals(1, executeUpdate(manager, "UPDATE tblspace1.tsql set n1=? where k1=?",
                        Arrays.asList(
                                value,
                                key, new TransactionContext(tx)
                        )).getUpdateCount()
                );
                expectedValues.put(key, value);

                if (random.nextInt(1000) <= 2) {
                    System.out.println("checkpoint after " + i + " iterations");
                    manager.triggerActivator(ActivatorRunRequest.FULL);
                }

                if (i % 1000 == 0) {
                    System.out.println("commit after " + i + " iterations");
                    System.out.println("stats: dirtypages:" + stats.getDirtypages() + " unloads:" + stats.getUnloadedPagesCount());
                    TestUtils.commitTransaction(manager, "tblspace1", tx);
                    tx = TestUtils.beginTransaction(manager, "tblspace1");
                }
            }
            TestUtils.commitTransaction(manager, "tblspace1", tx);

            for (Map.Entry<String, String> expected : expectedValues.entrySet()) {
                try (DataScanner scan = TestUtils.scan(manager, "SELECT n1 FROM tblspace1.tsql where k1=?", Arrays.asList(expected.getKey()))) {
                    List<DataAccessor> all = scan.consume();
                    assertEquals(1, all.size());
                    assertEquals(RawString.of(expected.getValue()), all.get(0).get(0));
                }
            }

            manager.checkpoint();

            for (Map.Entry<String, String> expected : expectedValues.entrySet()) {
                try (DataScanner scan = TestUtils.scan(manager, "SELECT n1 FROM tblspace1.tsql where k1=?", Arrays.asList(expected.getKey()))) {
                    List<DataAccessor> all = scan.consume();
                    assertEquals(1, all.size());
                    assertEquals(RawString.of(expected.getValue()), all.get(0).get(0));
                }
            }

        }

        // reboot
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config1, null)) {

            // we want frequent checkpoints
            manager.setCheckpointPeriod(10 * 1000L);

            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(60000));

            for (Map.Entry<String, String> expected : expectedValues.entrySet()) {
                try (DataScanner scan = TestUtils.scan(manager, "SELECT n1 FROM tblspace1.tsql where k1=?", Arrays.asList(expected.getKey()))) {
                    List<DataAccessor> all = scan.consume();
                    assertEquals(1, all.size());
                    assertEquals(RawString.of(expected.getValue()), all.get(0).get(0));
                }
            }
        }

    }

    @Test
    public void checkpointAfterLockTimeout() throws Exception {
        String nodeId = "localhost";

        ServerConfiguration config1 = new ServerConfiguration();
        // 1 second
        config1.set(ServerConfiguration.PROPERTY_WRITELOCK_TIMEOUT, 1);
        config1.set(ServerConfiguration.PROPERTY_READLOCK_TIMEOUT, 1);

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

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1))",
                    Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)",
                    Arrays.asList("mykey", "a", Integer.valueOf(1234))).getUpdateCount());

            DMLStatementExecutionResult lockRecord =
                    executeUpdate(manager, "UPDATE tblspace1.tsql set s1=? where k1=?",
                            Arrays.asList("a", "mykey"), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long tx = lockRecord.transactionId;
            assertTrue(tx > 0);
            assertEquals(1, lockRecord.getUpdateCount());

            // holding a lock on the record, but not the checkpoint lock
            manager.checkpoint();

            StatementExecutionException err = herddb.utils.TestUtils.expectThrows(herddb.model.StatementExecutionException.class, () -> {
                // this update will timeout, lock is already held by the transaction
                executeUpdate(manager, "UPDATE tblspace1.tsql set s1=? where k1=?",
                        Arrays.asList("a", "mykey"), TransactionContext.NO_TRANSACTION);
            });
            assertEquals("timed out acquiring lock for write", err.getCause().getMessage());

            manager.checkpoint();

            err = herddb.utils.TestUtils.expectThrows(herddb.model.StatementExecutionException.class, () -> {
                // this select in a transaction will timeout, write lock is already held by the transaction
                scan(manager, "SELECT * FROM tblspace1.tsql where k1=?",
                        Arrays.asList("mykey"), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            });
            assertEquals("timedout trying to read lock", err.getCause().getMessage());

            manager.checkpoint();

        }
    }
}
