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
import static herddb.model.TransactionContext.NO_TRANSACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import herddb.utils.RandomString;
import herddb.utils.RawString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
            null, null, config1);) {
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
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmpDir, null, config1)) {
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
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmpDir, null, config1)) {
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
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmpDir, null, config1)) {
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
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmpDir, null, config2)) {
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
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmpDir, null, config1)) {

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
                try (DataScanner scan = TestUtils.scan(manager, "SELECT n1 FROM tblspace1.tsql where k1=?", Arrays.asList(expected.getKey()));) {
                    List<DataAccessor> all = scan.consume();
                    assertEquals(1, all.size());
                    assertEquals(RawString.of(expected.getValue()), all.get(0).get(0));
                }
            }

            manager.checkpoint();

            for (Map.Entry<String, String> expected : expectedValues.entrySet()) {
                try (DataScanner scan = TestUtils.scan(manager, "SELECT n1 FROM tblspace1.tsql where k1=?", Arrays.asList(expected.getKey()));) {
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
            new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
            tmpDir, null, config1)) {

            // we want frequent checkpoints
            manager.setCheckpointPeriod(10 * 1000L);

            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(60000));

            for (Map.Entry<String, String> expected : expectedValues.entrySet()) {
                try (DataScanner scan = TestUtils.scan(manager, "SELECT n1 FROM tblspace1.tsql where k1=?", Arrays.asList(expected.getKey()));) {
                    List<DataAccessor> all = scan.consume();
                    assertEquals(1, all.size());
                    assertEquals(RawString.of(expected.getValue()), all.get(0).get(0));
                }
            }
        }

    }

}
