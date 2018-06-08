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
package herddb.upgrade;

import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.IndexOperation;
import herddb.index.SecondaryIndexPrefixScan;
import herddb.index.SecondaryIndexRangeScan;
import herddb.index.SecondaryIndexSeek;
import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.Index;
import herddb.model.StatementExecutionException;
import herddb.model.TransactionContext;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Upgrade a 0.5.0 database with BRIN indexes
 *
 * @author enrico.olivelli
 */
public class UpgradeFrom050WithBrinIndexesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testWithoutCheckpoint() throws Exception {
        test("dbdata_0.5.0_brin_indexes_nevercheckpointed.zip");
    }

    @Test
    public void testWithCheckpoint() throws Exception {
        test("dbdata_0.5.0_brin_indexes_after_checkpoint.zip");
    }

    private void test(String file) throws Exception {

        File dbdatadir = folder.newFolder("dbdata050_" + file);

        try (InputStream in = UpgradeFrom050WithBrinIndexesTest.class.getResourceAsStream(file);) {
            ZIPUtils.unZip(in, dbdatadir);
        }
        final Path dbdata = dbdatadir.toPath().resolve("dbdata");

        Path metadataPath = dbdata.resolve("metadata");
        Path dataPath = dbdata.resolve("data");
        Path logsPath = dbdata.resolve("txlog");
        Path tmoDir = dbdata.resolve("tmp");
        assertTrue(Files.isDirectory(metadataPath));
        assertTrue(Files.isDirectory(dataPath));
        assertTrue(Files.isDirectory(logsPath));
        Path nodeid = dataPath.resolve("nodeid");
        assertTrue(Files.isRegularFile(nodeid));
        String id = new String(Files.readAllBytes(nodeid), StandardCharsets.UTF_8);
        System.out.println("id:" + id);
        String expectedNodeId = "capra";
        assertTrue(id.endsWith("\n" + expectedNodeId));

        try (DBManager manager = new DBManager(expectedNodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();
            final String tableSpace = "herd";
            final String tableName = "testtable";
            assertEquals(expectedNodeId, manager.getNodeId());

            assertTrue(manager.waitForTablespace(tableSpace, 10000));

            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
            AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);

            List<Index> indexes = tableManager.getAvailableIndexes();
            for (Index e : indexes) {
                System.out.println("INDEX: " + e);
                assertEquals(e.type, Index.TYPE_BRIN);
            }
            assertEquals(4, indexes.size());

            for (Column c : tableManager.getTable().getColumns()) {
                System.out.println("COLUMN :" + c);
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(tableSpace, "SELECT * FROM " + tableName + " ORDER BY pk,n1,n2",
                        Collections.emptyList(),
                        true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                System.out.println("TABLE CONTENTS");
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {

                    for (DataAccessor r : scan1.consume()) {
                        System.out.println("RECORD " + r.toMap());
                    }
                }
            }

            test(new TestCase("SELECT * FROM " + tableSpace + "." + tableName + " WHERE n1=1", SecondaryIndexSeek.class, 4),
                    manager, tableSpace);
            // this could be SecondaryIndexSeek but we have more indexes and the planner is not so smart
            test(new TestCase("SELECT * FROM " + tableSpace + "." + tableName + " WHERE n2=3", SecondaryIndexPrefixScan.class, 2),
                    manager, tableSpace);
            test(new TestCase("SELECT * FROM " + tableSpace + "." + tableName + " WHERE n2>=3", SecondaryIndexRangeScan.class, 3),
                    manager, tableSpace);
            test(new TestCase("SELECT * FROM " + tableSpace + "." + tableName + " WHERE n1=1 and n2=3", SecondaryIndexPrefixScan.class, 1),
                    manager, tableSpace);

        }
    }

    private void test(TestCase tcase, final DBManager manager, final String tableSpace) throws StatementExecutionException, DataScannerException {
        System.out.println("QUERY: " + tcase.query);
        TranslatedQuery translated = manager.getPlanner().translate(tableSpace, tcase.query,
                Collections.emptyList(),
                true, true, false, -1);
        ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);

        int size = 0;
        try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {

            for (DataAccessor r : scan1.consume()) {
                System.out.println("FOUND " + r.toMap());
                size++;
            }

        }
        System.out.println("SIZE: " + size);
        IndexOperation indexOperation = scan.getPredicate().getIndexOperation();
        System.out.println("OPERATION: " + indexOperation);

        assertTrue(tcase.expectedIndexAccessType.isAssignableFrom(scan.getPredicate().getIndexOperation().getClass()));
        assertEquals(tcase.expectedRecordCount, size);
    }

    private static final class TestCase {

        private String query;
        private Class<? extends IndexOperation> expectedIndexAccessType;
        private int expectedRecordCount;

        public TestCase(String query, Class<? extends IndexOperation> expectedIndexAccessType, int expectedRecordCount) {
            this.query = query;
            this.expectedIndexAccessType = expectedIndexAccessType;
            this.expectedRecordCount = expectedRecordCount;
        }

    }
}
