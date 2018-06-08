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
import herddb.model.Index;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
    public void testStart() throws Exception {

        File dbdatadir = folder.newFolder("dbdata050");

        try (InputStream in = UpgradeFrom050WithBrinIndexesTest.class.getResourceAsStream("dbdata_0.5.0_brin_indexes.zip");) {
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
        assertTrue(id.endsWith("toro"));

        try (DBManager manager = new DBManager("toro",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();
            final String tableSpace = "herddb";
            final String tableName = "testtable";
            assertEquals("toro", manager.getNodeId());

            assertTrue(manager.waitForTablespace(tableSpace, 10000));

            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
            AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);

            List<Index> indexes = tableManager.getAvailableIndexes();
            for (Index e : indexes) {
                System.out.println("INDEX: " + e);
            }
            assertEquals(3, indexes.size());

        }
    }
}
