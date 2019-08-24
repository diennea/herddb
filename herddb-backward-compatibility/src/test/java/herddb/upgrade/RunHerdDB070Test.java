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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.ScanResult;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Upgrade a 0.5.0 database with BRIN indexes
 *
 * @author enrico.olivelli
 */
public class RunHerdDB070Test {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        String file = "herddb.070.joinerror.zip";

        File dbdatadir = folder.newFolder("dbdata070_" + file);

        try (InputStream in = RunHerdDB070Test.class.getResourceAsStream(file)) {
            ZIPUtils.unZip(in, dbdatadir);
        }
        System.out.println("UNZIPPED TO " + dbdatadir);
        final Path dbdata = dbdatadir.toPath().resolve("herddb.070.joinerror").resolve("dbdata");

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
        String expectedNodeId = "asino";
        assertTrue(id.endsWith("\n" + expectedNodeId));

        try (DBManager manager = new DBManager(expectedNodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            final String tableSpace = "herd";
            final String tableName = "testtable";
            assertEquals(expectedNodeId, manager.getNodeId());

            assertTrue(manager.waitForTablespace(tableSpace, 10000));

            AbstractTableManager tableManagerLicense = manager.getTableSpaceManager("herd").getTableManager("license");
            Table tableLicense = tableManagerLicense.getTable();
            System.out.println("TABLE PK: " + Arrays.toString(tableLicense.primaryKey));
            for (Column c : tableLicense.columns) {
                System.out.println("COL: " + c.name + " serialPos: " + c.serialPosition);
            }

            AbstractTableManager tableManagerCustomer = manager.getTableSpaceManager("herd").getTableManager("customer");
            Table tableCustomer = tableManagerCustomer.getTable();
            System.out.println("TABLE PK: " + Arrays.toString(tableCustomer.primaryKey));
            for (Column c : tableCustomer.columns) {
                System.out.println("COL: " + c.name + " serialPos: " + c.serialPosition);
            }

            {
                TranslatedQuery translated = manager
                        .getPlanner().translate(tableSpace,
                                "SELECT * FROM license",
                                Collections.emptyList(),
                                true, true, false, -1);
                System.out.println("TABLE CONTENTS");
                try (DataScanner scan1 = ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner) {

                    List<DataAccessor> consume = scan1.consume();
                    System.out.println("NUM " + consume.size());
                    assertEquals(15, consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }
                }
            }
            {
                TranslatedQuery translated = manager
                        .getPlanner().translate(tableSpace,
                                "SELECT * FROM customer",
                                Collections.emptyList(),
                                true, true, false, -1);
                System.out.println("TABLE CONTENTS");
                try (DataScanner scan1 = ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner) {

                    List<DataAccessor> consume = scan1.consume();
                    System.out.println("NUM " + consume.size());
                    assertEquals(7, consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }

                }
            }

            {
                TranslatedQuery translated = manager
                        .getPlanner().translate(tableSpace,
                                "SELECT * FROM license t0, customer c WHERE c.customer_id = 3 AND t0.customer_id = 3 AND c.customer_id = t0.customer_id\n"
                                        + "            ",
                                Collections.emptyList(),
                                true, true, false, -1);
                System.out.println("TABLE CONTENTS");
                try (DataScanner scan1 = ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner) {

                    List<DataAccessor> consume = scan1.consume();
                    System.out.println("NUM " + consume.size());
                    assertEquals(9, consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }
                }
            }

            {
                TranslatedQuery translated = manager
                        .getPlanner().translate(tableSpace,
                                "SELECT * FROM license t0, customer c WHERE c.customer_id = t0.customer_id",
                                Collections.emptyList(),
                                true, true, false, -1);
                System.out.println("TABLE CONTENTS");
                try (DataScanner scan1 = ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner) {

                    List<DataAccessor> consume = scan1.consume();
                    System.out.println("NUM " + consume.size());
                    assertEquals(15, consume.size());
                    for (DataAccessor r : consume) {
                        System.out.println("RECORD " + r.toMap());
                    }
                }
            }
        }
    }

}
