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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableStatement;

/**
 * Recovery from file
 *
 * @author enrico.olivelli
 */
public class LocalTableSnapshotRecoveryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test_1() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace(TableSpace.DEFAULT, 10000);

            Table table = Table
                    .builder()
                    .tablespace(TableSpace.DEFAULT)
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.execute(manager, "INSERT INTO t1(id,name) values(?,?)", Arrays.asList("a", "b"));
            manager.getTableSpaceManager(TableSpace.DEFAULT).checkpoint();
            TestUtils.execute(manager, "INSERT INTO t1(id,name) values(?,?)", Arrays.asList("c", "d"));
            manager.getTableSpaceManager(TableSpace.DEFAULT).getTableManager("t1").flush();
            TestUtils.execute(manager, "INSERT INTO t1(id,name) values(?,?)", Arrays.asList("e", "f"));
            try (DataScanner scan = TestUtils.scan(manager, "SELECT * FROM t1", Collections.emptyList());) {
                assertEquals(3, scan.consume().size());
            }

        }

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath, 64 * 1024 * 1024),
                tmoDir, null)) {
            manager.start();

            manager.waitForTablespace(TableSpace.DEFAULT, 10000);
            try (DataScanner scan = TestUtils.scan(manager, "SELECT * FROM t1", Collections.emptyList());) {
                assertEquals(3, scan.consume().size());
            }
        }

    }

}
