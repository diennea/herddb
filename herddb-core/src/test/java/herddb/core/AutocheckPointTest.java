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
import static org.junit.Assert.assertNotEquals;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.Tuple;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;

/**
 *
 *
 * @author enrico.olivelli
 */
public class AutocheckPointTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void autoCheckPointTest() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId,
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath),
            tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 int ,s1 string,n1 int, primary key(k1))", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList(1, "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList(2, "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList(3, "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList(4, "a", Integer.valueOf(1234))).getUpdateCount());

            manager.checkpoint();
            long tx = ((TransactionResult) execute(manager, "EXECUTE begintransaction 'tblspace1'", Collections.emptyList())).getTransactionId();
            execute(manager, "UPDATE tblspace1.tsql set s1='b' where k1=1", Collections.emptyList(), new TransactionContext(tx));

            long lastCheckpont = manager.getLastCheckPointTs();
            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql WHERE N1=1234", Collections.emptyList(), new TransactionContext(tx))) {
                List<DataAccessor> data = scan.consume();
                assertEquals(4, data.size());
            }
            manager.setCheckpointPeriod(1000);
            for (int i = 0; i < 100; i++) {
                if (lastCheckpont != manager.getLastCheckPointTs()) {
                    break;
                }
                Thread.sleep(100);
            }
            assertNotEquals(lastCheckpont, manager.getLastCheckPointTs());
            execute(manager, "EXECUTE committransaction 'tblspace1'," + tx, Collections.emptyList());

            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql WHERE N1=1234 and s1='b'", Collections.emptyList()).consumeAndClose().size());

        }
    }

    @Test
//    @Ignore
    public void autoCheckPointDuringActivityTest() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId,
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath),
            tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 int ,s1 string,n1 int, primary key(k1))", Collections.emptyList());

            for (int i = 0; i < 100; i++) {
                assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList(i, "a", Integer.valueOf(1234))).getUpdateCount());
            }

            manager.checkpoint();

            long lastCheckpont = manager.getLastCheckPointTs();
            // we want to checkpoint very ofter
            manager.setCheckpointPeriod(100);

            Random random = new Random();
            for (int trial = 0; trial < 1000; trial++) {
                int i = random.nextInt(100);
                execute(manager, "UPDATE tblspace1.tsql set s1='b" + trial + "' where k1=?", Arrays.asList(i), TransactionContext.NO_TRANSACTION);
                Thread.sleep(10);
            }

            assertNotEquals(lastCheckpont, manager.getLastCheckPointTs());

        }
    }

}
