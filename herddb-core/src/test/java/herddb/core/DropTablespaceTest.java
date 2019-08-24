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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.DropTableSpaceStatement;
import herddb.server.ServerConfiguration;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Drop Tablespace and Tables Tests
 *
 * @author enrico.olivelli
 */
public class DropTablespaceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM tblspace1.tsql ", Collections.emptyList())) {
                Number count = (Number) scan.consume().get(0).get(0);
                assertEquals(0, count.intValue());
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM systablespaces WHERE tablespace_name=?", Arrays.asList("tblspace1"))) {
                Number count = (Number) scan.consume().get(0).get(0);
                assertEquals(1, count.intValue());
            }
            manager.executeStatement(new DropTableSpaceStatement("tblspace1"), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM systablespaces WHERE tablespace_name=?", Arrays.asList("tblspace1"))) {
                Number count = (Number) scan.consume().get(0).get(0);
                assertEquals(0, count.intValue());
            }
            boolean ok = false;
            for (int i = 0; i < 100; i++) {
                try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM tblspace1.tsql ", Collections.emptyList())) {
                    Thread.sleep(200);
                } catch (herddb.model.StatementExecutionException expected) {
                    ok = true;
                }
            }
            assertTrue(ok);

            // create again the table space, all data should be lost
            CreateTableSpaceStatement st1_2 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1_2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM tblspace1.tsql ", Collections.emptyList())) {
                Number count = (Number) scan.consume().get(0).get(0);
                assertEquals(0, count.intValue());
            }
        }
    }

    @Test
    public void testCleanPolicyOnDropTable() throws Exception {
        String nodeId = "localhost";
        ServerConfiguration serverConfig = new ServerConfiguration();
        // very low memory
        serverConfig.set(ServerConfiguration.PROPERTY_MAX_DATA_MEMORY, 2_000_000);
        serverConfig.set(ServerConfiguration.PROPERTY_MAX_PK_MEMORY, 2_000_000);
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new MemoryCommitLogManager(false),
                tmoDir, null, serverConfig, NullStatsLogger.INSTANCE)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE tblspace1.tsql2 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            for (int i = 0; i < 10000; i++) {
                execute(manager, "INSERT INTO tblspace1.tsql (k1 ,n1 ,s1 ) values(?,?,?)",
                        Arrays.asList(i, i, "jkhdskfhjdskfhskdjfhksdhfkshfkjhsdlkhflksjhfklshdkljhsdlkf"
                                + "ksdjhfkdsflksdjhfklsdhjflksdhjflksdhjfklsdhjfklsdjhfsd"
                                + "skjhsdlkfjhdslkfjhsdlkfjhsdlkfhjsklhdfkldshjflksjhdflkhsdf"
                                + "jsdklfjhdslkfjhsdlkfhjslkjhfdlksjhdfklshjdflkhslkfdhslkfhslkfhslkdjfhklsdhjflkhsfd"
                                + "jklsdhfksdjhflksdjhflksjhfdlkjhsdlfkhjslkfjhslkdfjhklsdhjfklhsdff"
                                + "lksdjhflkshdjflkhjsdlkfjhsldkfjhslkfdfoo" + i));
            }
            // drop the table, any reference to the pages should be discarded
            execute(manager, "DROP TABLE tblspace1.tsql", Collections.emptyList());

            // write data to the second table, there will be no more space for table1 and if
            // any page is still reference we should see the error of
            // https://github.com/diennea/herddb/issues/434
            for (int i = 0; i < 3000; i++) {
                System.out.println("after " + i);
                execute(manager, "INSERT INTO tblspace1.tsql2 (k1 ,n1 ,s1 ) values(?,?,?)",
                        Arrays.asList(i, i, "jkhdskfhjdskfhskdjfhksdhfkshfkjhsdlkhflksjhfklshdkljhsdlkf"
                                + "ksdjhfkdsflksdjhfklsdhjflksdhjflksdhjfklsdhjfklsdjhfsd"
                                + "skjhsdlkfjhdslkfjhsdlkfjhsdlkfhjsklhdfkldshjflksjhdflkhsdf"
                                + "jsdklfjhdslkfjhsdlkfhjslkjhfdlksjhdfklshjdflkhslkfdhslkfhslkfhslkdjfhklsdhjflkhsfd"
                                + "jklsdhfksdjhflksdjhflksjhfdlkjhsdlfkhjslkfjhslkdfjhklsdhjfklhsdff"
                                + "lksdjhflkshdjflkhjsdlkfjhsldkfjhslkfdfoo" + i));
            }
        }
    }

}
