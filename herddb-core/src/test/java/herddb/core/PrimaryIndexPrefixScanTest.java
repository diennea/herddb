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
import static org.junit.Assert.assertTrue;
import herddb.file.FileDataStorageManager;
import herddb.index.PrimaryIndexPrefixScan;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author amitchavan
 */
public class PrimaryIndexPrefixScanTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void validatePrefixScanIndexWorksForValidDataSet() throws Exception {

        String nodeId = "localhost";
        Path dataPath = folder.newFolder("data").toPath();
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.INTEGER)
                    .column("n2", ColumnTypes.INTEGER)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .primaryKey("n2")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'r1')", Collections.emptyList());
            //Same first part of the primary key as the record above but 2nd half is different.
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',1,6,'r2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',3,6,'r3')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d',4,7,'r4')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e',5,5,'r5')", Collections.emptyList());

            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1=1", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexPrefixScan);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                List<DataAccessor> rows = scan1.consume();
                // Assert we got 2 rows.
                assertEquals(2, rows.size());
                for (DataAccessor row : rows) {

                    if (row.get("name").equals("r1")) {
                        assertEquals(row.get("n1"), 1);
                        assertEquals(row.get("n2"), 5);
                    } else {
                        assertEquals(row.get("name"), "r2");
                        assertEquals(row.get("n1"), 1);
                        assertEquals(row.get("n2"), 6);
                    }
                }
            }
        }

    }
}
