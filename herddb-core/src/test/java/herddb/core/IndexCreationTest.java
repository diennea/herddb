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

import java.nio.file.Path;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.codec.RecordSerializer;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.SecondaryIndexSeek;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.GetResult;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;

/**
 * Tests on index creation
 *
 * @author diego.salvi
 */
public class IndexCreationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void brinRecoverTableAndIndexWithCheckpoint() throws Exception {
        recoverTableAndIndexWithCheckpoint(Index.TYPE_BRIN);
    }

    @Test
    public void hashRecoverTableAndIndexWithCheckpoint() throws Exception {
        recoverTableAndIndexWithCheckpoint(Index.TYPE_HASH);
    }

    private void recoverTableAndIndexWithCheckpoint(String indexType) throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        String nodeId = "localhost";
        Table table;
        Index index;

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath),
            tmoDir, null)) {
            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            table = Table
                .builder()
                .tablespace("tblspace1")
                .name("t1")
                .column("id", ColumnTypes.INTEGER)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();


            manager.executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            manager.executeStatement(new InsertStatement("tblspace1", table.name, RecordSerializer.makeRecord(table, "id", 1, "name", "uno")), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            GetResult result = manager.get(new GetStatement("tblspace1", table.name, Bytes.from_int(1), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());

            manager.checkpoint();

        }

        try (DBManager manager = new DBManager("localhost",
            new FileMetadataStorageManager(metadataPath),
            new FileDataStorageManager(dataPath),
            new FileCommitLogManager(logsPath),
            tmoDir, null)) {
            manager.start();

            manager.waitForTablespace("tblspace1", 10000);

            index = Index.builder().onTable(table).column("name", ColumnTypes.STRING).type(indexType).build();
            manager.executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            /* Access through index  */
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name=\'uno\'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION);) {
                assertEquals(1, scan1.consume().size());
            }

        }

    }

}
