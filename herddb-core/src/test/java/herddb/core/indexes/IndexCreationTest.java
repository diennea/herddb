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

package herddb.core.indexes;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.AbstractIndexManager;
import herddb.core.DBManager;
import herddb.core.TestUtils;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.SecondaryIndexSeek;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.GetResult;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionResult;
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
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests on index creation
 *
 * @author diego.salvi
 */
public class IndexCreationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void brinNonUniqueRecoverTableAndIndexWithCheckpoint() throws Exception {
        recoverTableAndIndexWithCheckpoint(Index.TYPE_BRIN, false);
    }

    @Test
    public void hashNonUniqueRecoverTableAndIndexWithCheckpoint() throws Exception {
        recoverTableAndIndexWithCheckpoint(Index.TYPE_HASH, false);
    }

    @Test
    public void hashUniqueRecoverTableAndIndexWithCheckpoint() throws Exception {
        recoverTableAndIndexWithCheckpoint(Index.TYPE_HASH, true);
    }

    private void recoverTableAndIndexWithCheckpoint(String indexType, boolean unique) throws Exception {

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

            index = Index.builder().onTable(table).column("name", ColumnTypes.STRING).type(indexType).unique(unique).build();
            manager.executeStatement(new CreateIndexStatement(index), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            /* Access through index  */
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE name=\'uno\'", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(1, scan1.consume().size());
            }

        }

    }

    @Test
    public void caseSensitivity() throws Exception {

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();

            CreateTableSpaceStatement st1 =
                    new CreateTableSpaceStatement("tbl1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tbl1", 10000);

            // Table create uppercase, index create uppercase
            {
                execute(manager,
                        "CREATE TABLE tbl1.TABLE_1 (TABLE_ID BIGINT NOT NULL, FIELD INT NOT NULL, PRIMARY KEY (TABLE_ID))",
                        Collections.emptyList());

                execute(manager, "CREATE INDEX TABLE_1_INDEX ON tbl1.TABLE_1(TABLE_ID,FIELD)", Collections.emptyList());
            }

            // Table create lowercase, index create lowercase
            {
                execute(manager,
                        "CREATE TABLE tbl1.table_2 (TABLE_ID BIGINT NOT NULL, FIELD INT NOT NULL, PRIMARY KEY (TABLE_ID))",
                        Collections.emptyList());

                execute(manager, "CREATE INDEX table_2_index ON tbl1.table_2(TABLE_ID,FIELD)", Collections.emptyList());
            }

            // Table create uppercase, index create lowercase
            {
                execute(manager,
                        "CREATE TABLE tbl1.TABLE_3 (TABLE_ID BIGINT NOT NULL, FIELD INT NOT NULL, PRIMARY KEY (TABLE_ID))",
                        Collections.emptyList());

                execute(manager, "CREATE INDEX TABLE_3_INDEX ON tbl1.table_3(TABLE_ID,FIELD)", Collections.emptyList());
            }

            // Table create lowercase, index create uppercase
            {
                execute(manager,
                        "CREATE TABLE tbl1.table_4 (TABLE_ID BIGINT NOT NULL, FIELD INT NOT NULL, PRIMARY KEY (TABLE_ID))",
                        Collections.emptyList());

                execute(manager, "CREATE INDEX table_4_index ON tbl1.TABLE_4(TABLE_ID,FIELD)", Collections.emptyList());
            }

            // create index and table with one single statement
            execute(manager,
                        "CREATE TABLE tbl1.table_5 (TABLE_ID BIGINT NOT NULL,"
                                + "FIELD STRING NOT NULL,"
                                + "SECONDFIELD TIMESTAMP UNIQUE," // single unique column
                                + "PRIMARY KEY (TABLE_ID),"
                                + "INDEX t5index(field),"
                                + "KEY t5index2(field,secondfield),"
                                + "UNIQUE KEY index5u(field,table_id)" // we don't support "UNIQUE INDEX", but "UNIQUE KEY"
                                + ")",
                        Collections.emptyList());
            Map<String, AbstractIndexManager> indexesOnTable = manager.getTableSpaceManager("tbl1").getIndexesOnTable("table_5");
            assertFalse(indexesOnTable.get("t5index").isUnique());
            assertEquals(ColumnTypes.NOTNULL_STRING, indexesOnTable.get("t5index").getIndex().columnByName.get("field").type);
            assertFalse(indexesOnTable.get("t5index2").isUnique());
            assertEquals(ColumnTypes.NOTNULL_STRING, indexesOnTable.get("t5index2").getIndex().columnByName.get("field").type);
            assertEquals(ColumnTypes.TIMESTAMP, indexesOnTable.get("t5index2").getIndex().columnByName.get("secondfield").type);
            assertTrue(indexesOnTable.get("index5u").isUnique());
            assertEquals(ColumnTypes.NOTNULL_STRING, indexesOnTable.get("index5u").getIndex().columnByName.get("field").type);
            assertEquals(ColumnTypes.NOTNULL_LONG, indexesOnTable.get("index5u").getIndex().columnByName.get("table_id").type);
            assertTrue(indexesOnTable.get("table_5_unique_secondfield").isUnique());
            assertEquals(ColumnTypes.TIMESTAMP, indexesOnTable.get("table_5_unique_secondfield").getIndex().columnByName.get("secondfield").type);
            assertEquals("Missing some index " + indexesOnTable.keySet(), 4, indexesOnTable.size());


            // create index and table with one single statement, in transaction
            StatementExecutionResult execute =
                    execute(manager,
                            "CREATE TABLE tbl1.table_6 (TABLE_ID BIGINT NOT NULL,"
                                    + "FIELD STRING NOT NULL,"
                                    + "SECONDFIELD TIMESTAMP UNIQUE," // single unique column
                                    + "PRIMARY KEY (TABLE_ID),"
                                    + "INDEX t6index(field),"
                                    + "KEY t6index2(field,secondfield),"
                                    + "UNIQUE KEY index6u(field,table_id)" // we don't support "UNIQUE INDEX", but "UNIQUE KEY"
                                    + ")",
                            Collections.emptyList(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long tx = execute.transactionId;
            indexesOnTable = manager.getTableSpaceManager("tbl1").getIndexesOnTable("table_6");
            assertFalse(indexesOnTable.get("t6index").isUnique());
            assertEquals(ColumnTypes.NOTNULL_STRING, indexesOnTable.get("t6index").getIndex().columnByName.get("field").type);
            assertFalse(indexesOnTable.get("t6index2").isUnique());
            assertEquals(ColumnTypes.NOTNULL_STRING, indexesOnTable.get("t6index2").getIndex().columnByName.get("field").type);
            assertEquals(ColumnTypes.TIMESTAMP, indexesOnTable.get("t6index2").getIndex().columnByName.get("secondfield").type);
            assertTrue(indexesOnTable.get("index6u").isUnique());
            assertEquals(ColumnTypes.NOTNULL_STRING, indexesOnTable.get("index6u").getIndex().columnByName.get("field").type);
            assertEquals(ColumnTypes.NOTNULL_LONG, indexesOnTable.get("index6u").getIndex().columnByName.get("table_id").type);
            assertTrue(indexesOnTable.get("table_6_unique_secondfield").isUnique());
            assertEquals(ColumnTypes.TIMESTAMP, indexesOnTable.get("table_6_unique_secondfield").getIndex().columnByName.get("secondfield").type);
            assertEquals("Missing some index " + indexesOnTable.keySet(), 4, indexesOnTable.size());
            TestUtils.commitTransaction(manager, "tbl1", tx);
        }

    }
}
