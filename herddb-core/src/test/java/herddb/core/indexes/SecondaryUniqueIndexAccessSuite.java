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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.core.TestUtils;
import herddb.index.SecondaryIndexPrefixScan;
import herddb.index.SecondaryIndexSeek;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.UniqueIndexContraintViolationException;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.ScanStatement;
import herddb.server.ServerConfiguration;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
import herddb.utils.LockAcquireTimeoutException;
import herddb.utils.RawString;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author enrico.olivelli
 */
public class SecondaryUniqueIndexAccessSuite {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    protected String indexType;

    public SecondaryUniqueIndexAccessSuite() {
        this.indexType = Index.TYPE_HASH;
    }

    @Test
    public void secondaryUniqueIndexPrefixScan() throws Exception {
        String nodeId = "localhost";
        Path tmp = tmpDir.newFolder().toPath();
        ServerConfiguration serverConfiguration = new ServerConfiguration(tmp);
        serverConfiguration.set(ServerConfiguration.PROPERTY_READLOCK_TIMEOUT, 3);
        serverConfiguration.set(ServerConfiguration.PROPERTY_WRITELOCK_TIMEOUT, 3);
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), tmp, null,
                serverConfiguration, NullStatsLogger.INSTANCE)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("id", ColumnTypes.STRING)
                    .column("n1", ColumnTypes.INTEGER)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            Index index = Index
                    .builder()
                    .onTable(table)
                    .type(Index.TYPE_HASH)
                    .unique(true)
                    .column("n1", ColumnTypes.INTEGER)
                    .column("name", ColumnTypes.STRING).
                            build();

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('a',1,'n1')", Collections.emptyList());

            // create index, it will be built using existing data
            CreateIndexStatement st3 = new CreateIndexStatement(index);
            manager.executeStatement(st3, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // cannot insert another record with '1-n1'
            herddb.utils.TestUtils.assertThrows(UniqueIndexContraintViolationException.class, () -> {
                TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('b',1,'n1')", Collections.emptyList());
            });
            // multiple values
            // it is not a transaction, the first INSERT will succeed
            herddb.utils.TestUtils.assertThrows(UniqueIndexContraintViolationException.class, () -> {
                TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('b',8,'n1'),('c',1,'n1')", Collections.emptyList());
            });
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('d',2,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('e',3,'n2')", Collections.emptyList());

            // single record UPDATE
            herddb.utils.TestUtils.assertThrows(UniqueIndexContraintViolationException.class, () -> {
                TestUtils.executeUpdate(manager, "UPDATE tblspace1.t1 set n1=1,name='n1' where id='d'", Collections.emptyList());
            });

            // multi record UPDATE
            herddb.utils.TestUtils.assertThrows(UniqueIndexContraintViolationException.class, () -> {
                TestUtils.executeUpdate(manager, "UPDATE tblspace1.t1 set n1=1,name='n1' where id='d'", Collections.emptyList());
            });


            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1=1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexPrefixScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(1, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1=8", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexPrefixScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(1, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1=2 and name='n2'", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexSeek);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(1, scan1.consume().size());
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1>=1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertNull(scan.getPredicate().getIndexOperation());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    assertEquals(4, scan1.consume().size());
                }
            }

            // update the index
            TestUtils.executeUpdate(manager, "UPDATE tblspace1.t1 set n1=10,name='n1' where id='a'", Collections.emptyList());
            TestUtils.executeUpdate(manager, "UPDATE tblspace1.t1 set n1=1,name='n1' where id='d'", Collections.emptyList());

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT * FROM tblspace1.t1 WHERE n1=1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof SecondaryIndexPrefixScan);
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> consume = scan1.consume();
                    assertEquals(1, consume.size());
                    assertEquals(RawString.of("d"), consume.get(0).get("id"));
                }
            }

            // delete, update the index
            TestUtils.executeUpdate(manager, "DELETE FROM tblspace1.t1 where id='d'", Collections.emptyList());
            // enure another record can be stored on the same key
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('o',1,'n1')", Collections.emptyList());


            // test transactions
            long tx = TestUtils.beginTransaction(manager, "tblspace1");
            // insert a new record, and a new index entry, but in transaction, the transacition will hold the index lock on "100-n100"
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('t1',100,'n100')", Collections.emptyList(), new TransactionContext(tx));
            Transaction transaction = manager.getTableSpaceManager("tblspace1").getTransaction(tx);
            assertEquals(1, transaction.locks.get("t1_n1_name").size());
            // delete the same record, the index still hasn't been touched, but we are still holding a lock on "100-n100"
            TestUtils.executeUpdate(manager, "DELETE FROM  tblspace1.t1 where id='t1'", Collections.emptyList(), new TransactionContext(tx));
            // insert a new record again, with the same key in the index, we are still holding the lock on "100-n100"
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('t2',100,'n100')", Collections.emptyList(), new TransactionContext(tx));

            // check that we are going to timeout on lock acquisition from another context (for instance no transaction)
            StatementExecutionException err = herddb.utils.TestUtils.expectThrows(StatementExecutionException.class, () -> {
                TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('t3',100,'n100')", Collections.emptyList());
            });
            assertThat(err.getCause(), instanceOf(LockAcquireTimeoutException.class));

            TestUtils.commitTransaction(manager, "tblspace1", tx);

            // cannot insert another record with "100-n100"
            herddb.utils.TestUtils.assertThrows(UniqueIndexContraintViolationException.class, () -> {
                TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,name) values('t3',100,'n100')", Collections.emptyList());
            });


        }

    }

}
