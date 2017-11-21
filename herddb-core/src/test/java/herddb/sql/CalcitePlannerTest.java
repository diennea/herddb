/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache Licensename, Version 2.0 (the
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
package herddb.sql;

import herddb.core.DBManager;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.scan;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.SQLPlannedOperationStatement;
import herddb.model.planner.BindableTableScanOp;
import herddb.model.planner.PlannerOp;
import herddb.model.planner.ProjectedTableScanOp;
import herddb.model.planner.TableScanOp;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class CalcitePlannerTest {

    @Test
    public void simpleInsertTests() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "INSERT INTO tblspace1.tsql (k1,n1) values(?,?)", Arrays.asList("mykey", 1234), TransactionContext.NO_TRANSACTION);
            try (DataScanner scan = scan(manager, "SELECT n1,k1 FROM tblspace1.tsql where k1='mykey'", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            }

            assertInstanceOf(plan(manager, "select * from tblspace1.tsql"), TableScanOp.class);
            assertInstanceOf(plan(manager, "select * from tblspace1.tsql where n1=1"), BindableTableScanOp.class);
            assertInstanceOf(plan(manager, "select n1 from tblspace1.tsql"), BindableTableScanOp.class);
        }
    }

    private static PlannerOp plan(final DBManager manager, String query) throws StatementExecutionException {
        TranslatedQuery translate = manager.getPlanner().translate(TableSpace.DEFAULT,
                query,
                Collections.emptyList(),
                true, true, true, -1);
        return translate.plan.mainStatement.unwrap(SQLPlannedOperationStatement.class).getRootOp();
    }

    private static void assertInstanceOf(PlannerOp plan, Class<?> aClass) {
        Assert.assertTrue("expecting " + aClass + " but found " + plan.getClass(),
                plan.getClass().equals(aClass));
    }

}
