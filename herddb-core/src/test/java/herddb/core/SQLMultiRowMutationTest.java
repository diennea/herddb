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
import static org.junit.Assert.assertEquals;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * @author enrico.olivelli
 */
public class SQLMultiRowMutationTest {

    @Test
    public void updateMulti() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1) values(?,?)", Arrays.asList("mykey2", Integer.valueOf(1234))).getUpdateCount());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> rows = scan.consume();
                assertEquals(2, rows.size());
                assertEquals(Integer.valueOf(1234), rows.get(0).get("n1"));
                assertEquals(Integer.valueOf(1234), rows.get(1).get("n1"));
            }

            executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = 2 ", Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> rows = scan.consume();
                assertEquals(2, rows.size());
                assertEquals(Integer.valueOf(2), rows.get(0).get("n1"));
                assertEquals(Integer.valueOf(2), rows.get(1).get("n1"));
            }

            executeUpdate(manager, "UPDATE tblspace1.tsql SET n1 = 3 WHERE k1 <> 'aa' ", Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> rows = scan.consume();
                assertEquals(2, rows.size());
                assertEquals(Integer.valueOf(3), rows.get(0).get("n1"));
                assertEquals(Integer.valueOf(3), rows.get(1).get("n1"));
            }

            executeUpdate(manager, "DELETE FROM  tblspace1.tsql WHERE k1 <> 'aa' ", Collections.emptyList());

            try (DataScanner scan = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList())) {
                List<DataAccessor> rows = scan.consume();
                assertEquals(0, rows.size());
            }

        }
    }

}
