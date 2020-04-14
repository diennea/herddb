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
package herddb.data.consistency;

import static herddb.core.TestUtils.execute;
import herddb.core.DBManager;
import herddb.core.ReplicatedLogtestcase;
import herddb.core.TestUtils;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.TableConsistencyCheckStatement;
import herddb.utils.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;


/**
 *
 * @author Hamado.Dene
 */
public class TableDataCheckSumTest extends ReplicatedLogtestcase {

    @Test
    public void consistencyCheckReplicaTest() throws Exception {
        final String tableName = "table1";
        final String tableSpaceName = "t2";
        try (DBManager manager1 = startDBManager("node1")) {

            manager1.executeStatement(new CreateTableSpaceStatement(tableSpaceName, new HashSet<>(Arrays.asList("node1", "node2")), "node1", 2, 0, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager1.waitForTablespace(tableSpaceName, 10000, true);
            try (DBManager manager2 = startDBManager("node2")) {
                manager2.waitForTablespace(tableSpaceName, 10000, false);
                manager1.executeStatement(new CreateTableStatement(Table.builder().tablespace(tableSpaceName).name(tableName).primaryKey("key").column("key", ColumnTypes.STRING).build()), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                manager1.waitForTable(tableSpaceName, tableName, 10000, true);
                manager1.executeStatement(new InsertStatement(tableSpaceName, tableName, new Record(Bytes.from_string("one"), Bytes.from_string("two"))), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                manager1.executeStatement(new InsertStatement(tableSpaceName, tableName, new Record(Bytes.from_string("second"), Bytes.from_string("two"))), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                manager2.waitForTable(tableSpaceName, tableName, 10000, false);

                TableConsistencyCheckStatement statement = new TableConsistencyCheckStatement("table1", "t2");
                manager1.createTableCheksum(statement, null);

                manager1.executeStatement(new InsertStatement(tableSpaceName, tableName, new Record(Bytes.from_string("kk"), Bytes.from_string("kk"))), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            }
        }
    }

    @Test
    public void consistencyCheckSyntax() throws Exception {

        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            execute(manager, "CREATE TABLESPACE 'consistence'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE consistence.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE consistence.tsql1 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            execute(manager, "CREATE TABLE consistence.tsql2 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());

            for (int i = 0; i < 10; i++) {
                System.out.println("insert number " + i);
                execute(manager, "INSERT INTO consistence.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
                execute(manager, "INSERT INTO consistence.tsql1 (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
                execute(manager, "INSERT INTO consistence.tsql2 (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM consistence.tsql", Collections.emptyList())) {
            }
            execute(manager, "tableconsistencycheck consistence.tsql1", Collections.emptyList());
            execute(manager, "tableconsistencycheck consistence.tsql2", Collections.emptyList());
            execute(manager, "tablespaceconsistencycheck consistence", Collections.emptyList());
            execute(manager, "tablespaceconsistencycheck 'consistence'", Collections.emptyList());

        }
    }

}
