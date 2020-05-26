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

import herddb.core.DBManager;
import herddb.core.ReplicatedLogtestcase;
import static herddb.core.TestUtils.execute;
import herddb.model.ColumnTypes;
import herddb.model.DataConsistencyStatementResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.TableConsistencyCheckStatement;
import herddb.utils.SystemInstrumentation;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author hamado
 */
public class ConsistencyCheckDuringRecovery extends ReplicatedLogtestcase{
    
        final String tableName = "table1";
    final String tableSpaceName = "t2";

    @Test
    public void consistencyCheckReplicaTest() throws Exception {
        final AtomicInteger callCount = new AtomicInteger();
        SystemInstrumentation.addListener(new SystemInstrumentation.SingleInstrumentationPointListener("createChecksum") {
            @Override
            public void acceptSingle(Object... args) throws Exception {
                callCount.incrementAndGet();
            }
        });
        try (DBManager manager1 = startDBManager("node1")) {
            manager1.executeStatement(new CreateTableSpaceStatement(tableSpaceName, new HashSet<>(Arrays.asList("node1", "node2")), "node1", 2, 0, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager1.waitForTablespace(tableSpaceName, 10000, true);
            try (DBManager manager2 = startDBManager("node2")) {
                manager2.waitForTablespace(tableSpaceName, 10000, false);
                Table table = Table
                    .builder()
                    .tablespace(tableSpaceName)
                    .name(tableName)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.BOOLEAN)
                    .primaryKey("id")
                    .build();

                CreateTableStatement st = new CreateTableStatement(table);
                manager1.executeStatement(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                manager1.waitForTable(tableSpaceName, tableName, 10000, true);
                execute(manager1, "INSERT INTO t2.table1 (id,name) values (?,?)", Arrays.asList("1", true));
                execute(manager1, "INSERT INTO t2.table1 (id,name) values (?,?)", Arrays.asList("2", false));
                manager2.waitForTable(tableSpaceName, tableName, 10000, false);

                TableConsistencyCheckStatement statement = new TableConsistencyCheckStatement("table1", "t2");
                DataConsistencyStatementResult result = manager1.createTableCheckSum(statement, null);
                assertTrue("Consistency check replicated test ", result.getOk());

                //The follower is always back 1, I make an entry to make him apply consistency log
                execute(manager1, "INSERT INTO t2.table1 (id,name) values (?,?)", Arrays.asList("3", false));
            }
        }

        try (DBManager manager1 = startDBManager("node1")) {
            //restart leader node
            manager1.waitForTablespace("t2", 10000);
        }
        
        try (DBManager manager1 = startDBManager("node2")) {
            //restart follower  node
            manager1.waitForTablespace("t2", 1000);
        }
        
        
                //Expected 2 call for createChecksum (node1 and node2)
        assertEquals(3, callCount.get());
    }
    
}
