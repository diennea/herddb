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

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.utils.Bytes;

/**
 * Basic functionality
 *
 * @author enrico.olivelli
 */
public class SimpleReplicationTest extends ReplicatedLogtestcase {

    @Test
    public void test() throws Exception {
        final String tableName = "table1";
        final String tableSpaceName = "t2";
        try (DBManager manager1 = startDBManager("node1")) {

            manager1.executeStatement(new CreateTableSpaceStatement(tableSpaceName, new HashSet<>(Arrays.asList("node1", "node2")), "node1", 2, 0, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(manager1.waitForTablespace(tableSpaceName, 10000, true));
            try (DBManager manager2 = startDBManager("node2")) {
                assertTrue(manager2.waitForTablespace(tableSpaceName, 10000, false));

                manager1.executeStatement(new CreateTableStatement(Table.builder().tablespace(tableSpaceName).name(tableName).primaryKey("key").column("key", ColumnTypes.STRING).build()), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(manager1.waitForTable(tableSpaceName, tableName, 10000, true));

                manager1.executeStatement(new InsertStatement(tableSpaceName, tableName, new Record(Bytes.from_string("one"), Bytes.from_string("two"))), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                // write a second entry on the ledger, to speed up the ack from the bookie
                manager1.executeStatement(new InsertStatement(tableSpaceName, tableName, new Record(Bytes.from_string("second"), Bytes.from_string("two"))), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                assertTrue(manager1.get(new GetStatement(tableSpaceName, tableName, Bytes.from_string("one"), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());

                assertTrue(manager2.waitForTable(tableSpaceName, tableName, 10000, false));
                
                manager2.setErrorIfNotLeader(false);

                for (int i = 0; i < 100; i++) {
                    boolean ok = manager2.get(new GetStatement(tableSpaceName, tableName, Bytes.from_string("one"), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found();
                    if (ok) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertTrue(manager2.get(new GetStatement(tableSpaceName, tableName, Bytes.from_string("one"), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            }
        }
    }
}
