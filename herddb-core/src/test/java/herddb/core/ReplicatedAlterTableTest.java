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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.utils.Bytes;
import java.util.Collections;
import static org.junit.Assert.assertNull;

/**
 * Basic functionality
 *
 * @author enrico.olivelli
 */
public class ReplicatedAlterTableTest extends ReplicatedLogtestcase {

    @Test
    public void test() throws Exception {
        final String tableName = "tsql";
        final String tableName2 = "tsql2";
        final String tableSpaceName = "tblspace1";
        try (DBManager manager1 = startDBManager("node1")) {

            manager1.executeStatement(new CreateTableSpaceStatement(tableSpaceName, new HashSet<>(Arrays.asList("node1", "node2")), "node1", 2, 0, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(manager1.waitForTablespace(tableSpaceName, 10000, true));
            try (DBManager manager2 = startDBManager("node2")) {
                assertTrue(manager2.waitForTablespace(tableSpaceName, 10000, false));

                execute(manager1, "CREATE TABLE " + tableSpaceName + "." + tableName + " (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
                execute(manager1, "INSERT INTO " + tableSpaceName + "." + tableName + " (k1,n1,s1) values('one',1, 'b')", Collections.emptyList());
                execute(manager1, "INSERT INTO " + tableSpaceName + "." + tableName + " (k1,n1,s1) values('two',1, 'b')", Collections.emptyList());
                assertTrue(manager1.get(new GetStatement(tableSpaceName, tableName, Bytes.from_string("one"), null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
                assertTrue(manager1.get(new GetStatement(tableSpaceName, tableName, Bytes.from_string("two"), null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());

                assertTrue(manager2.waitForTable(tableSpaceName, tableName, 10000, false));
                manager2.setErrorIfNotLeader(false);
                for (int i = 0; i < 100; i++) {
                    boolean ok = manager2.get(new GetStatement(tableSpaceName, tableName, Bytes.from_string("one"), null, false),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found();
                    System.out.println("ok:" + ok);
                    if (ok) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertTrue(manager2.get(new GetStatement(tableSpaceName, tableName, Bytes.from_string("one"), null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());

                execute(manager1, "EXECUTE RENAMETABLE '" + tableSpaceName + "','" + tableName + "','" + tableName2 + "'", Collections.emptyList());
                execute(manager1, "INSERT INTO " + tableSpaceName + "." + tableName2 + " (k1,n1,s1) values('three',1, 'b')", Collections.emptyList());
                assertNull(manager1.getTableSpaceManager(tableSpaceName).getTableManager(tableName));

                assertTrue(manager1.get(new GetStatement(tableSpaceName, tableName2, Bytes.from_string("one"), null, false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());

                assertTrue(manager2.waitForTable(tableSpaceName, tableName2, 10000, false));
                assertNull(manager2.getTableSpaceManager(tableSpaceName).getTableManager(tableName));
                manager2.setErrorIfNotLeader(false);
                for (int i = 0; i < 100; i++) {
                    boolean ok = manager2.get(new GetStatement(tableSpaceName, tableName2, Bytes.from_string("one"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found();
                    if (ok) {
                        break;
                    }
                    Thread.sleep(100);
                }
                assertTrue(manager2.get(new GetStatement(tableSpaceName, tableName2, Bytes.from_string("one"), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
            }
        }

        // reboot
        try (DBManager manager1 = startDBManager("node1")) {
            assertTrue(manager1.waitForTable(tableSpaceName, tableName2, 10000, false));
            assertNull(manager1.getTableSpaceManager(tableSpaceName).getTableManager(tableName));
            assertTrue(manager1.get(new GetStatement(tableSpaceName, tableName2, Bytes.from_string("one"), null, false),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).found());
        }

    }

}
