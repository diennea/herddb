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
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.Test;

/**
 * Basic functionality
 *
 * @author enrico.olivelli
 */
public class UnderreplicationTest extends ReplicatedLogtestcase {

    @Test
    public void test() throws Exception {
        // we want replication factor = 2, we need two bookies
        testEnv.startNewBookie();

        final String tableSpaceName = "t2";
        try (DBManager manager1 = startDBManager("node1")) {
            // setting expectedReplicaCount = 2
            // the system will automatically add node2 as soon as it is available
            manager1.executeStatement(new CreateTableSpaceStatement(tableSpaceName, new HashSet<>(Arrays.asList("node1")), "node1", 2, 0, 0), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(manager1.waitForTablespace(tableSpaceName, 10000, true));
            try (DBManager manager2 = startDBManager("node2")) {
                assertTrue(manager2.waitForTablespace(tableSpaceName, 10000, false));
            }
        }
    }
}
