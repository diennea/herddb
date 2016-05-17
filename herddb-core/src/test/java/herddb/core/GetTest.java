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

import herddb.model.GetResult;
import herddb.model.commands.InsertStatement;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.predicates.RawValueEquals;
import herddb.utils.Bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class GetTest extends BaseTestcase {

    @Test
    public void test() throws Exception {

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        {
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, Bytes.from_string("key1"), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
            assertEquals(result.getRecord().key, Bytes.from_string("key1"));
            assertEquals(result.getRecord().value, Bytes.from_int(0));
        }

        {
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, Bytes.from_string("key1"), new RawValueEquals(Bytes.from_int(0))), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
            assertEquals(result.getRecord().key, Bytes.from_string("key1"));
            assertEquals(result.getRecord().value, Bytes.from_int(0));
        }
        {
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, Bytes.from_string("key1"), new RawValueEquals(Bytes.from_int(2))), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result.found());
        }
        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, Bytes.from_string("key1"), null);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, Bytes.from_string("key1"), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertFalse(result.found());
        }

    }
}
