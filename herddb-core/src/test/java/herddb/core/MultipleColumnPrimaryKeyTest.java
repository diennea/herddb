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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import herddb.codec.RecordSerializer;
import herddb.model.ColumnTypes;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;

/**
 *
 *
 * @author enrico.olivelli
 */
public class MultipleColumnPrimaryKeyTest extends BaseTestcase {

    @Override
    protected Table createTable() {
        return Table
            .builder()
            .name(tableName)
            .tablespace(tableSpace)
            .column("id", ColumnTypes.STRING)
            .column("name", ColumnTypes.STRING)
            .column("number", ColumnTypes.INTEGER)
            .primaryKey("id")
            .primaryKey("name")
            .build();
    }

    @Test
    public void test() throws Exception {

        {
            Record record = RecordSerializer.makeRecord(table, "id", "r1", "name", "k1");
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        {
            Map<String, Object> key = new HashMap<>();
            key.put("id", "r1");
            key.put("name", "k1");
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, RecordSerializer.serializePrimaryKey(key, table, table.primaryKey), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(result.found());
        }

        {
            Map<String, Object> key = new HashMap<>();
            key.put("id", "r1");
            key.put("name", "k2");
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, RecordSerializer.serializePrimaryKey(key, table, table.primaryKey), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            assertTrue(!result.found());
        }

        {
            Map<String, Object> key = new HashMap<>();
            key.put("id", "r1");
            key.put("name", "k1");
            {
                GetResult result = manager.get(new GetStatement(tableSpace, tableName, RecordSerializer.serializePrimaryKey(key, table, table.primaryKey), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertTrue(result.found());
            }
            {
                DeleteStatement st = new DeleteStatement(tableSpace, tableName, RecordSerializer.serializePrimaryKey(key, table, table.primaryKey), null);
                assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
            }
            {
                GetResult result = manager.get(new GetStatement(tableSpace, tableName, RecordSerializer.serializePrimaryKey(key, table, table.primaryKey), null, false), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                assertFalse(result.found());
            }
        }

    }
}
