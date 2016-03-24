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

import herddb.model.InsertStatement;
import herddb.model.Record;
import herddb.model.UpdateStatement;
import herddb.model.predicates.RawValueEquals;
import herddb.utils.Bytes;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class UpdateTest extends BaseTestcase {

    @Test
    public void createTable1() throws Exception {
        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_int(0));

            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, null);
            assertEquals(0, manager.executeStatement(st).getUpdateCount());
        }

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_int(0));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeStatement(st).getUpdateCount());
        }

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_int(1));
            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, null);
            assertEquals(1, manager.executeStatement(st).getUpdateCount());
        }

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_int(1));
            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, new RawValueEquals(Bytes.from_int(2)));
            assertEquals(0, manager.executeStatement(st).getUpdateCount());
        }
        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_int(5));
            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, new RawValueEquals(Bytes.from_int(1)));
            assertEquals(1, manager.executeStatement(st).getUpdateCount());
        }

    }
}
