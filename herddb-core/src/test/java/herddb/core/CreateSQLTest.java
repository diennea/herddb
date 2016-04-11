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

import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.utils.Bytes;
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class CreateSQLTest {

    @Test
    public void createTable1() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            CreateTableStatement st2 = (CreateTableStatement) manager.getTranslator().translate("CREATE TABLE tblspace1.tsql (n1 string primary key)", Collections.emptyList());
            manager.executeStatement(st2);

            {
                Record record = new Record(Bytes.from_string("key1"), Bytes.from_int(0));
                InsertStatement st = new InsertStatement("tblspace1", "tsql", record);
                assertEquals(1, manager.executeUpdate(st).getUpdateCount());
            }

            {
                GetResult result = manager.get(new GetStatement("tblspace1", "tsql", Bytes.from_string("key1"), null));
                assertTrue(result.found());
                assertEquals(result.getRecord().key, Bytes.from_string("key1"));
                assertEquals(result.getRecord().value, Bytes.from_int(0));
            }
        }

    }
}
