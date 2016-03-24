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
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import java.util.Collections;
import org.junit.Test;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class CreateTableTest {

    @Test
    public void createTable1() throws Exception {
        String nodeId = "localhost";
        DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());
        manager.boot();
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(TableSpace.DEFAULT, Collections.singleton(nodeId), nodeId);
        manager.executeStatement(st1);

        Table table = Table
                .builder()
                .name("t1")
                .column("id", ColumnTypes.STRING)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

        CreateTableStatement st2 = new CreateTableStatement(table);
        manager.executeStatement(st2);

    }
}
