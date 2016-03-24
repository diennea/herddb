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

import herddb.log.CommitLogManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.MetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.storage.DataStorageManager;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;

/**
 *
 * @author enrico.olivelli
 */
public class BaseTestcase {

    protected String nodeId = "localhost";
    protected String tableSpace = TableSpace.DEFAULT;
    protected Table table;
    protected String tableName;
    protected DBManager manager;
    protected DataStorageManager dataStorageManager;
    protected MetadataStorageManager metadataStorageManager;
    protected CommitLogManager commitLogManager;

    protected CommitLogManager makeCommitLogManager() {
        return new MemoryCommitLogManager();
    }

    protected DataStorageManager makeDataStorageManager() {
        return new MemoryDataStorageManager();
    }

    protected MetadataStorageManager makeMetadataStorageManager() {
        return new MemoryMetadataStorageManager();
    }

    @Before
    public void setup() throws Exception {
        metadataStorageManager = makeMetadataStorageManager();
        commitLogManager = makeCommitLogManager();
        dataStorageManager = makeDataStorageManager();
        System.setErr(System.out);
        manager = new DBManager("localhost", metadataStorageManager, dataStorageManager, commitLogManager);
        manager.start();
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(TableSpace.DEFAULT, Collections.singleton(nodeId), nodeId);
        manager.executeStatement(st1);
        tableName = "t1";
        table = Table
                .builder()
                .name(tableName)
                .column("id", ColumnTypes.STRING)
                .column("name", ColumnTypes.STRING)
                .primaryKey("id")
                .build();

        CreateTableStatement st2 = new CreateTableStatement(table);
        manager.executeStatement(st2);
    }

    @After
    public void teardown() throws Exception {
        manager.close();
        manager = null;
    }
}
