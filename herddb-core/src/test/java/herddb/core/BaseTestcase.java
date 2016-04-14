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
import static org.junit.Assert.assertTrue;
import org.junit.Before;

/**
 *
 * @author enrico.olivelli
 */
public class BaseTestcase {

    protected String nodeId = "localhost";
    protected String tableSpace = "tblspace1";
    protected Table table;
    protected String tableName;
    protected DBManager manager;
    protected DataStorageManager dataStorageManager;
    protected MetadataStorageManager metadataStorageManager;
    protected CommitLogManager commitLogManager;

    protected void beforeSetup() throws Exception {
    }

    protected void afterTeardown() throws Exception {
    }

    protected CommitLogManager makeCommitLogManager() throws Exception {
        return new MemoryCommitLogManager();
    }

    protected DataStorageManager makeDataStorageManager() throws Exception {
        return new MemoryDataStorageManager();
    }

    protected MetadataStorageManager makeMetadataStorageManager() throws Exception {
        return new MemoryMetadataStorageManager();
    }

    @Before
    public void setup() throws Exception {
        beforeSetup();
        metadataStorageManager = makeMetadataStorageManager();
        commitLogManager = makeCommitLogManager();
        dataStorageManager = makeDataStorageManager();
        System.setErr(System.out);
        manager = new DBManager("localhost", metadataStorageManager, dataStorageManager, commitLogManager);
        manager.start();
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId);
        manager.executeStatement(st1);
        assertTrue(manager.waitForTablespace(tableSpace, 10000));
        tableName = "t1";
        table = Table
                .builder()
                .tablespace("tblspace1")
                .name(tableName)
                .tablespace(tableSpace)
                .column("id", ColumnTypes.STRING)
                .column("name", ColumnTypes.STRING)
                .column("ts1", ColumnTypes.TIMESTAMP)
                .primaryKey("id")
                .build();

        CreateTableStatement st2 = new CreateTableStatement(table);
        manager.executeStatement(st2);
    }

    @After
    public void teardown() throws Exception {
        if (manager != null) {
            manager.close();
        }
        manager = null;

        table = null;
        tableName = null;

        dataStorageManager = null;
        metadataStorageManager = null;
        commitLogManager = null;
        afterTeardown();
    }
}
