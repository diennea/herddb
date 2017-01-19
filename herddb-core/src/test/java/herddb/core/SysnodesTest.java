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

import static herddb.core.TestUtils.scan;
import herddb.file.FileMetadataStorageManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.TableSpace;
import herddb.model.Tuple;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 *
 *
 * @author enrico.olivelli
 */
public class SysnodesTest {

    @Test
    public void listNodesTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId, new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null, null);) {
            manager.start();
            assertTrue(manager.waitForTablespace(TableSpace.DEFAULT, 10000));

            try (DataScanner scan = scan(manager, "SELECT * FROM SYSNODES", Collections.emptyList());) {
                List<Tuple> tuples = scan.consume();
                assertEquals(1, tuples.size());
                for (Tuple t : tuples) {
                    System.out.println("node: " + t.toMap());
                    assertNotNull(t.get("nodeid"));
                    assertNotNull(t.get("address"));
                }
            }
        }
    }

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void listNodesFileManagerTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId, new FileMetadataStorageManager(tmpFolder.getRoot().toPath()),
            new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null, null);) {
            manager.start();
            assertTrue(manager.waitForTablespace(TableSpace.DEFAULT, 10000));

            try (DataScanner scan = scan(manager, "SELECT * FROM SYSNODES", Collections.emptyList());) {
                List<Tuple> tuples = scan.consume();
                assertEquals(1, tuples.size());
                for (Tuple t : tuples) {
                    System.out.println("node: " + t.toMap());
                    assertNotNull(t.get("nodeid"));
                    assertNotNull(t.get("address"));
                }
            }
        }
    }

}
