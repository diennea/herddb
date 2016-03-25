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

import herddb.file.FileCommitLog;
import herddb.file.SimpleFileDataStorageManager;
import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryDataStorageManager.Page;
import herddb.model.GetResult;
import herddb.model.commands.InsertStatement;
import herddb.model.Record;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.UpdateStatement;
import herddb.storage.DataStorageManager;
import herddb.utils.Bytes;
import java.io.IOException;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class FlushFileTest extends BaseTestcase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Override
    protected CommitLogManager makeCommitLogManager() {
        return new CommitLogManager() {
            @Override
            public CommitLog createCommitLog(String tableSpace) {
                try {
                    return new FileCommitLog(folder.newFolder(tableSpace).toPath(), 1024 * 1024);
                } catch (IOException err) {
                    throw new RuntimeException(err);
                }
            }
        };
    }

    @Override
    protected DataStorageManager makeDataStorageManager() {
        try {
            return new SimpleFileDataStorageManager(folder.newFolder("data").toPath());
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    @Test
    public void test() throws Exception {

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("0"));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        assertNull(dataStorageManager.loadPage(tableName, 1L));
        assertEquals(0, dataStorageManager.getActualNumberOfPages(tableName));
        manager.flush();
        assertNotNull(dataStorageManager.loadPage(tableName, 1L));
        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableName));

        {
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, Bytes.from_string("key1"), null));
            assertTrue(result.found());
        }

        manager.flush();
        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableName));

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("5"));
            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, null);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }

        // a new page must be allocated
        manager.flush();
        assertEquals(2, dataStorageManager.getActualNumberOfPages(tableName));

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("6"));
            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, null);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("7"));
            UpdateStatement st = new UpdateStatement(tableSpace, tableName, record, null);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        // only a new page must be allocated, not two more
        manager.flush();
        assertEquals(3, dataStorageManager.getActualNumberOfPages(tableName));

        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, Bytes.from_string("key1"), null);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
            GetResult result = manager.get(new GetStatement(tableSpace, tableName, Bytes.from_string("key1"), null));
            assertFalse(result.found());
        }

        // a delete does not trigger new pages in this case
        manager.flush();
        assertEquals(3, dataStorageManager.getActualNumberOfPages(tableName));

        {
            assertEquals(1, manager.executeUpdate(new InsertStatement(tableSpace, tableName, new Record(Bytes.from_string("key2"), Bytes.from_string("50")))).getUpdateCount());
            assertEquals(1, manager.executeUpdate(new InsertStatement(tableSpace, tableName, new Record(Bytes.from_string("key3"), Bytes.from_string("60")))).getUpdateCount());
        }

        manager.flush();
        assertEquals(4, dataStorageManager.getActualNumberOfPages(tableName));
        {
            DeleteStatement st = new DeleteStatement(tableSpace, tableName, Bytes.from_string("key2"), null);
            assertEquals(1, manager.executeUpdate(st).getUpdateCount());
        }
        // a new page, containg the key3 record is needed
        manager.flush();

        MemoryDataStorageManager mem = (MemoryDataStorageManager) dataStorageManager;
        for (long pageId = 1; pageId <= dataStorageManager.getActualNumberOfPages(tableName); pageId++) {
            Page page = mem.getPage(tableName, pageId);
            List<Record> records = page.getRecords();
            System.out.println("PAGE #" + pageId + " records :" + records + " seq " + page.getSequenceNumber());
        }

        assertEquals(5, dataStorageManager.getActualNumberOfPages(tableName));

    }
}
