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
import herddb.file.FileDataStorageManager;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.InsertStatement;
import herddb.storage.DataStorageManager;
import herddb.utils.Bytes;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author enrico.olivelli
 */
public class DropOldPagesTest extends BaseTestcase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Override
    protected DataStorageManager makeDataStorageManager() {
        try {
            return new FileDataStorageManager(folder.newFolder("data").toPath());
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    @Test
    public void test() throws Exception {

        FileDataStorageManager fileDataStorageManager = (FileDataStorageManager) dataStorageManager;

        {
            Record record = new Record(Bytes.from_string("key1"), Bytes.from_string("0"));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }

        String tableUuid = manager.getTableSpaceManager(tableSpace).getTableManager(tableName).getTable().uuid;
        assertEquals(0, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));
        assertEquals(0, fileDataStorageManager.getTablePageFiles(tableSpaceUUID, tableUuid).size());

        manager.checkpoint();


        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));
        assertEquals(1, fileDataStorageManager.getTablePageFiles(tableSpaceUUID, tableUuid).size());
        {
            Record record = new Record(Bytes.from_string("key2"), Bytes.from_string("0"));
            InsertStatement st = new InsertStatement(tableSpace, tableName, record);
            assertEquals(1, manager.executeUpdate(st, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());

            assertEquals(1, manager.executeUpdate(new DeleteStatement(tableSpace, tableName, Bytes.from_string("key1"), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION).getUpdateCount());
        }
        manager.checkpoint();

        assertEquals(1, dataStorageManager.getActualNumberOfPages(tableSpaceUUID, tableUuid));
        assertEquals(1, fileDataStorageManager.getTablePageFiles(tableSpaceUUID, tableUuid).size());

    }

}
