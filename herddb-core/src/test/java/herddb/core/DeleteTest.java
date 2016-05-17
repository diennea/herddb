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

import herddb.codec.RecordSerializer;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.PrimaryKeyIndexSeekPredicate;
import herddb.model.ScanResult;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionResult;
import herddb.model.Tuple;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class DeleteTest {

    private DMLStatementExecutionResult executeUpdate(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters, true, true);
        return (DMLStatementExecutionResult) manager.executePlan(translated.plan, translated.context);
    }

    private StatementExecutionResult execute(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters, true, true);
        return manager.executePlan(translated.plan, translated.context);
    }

    private DataScanner scan(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters, true, true);
        return ((ScanResult) manager.executePlan(translated.plan, translated.context)).dataScanner;
    }

    @Test
    public void deleteMulticolumnPKTest() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager());) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1);
            manager.executeStatement(st1);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (K1 string ,s1 string,n1 int, primary key(k1,s1))", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey2", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey3", "a", Integer.valueOf(1234))).getUpdateCount());
            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,s1,n1) values(?,?,?)", Arrays.asList("mykey4", "a", Integer.valueOf(1234))).getUpdateCount());

            assertEquals(4, executeUpdate(manager, "DELETE FROM tblspace1.tsql WHERE N1=1234", Collections.emptyList()).getUpdateCount());

            assertEquals(0, scan(manager, "SELECT * FROM tblspace1.tsql WHERE N1=1234", Collections.emptyList()).consume().size());

        }
    }

}
