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
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.ScanResult;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TableSpace;
import herddb.model.TableSpaceAlreadyExistsException;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.sql.TranslatedQuery;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests on table creation
 *
 * @author enrico.olivelli
 */
public class AlterTablespaceSQLTest {

    private DMLStatementExecutionResult executeUpdate(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters, true, true);
        return (DMLStatementExecutionResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION);
    }

    private StatementExecutionResult execute(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters, true, true);
        return manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION);
    }

    private DataScanner scan(DBManager manager, String query, List<Object> parameters) throws StatementExecutionException {
        TranslatedQuery translated = manager.getTranslator().translate(query, parameters, true, true);
        return ((ScanResult) manager.executePlan(translated.plan, translated.context, TransactionContext.NO_TRANSACTION)).dataScanner;
    }

    @Test
    public void createAlterTableSpace() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId, new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null);) {
            manager.start();
            assertTrue(manager.waitForTablespace(TableSpace.DEFAULT, 10000));
            execute(manager, "EXECUTE CREATETABLESPACE 'ttt'", Collections.emptyList());
            execute(manager, "EXECUTE CREATETABLESPACE 'ttt2','" + nodeId + "'", Collections.emptyList());
            try {
                execute(manager, "EXECUTE CREATETABLESPACE 'ttt2','othernode'", Collections.emptyList());
                fail();
            } catch (TableSpaceAlreadyExistsException err) {
            }
            execute(manager, "EXECUTE CREATETABLESPACE 'ttt3','othernode'", Collections.emptyList());

            execute(manager, "EXECUTE ALTERTABLESPACE 'ttt3','replica','" + nodeId + ",othernode'", Collections.emptyList());
            execute(manager, "EXECUTE ALTERTABLESPACE 'ttt3','leader','othernode'", Collections.emptyList());
            execute(manager, "EXECUTE ALTERTABLESPACE 'ttt3','expectedReplicaCount','12'", Collections.emptyList());
            TableSpace ttt3 = manager.getMetadataStorageManager().describeTableSpace("ttt3");
            assertEquals("othernode", ttt3.leaderId);
            assertEquals(12, ttt3.expectedReplicaCount);
            assertTrue(ttt3.replicas.contains("othernode"));
            assertTrue(ttt3.replicas.contains(nodeId));

            try (DataScanner scan = scan(manager, "SELECT * FROM SYSTABLESPACES", Collections.emptyList());) {
                List<Tuple> tuples = scan.consume();
                assertEquals(4, tuples.size());
                for (Tuple t : tuples) {
                    System.out.println("tablespace: " + t.toMap());
                    assertNotNull(t.get("expectedreplicacount"));
                    assertNotNull(t.get("tablespace_name"));
                    assertNotNull(t.get("replica"));
                    assertNotNull(t.get("leader"));
                }
            }
            try (DataScanner scan = scan(manager, "SELECT expectedreplicacount FROM SYSTABLESPACES where tablespace_name='ttt3'", Collections.emptyList());) {
                List<Tuple> tuples = scan.consume();
                assertEquals(1, tuples.size());
                for (Tuple t : tuples) {
                     System.out.println("tablespace: " + t.toMap());
                    assertEquals(12, t.get("expectedreplicacount"));
                }
            }
        }
    }

}
