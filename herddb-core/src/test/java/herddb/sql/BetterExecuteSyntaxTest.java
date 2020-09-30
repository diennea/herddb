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

package herddb.sql;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
<<<<<<< HEAD:herddb-core/src/test/java/herddb/sql/BetterExecuteSyntaxTest.java
import herddb.core.DBManager;
import herddb.core.TestUtils;
=======
import static org.junit.Assert.assertTrue;
>>>>>>> fix flaky tests:herddb-core/src/test/java/herddb/core/BetterExecuteSyntaxTest.java
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.TransactionResult;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

/**
 * Tests about statement rewriting for EXECUTE syntax
 *
 * @author enrico.olivelli
 */
public class BetterExecuteSyntaxTest {

    @Test
    public void betterSyntax() throws Exception {

        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            assertTrue(manager.waitForBootOfLocalTablespaces(10000));
            execute(manager, "CREATE TABLESPACE 'tblspace1'", Collections.emptyList());
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "ALTER TABLESPACE 'TBLSPACE1','expectedreplicacount:2'", Collections.emptyList());
            long tx = ((TransactionResult) execute(manager, "BEGIN TRANSACTION 'tblspace1'", Collections.emptyList())).getTransactionId();
            execute(manager, "COMMIT TRANSACTION 'tblspace1'," + tx, Collections.emptyList());

            long tx2 = ((TransactionResult) execute(manager, "BEGIN TRANSACTION 'tblspace1'", Collections.emptyList())).getTransactionId();
            execute(manager, "ROLLBACK TRANSACTION 'tblspace1'," + tx2, Collections.emptyList());

            execute(manager, "DROP TABLESPACE 'tblspace1'", Collections.emptyList());

            try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM systablespaces WHERE tablespace_name=?", Arrays.asList("tblspace1"))) {
                DataAccessor first = scan.consume().get(0);
                Number count = (Number) first.get(first.getFieldNames()[0]);
                assertEquals(0, count.intValue());
            }

        }
    }
}
