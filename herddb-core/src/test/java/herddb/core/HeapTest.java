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

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import herddb.codec.DataAccessorForFullRecord;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * Tests on table creation. An heap is a table without primary key, that is that
 * the full record is the PK
 *
 * @author enrico.olivelli
 */
public class HeapTest {

    @Test
    public void testHeapTable() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string, n1 int,s1 string)", Collections.emptyList());

            assertEquals(1, executeUpdate(manager, "INSERT INTO tblspace1.tsql(k1,n1,s1) values(?,?,?)", Arrays.asList("mykey", Integer.valueOf(1234), "aa")).getUpdateCount());

            assertEquals(1, scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList()).consumeAndClose().size());

            try (DataScanner dataScanner = scan(manager, "SELECT * FROM tblspace1.tsql", Collections.emptyList())) {

                // this is mostly what is done while serializing the result set to the network client
                String[] columns = dataScanner.getFieldNames();
                List<DataAccessor> records = dataScanner.consume();
                for (DataAccessor da : records) {
                    assertThat(da, instanceOf(DataAccessorForFullRecord.class));
                }
                assertEquals(1, records.size());
//                TuplesList tuplesList = new TuplesList(columns, records);
//                Message msg = Message.RESULTSET_CHUNK("xxx", tuplesList, true, dataScanner.transactionId);
//                msg.assignMessageId();
//                ByteBuf buffer = Unpooled.buffer();
//                MessageUtils.encodeMessage(buffer, msg);
//                Message msgDecoded = MessageUtils.decodeMessage(buffer);
//
//                // remote the two different forms of the same datum
//                RecordsBatch batchReceived = (RecordsBatch) msgDecoded.parameters.remove("data");
//                TuplesList batchSent = (TuplesList) msg.parameters.remove("data");
//
//                assertEquals(msgDecoded.messageId, msg.messageId);
//
//                assertTrue(batchReceived.hasNext());
//                DataAccessor recordReceived = batchReceived.next();
//                DataAccessor recordSent = batchSent.tuples.get(0);
//                assertFalse(batchReceived.hasNext());
//                batchReceived.release();
//                assertEquals(recordReceived, recordSent);
            }

        }
    }

    @Test
    public void insertWithoutValuesAndPrimaryKeyIsNotTheFirstColumn() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE `ao_21d670_whitelist_rules` (\n"
                    + "  `ALLOWINBOUND` tinyint(1) DEFAULT NULL,\n"
                    + "  `EXPRESSION` longtext COLLATE utf8_bin NOT NULL,\n"
                    + "  `ID` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "  `TYPE` varchar(255) COLLATE utf8_bin NOT NULL,\n"
                    + "  PRIMARY KEY (`ID`)\n"
                    + ") ", Collections.emptyList());

            execute(manager, "INSERT INTO `ao_21d670_whitelist_rules` VALUES (?,?,?,?)\n"
                    + "", Arrays.asList(0, "http://www.xxx.com/*", 3, "WILDCARD_EXPRESSION"));

        }
    }

}
