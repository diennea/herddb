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
import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import static org.junit.Assert.assertEquals;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.TransactionResult;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.junit.Test;

/**
 * Tests about statement rewriting for EXECUTE syntax
 *
 * @author enrico.olivelli
 */
public class BetterExecuteSyntaxTest {
        List<LogEntry> entries = new CopyOnWriteArrayList<>();

        class MyMemoryCommitLogManager extends CommitLogManager {
                 @Override
                 public CommitLog createCommitLog(String tableSpace, String tablespaceName, String localNodeId) {
                     return new CommitLog() {
                         AtomicLong offset = new AtomicLong(-1);
                         @Override
                         public CommitLogResult log(LogEntry entry, boolean synch) throws LogNotAvailableException {
                             if (isHasListeners()) {
                                 synch = true;
                             }
                             LogSequenceNumber logPos = new LogSequenceNumber(1, offset.incrementAndGet());
                             notifyListeners(logPos, entry);
                             entries.add(entry);
                             System.out.println("LOG " + entry);
                             return new CommitLogResult(logPos, !synch, synch);
                         }
                         @Override
                         public LogSequenceNumber getLastSequenceNumber() {
                             return new LogSequenceNumber(1, offset.get());
                         }
                         private volatile boolean closed;
                         @Override
                         public void close() throws LogNotAvailableException {
                             closed = true;
                         }
                         @Override
                         public boolean isFailed() {
                             return false;
                         }
                         @Override
                         public boolean isClosed() {
                             return closed;
                         }        
                         @Override
                         public void recovery(
                                 LogSequenceNumber snapshotSequenceNumber,
                                 BiConsumer<LogSequenceNumber, LogEntry> consumer, boolean fencing
                         ) throws LogNotAvailableException {
                         }
                         @Override
                         public void dropOldLedgers(LogSequenceNumber lastCheckPointSequenceNumber) throws LogNotAvailableException {
                         }

                         @Override
                         public void startWriting() throws LogNotAvailableException {
                         }
                         @Override
                         public void clear() throws LogNotAvailableException {
                         }



                     };
                 }
             }
    @Test
    public void betterSyntax() throws Exception {

        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
                manager.start();
                execute(manager, "CREATE TABLESPACE 'consistence'", Collections.emptyList());
                manager.waitForTablespace("tblspace1", 10000);
                execute(manager, "CREATE TABLE consistence.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
                execute(manager, "CREATE TABLE consistence.tsql1 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
                execute(manager, "CREATE TABLE consistence.tsql2 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
                    
                for(int i=0; i<10; i++){
                    System.out.println("insert number " + i);
                    execute(manager, "INSERT INTO consistence.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));                
                    execute(manager, "INSERT INTO consistence.tsql1 (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));                
                    execute(manager, "INSERT INTO consistence.tsql2 (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
                }
                    try (DataScanner scan = TestUtils.scan(manager, "SELECT COUNT(*) FROM consistence.tsql", Collections.emptyList())) {                 
                }
                //execute(manager, "TABLECONSISTENCYCHECK consistence.tsql", Collections.emptyList());
                execute(manager, "TABLECONSISTENCYCHECK consistence.tsql1", Collections.emptyList());
                execute(manager,"TABLECONSISTENCYCHECK consistence.tsql2", Collections.emptyList());          
                execute(manager,"TABLESPACECONSISTENCYCHECK consistence", Collections.emptyList()); 
                execute(manager,"TABLESPACECONSISTENCYCHECK 'consistence'", Collections.emptyList()); 

        }
    }
}



