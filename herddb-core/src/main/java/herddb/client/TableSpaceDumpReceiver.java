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

package herddb.client;

import herddb.backup.DumpedLogEntry;
import herddb.backup.DumpedTableMetadata;
import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.model.Transaction;
import herddb.storage.DataStorageManagerException;
import java.util.List;
import java.util.Map;

/**
 * Receives a full dump of a TableSpace
 *
 * @author enrico.olivelli
 */
public class TableSpaceDumpReceiver {

    public void start(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException {
    }

    public void beginTable(DumpedTableMetadata table, Map<String, Object> stats) throws DataStorageManagerException {
    }

    public void receiveTableDataChunk(List<Record> record) throws DataStorageManagerException {
    }

    public void endTable() throws DataStorageManagerException {
    }

    public void finish(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException {
    }

    public void onError(Throwable error) throws DataStorageManagerException {
    }

    public void receiveTransactionLogChunk(List<DumpedLogEntry> entries) throws DataStorageManagerException {
    }

    public void receiveTransactionsAtDump(List<Transaction> transactions) throws DataStorageManagerException {
    }
}
