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
package herddb.backup;

import herddb.client.TableSpaceDumpReceiver;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.storage.DataStorageManagerException;
import herddb.utils.ExtendedDataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import herddb.utils.Holder;

/**
 * Writes a full tabledump to file
 *
 * @author enrico.olivelli
 */
class TableSpaceDumpFileWriter extends TableSpaceDumpReceiver {

    private final ProgressListener listener;
    private final Holder<Throwable> errorHolder;
    private final CountDownLatch waiter;
    private final String schema;
    private final ExtendedDataOutputStream out;

    public TableSpaceDumpFileWriter(ProgressListener listener, Holder<Throwable> errorHolder, CountDownLatch waiter, String schema, ExtendedDataOutputStream out) {
        this.listener = listener;
        this.errorHolder = errorHolder;
        this.waiter = waiter;
        this.schema = schema;
        this.out = out;
    }
    long tableRecordsCount;
    String currentTable;

    @Override
    public void onError(Throwable error) throws DataStorageManagerException {
        listener.log("error", "Fatal error: " + error, Collections.singletonMap("error", error));
        errorHolder.value = error;
        waiter.countDown();
    }

    @Override
    public void finish(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException {
        listener.log("finish", "Dump finished for tablespace " + schema + " at " + logSequenceNumber, Collections.singletonMap("tablespace", schema));
        try {
            out.writeUTF(BackupFileConstants.ENTRY_TYPE_END);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        waiter.countDown();
    }

    @Override
    public void endTable() throws DataStorageManagerException {
        listener.log("endTable", "endTable " + currentTable + ", records " + tableRecordsCount, Collections.singletonMap("table", currentTable));
        currentTable = null;
        try {
            out.writeVInt(Integer.MIN_VALUE); // EndOfTableMarker
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void receiveTableDataChunk(List<Record> record) throws DataStorageManagerException {
        try {
            out.writeVInt(record.size());
            for (Record r : record) {
                out.writeArray(r.key.data);
                out.writeArray(r.value.data);
            }
            tableRecordsCount += record.size();
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        listener.log("receivedata", "table " + currentTable + ", dumped " + tableRecordsCount + " records", Collections.singletonMap("count", tableRecordsCount));
    }

    @Override
    public void beginTable(DumpedTableMetadata tableMetadata, Map<String, Object> stats) throws DataStorageManagerException {
        Table table = tableMetadata.table;
        currentTable = table.name;
        listener.log("beginTable", "beginTable " + currentTable + ", stats " + stats, Collections.singletonMap("table", table.name));
        try {
            out.writeUTF(BackupFileConstants.ENTRY_TYPE_TABLE);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        tableRecordsCount = 0;
        try {
            out.writeLong(tableMetadata.logSequenceNumber.ledgerId);
            out.writeLong(tableMetadata.logSequenceNumber.offset);
            out.writeArray(table.serialize());
            out.writeInt(tableMetadata.indexes.size());
            for (Index index : tableMetadata.indexes) {
                out.writeArray(index.serialize());
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void start(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException {
        listener.log("start_tablespace", "dumping tablespace " + schema + ", log position " + logSequenceNumber, Collections.singletonMap("tablespace", schema));
        try {
            out.writeUTF(BackupFileConstants.ENTRY_TYPE_START);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public void receiveTransactionLogChunk(List<DumpedLogEntry> entries) throws DataStorageManagerException {
        try {
            out.writeUTF(BackupFileConstants.ENTRY_TYPE_TXLOGCHUNK);
            out.writeVInt(entries.size());
            for (DumpedLogEntry entry : entries) {
                out.writeVLong(entry.logSequenceNumber.ledgerId);
                out.writeVLong(entry.logSequenceNumber.offset);
                out.writeArray(entry.entryData);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        listener.log("receivetxlog", "dumped " + entries.size() + " txlogentries", Collections.singletonMap("count", entries.size()));
    }

    @Override
    public void receiveTransactionsAtDump(List<Transaction> entries) throws DataStorageManagerException {
        try {
            out.writeUTF(BackupFileConstants.ENTRY_TYPE_TRANSACTIONS);
            out.writeVInt(entries.size());
            for (Transaction entry : entries) {
                out.writeArray(entry.serialize());
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
        listener.log("receivetransactions", "dumped " + entries.size() + " txentries", Collections.singletonMap("count", entries.size()));
    }

}
