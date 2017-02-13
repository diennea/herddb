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

import herddb.client.TableSpaceRestoreSource;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Table;
import herddb.network.KeyValue;
import herddb.storage.DataStorageManagerException;
import herddb.utils.ExtendedDataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Drives the restore from File
 *
 * @author enrico.olivelli
 */
class TableSpaceRestoreSourceFromFile extends TableSpaceRestoreSource {

    private final ExtendedDataInputStream in;
    private final ProgressListener listener;

    public TableSpaceRestoreSourceFromFile(ExtendedDataInputStream in, ProgressListener listener) {
        this.in = in;
        this.listener = listener;
    }
    private long currentTableSize;

    @Override
    public String nextEntryType() throws DataStorageManagerException {
        try {
            return in.readUTF();
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public List<KeyValue> nextTransactionLogChunk() throws DataStorageManagerException {
        try {
            int numRecords = in.readVInt();
            listener.log("nexttxchunk", "sending " + numRecords + " tx log entries", Collections.singletonMap("count", numRecords));
            List<KeyValue> records = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                long ledgerId = in.readVLong();
                long offset = in.readVLong();
                byte[] value = in.readArray();
                records.add(new KeyValue(new LogSequenceNumber(ledgerId, offset).serialize(), value));
            }
            return records;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public List<byte[]> nextTransactionsBlock() throws DataStorageManagerException {
        try {
            int numRecords = in.readVInt();
            listener.log("nextTransactionsBlock", "sending " + numRecords + " tx entries", Collections.singletonMap("count", numRecords));
            List<byte[]> records = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                byte[] value = in.readArray();
                records.add(value);
            }
            return records;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public List<KeyValue> nextTableDataChunk() throws DataStorageManagerException {
        try {
            int numRecords = in.readVInt();
            if (Integer.MIN_VALUE == numRecords) {
                // EndOfTableMarker
                listener.log("tablefinished", "table finished after " + currentTableSize + " records", Collections.singletonMap("count", numRecords));
                return null;
            }
            listener.log("sendtabledata", "sending " + numRecords + ", total " + currentTableSize, Collections.singletonMap("count", numRecords));
            List<KeyValue> records = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                byte[] key = in.readArray();
                byte[] value = in.readArray();
                records.add(new KeyValue(key, value));
            }
            currentTableSize += numRecords;
            return records;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    @Override
    public DumpedTableMetadata nextTable() throws DataStorageManagerException {
        currentTableSize = 0;
        try {
            long dumpLedgerId = in.readLong();
            long dumpOffset = in.readLong();
            byte[] table = in.readArray();
            int numIndexes = in.readInt();
            List<byte[]> indexesData = new ArrayList<>();
            for (int i = 0; i < numIndexes; i++) {
                byte[] index = in.readArray();
                indexesData.add(index);
            }
            List<Index> indexes = indexesData.stream().map(Index::deserialize).collect(Collectors.toList());
            Table tableMetadata = Table.deserialize(table);
            Map<String, Object> data = new HashMap<>();
            data.put("table", tableMetadata.name);
            data.put("dumpLedgerId", dumpLedgerId);
            data.put("dumpOffset", dumpOffset);
            data.put("indexes", indexesData.size());
            listener.log("starttable", "starting table " + tableMetadata.name, data);
            return new DumpedTableMetadata(tableMetadata, new LogSequenceNumber(dumpLedgerId, dumpOffset), indexes);
        } catch (EOFException end) {
            return null;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

}
