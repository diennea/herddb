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

import herddb.client.HDBConnection;
import herddb.client.TableSpaceDumpReceiver;
import herddb.client.TableSpaceRestoreSource;
import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.network.KeyValue;
import herddb.storage.DataStorageManagerException;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import javax.xml.ws.Holder;

/**
 * Backup Restore Utility
 *
 * @author enrico.olivelli
 */
public class BackupUtils {

    public static void dumpTableSpace(String schema, int fetchSize, HDBConnection connection, OutputStream fout, ProgressListener listener) throws Exception {
        try (ExtendedDataOutputStream eout = new ExtendedDataOutputStream(fout)) {
            dumpTablespace(schema, fetchSize, connection, eout, listener);
        }
    }

    private static void dumpTablespace(String schema, int fetchSize, HDBConnection hdbconnection, ExtendedDataOutputStream out, ProgressListener listener) throws Exception {

        Holder<Throwable> errorHolder = new Holder<>();
        CountDownLatch waiter = new CountDownLatch(1);
        hdbconnection.dumpTableSpace(schema, new TableSpaceDumpReceiver() {

            long tableRecordsCount;
            String currentTable;

            @Override
            public void onError(Throwable error) throws DataStorageManagerException {
                listener.log("Fatal error: " + error, Collections.singletonMap("error", error));
                errorHolder.value = error;
                waiter.countDown();
            }

            @Override
            public void finish() throws DataStorageManagerException {
                listener.log("Dump finished for tablespace " + schema, Collections.singletonMap("tablespace", schema));
                waiter.countDown();
            }

            @Override
            public void endTable() throws DataStorageManagerException {
                listener.log("endTable " + currentTable + ", records " + tableRecordsCount, Collections.singletonMap("table", currentTable));
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
                listener.log("table " + currentTable + ", dumped " + tableRecordsCount + " records", Collections.singletonMap("count", tableRecordsCount));
            }

            @Override
            public void beginTable(Table table, Map<String, Object> stats) throws DataStorageManagerException {
                currentTable = table.name;
                listener.log("beginTable " + currentTable + ", stats " + stats, Collections.singletonMap("table", table.name));
                tableRecordsCount = 0;
                try {
                    out.writeArray(table.serialize());
                } catch (IOException err) {
                    throw new DataStorageManagerException(err);
                }
            }

            @Override
            public void start(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException {
                listener.log("dumping tablespace " + schema + ", log position " + logSequenceNumber, Collections.singletonMap("tablespace", schema));
            }

        }, fetchSize);
        if (errorHolder.value != null) {
            throw new Exception(errorHolder.value);
        }
        waiter.await();
    }

    public static void restoreTableSpace(String schema, String node, HDBConnection hdbconnection, InputStream fin, ProgressListener listener) throws Exception {
        try (ExtendedDataInputStream ii = new ExtendedDataInputStream(new BufferedInputStream(fin, 64 * 1024 * 1024))) {
            listener.log("Creating tablespace " + schema + " with leader " + node, Collections.singletonMap("tablespace", schema));
            hdbconnection.executeUpdate(TableSpace.DEFAULT, "CREATE TABLESPACE '" + schema + "','leader:" + node + "','wait:60000'", 0, Collections.emptyList());

            TableSpaceRestoreSource source = new TableSpaceRestoreSource() {
                long currentTableSize;

                @Override
                public List<KeyValue> nextTableDataChunk() throws DataStorageManagerException {
                    try {
                        int numRecords = ii.readVInt();
                        if (Integer.MIN_VALUE == numRecords) {
                            // EndOfTableMarker
                            listener.log("table finished after " + currentTableSize + " records", Collections.singletonMap("count", numRecords));
                            return null;
                        }
                        listener.log("sending " + numRecords + ", total " + currentTableSize,
                            Collections.singletonMap("count", numRecords));
                        List<KeyValue> records = new ArrayList<>(numRecords);
                        for (int i = 0; i < numRecords; i++) {
                            byte[] key = ii.readArray();
                            byte[] value = ii.readArray();
                            records.add(new KeyValue(key, value));
                        }
                        currentTableSize += numRecords;
                        return records;
                    } catch (IOException err) {
                        throw new DataStorageManagerException(err);
                    }
                }

                @Override
                public Table nextTable() throws DataStorageManagerException {
                    currentTableSize = 0;
                    try {
                        byte[] table = ii.readArray();
                        Table tableMetadata = Table.deserialize(table);
                        listener.log("starting table " + tableMetadata.name, Collections.singletonMap("table", tableMetadata.name));
                        return tableMetadata;
                    } catch (EOFException end) {
                        return null;
                    } catch (IOException err) {
                        throw new DataStorageManagerException(err);
                    }
                }

            };
            hdbconnection.restoreTableSpace(schema, source);
        }
    }
}
