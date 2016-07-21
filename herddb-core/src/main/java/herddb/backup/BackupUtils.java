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
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleBufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import static java.sql.DriverManager.println;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.xml.ws.Holder;

/**
 * Backup Restore Utility
 *
 * @author enrico.olivelli
 */
public class BackupUtils {

    public static void dumpTableSpace(String schema, HDBConnection connection, OutputStream fout) throws Exception {
        try (ExtendedDataOutputStream eout = new ExtendedDataOutputStream(fout)) {
            dumpTablespace(schema, connection, eout);
        }
    }

    private static void dumpTablespace(String schema, HDBConnection hdbconnection, ExtendedDataOutputStream out) throws Exception {

        Holder<Throwable> errorHolder = new Holder<>();
        CountDownLatch waiter = new CountDownLatch(1);
        hdbconnection.dumpTableSpace(schema, new TableSpaceDumpReceiver() {

            long tableCount;

            @Override
            public void onError(Throwable error) throws DataStorageManagerException {
                errorHolder.value = error;
                waiter.countDown();
            }

            @Override
            public void finish() throws DataStorageManagerException {
                waiter.countDown();
            }

            @Override
            public void endTable() throws DataStorageManagerException {
                println("endTable, records " + tableCount);
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
                    tableCount += record.size();
                } catch (IOException err) {
                    throw new DataStorageManagerException(err);
                }
            }

            @Override
            public void beginTable(Table table) throws DataStorageManagerException {
                println("beginTable " + table.name);
                tableCount = 0;
                try {
                    out.writeArray(table.serialize());
                } catch (IOException err) {
                    throw new DataStorageManagerException(err);
                }
            }

            @Override
            public void start(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException {
                println("Backup at log position: " + logSequenceNumber);
            }

        }, 64 * 1024);
        if (errorHolder.value != null) {
            throw new Exception(errorHolder.value);
        }
        waiter.await();
    }

    public static void restoreTableSpace(String schema, String node, HDBConnection hdbconnection, InputStream fin) throws Exception {
        try (ExtendedDataInputStream ii = new ExtendedDataInputStream(new BufferedInputStream(fin, 64 * 1024 * 1024))) {
            hdbconnection.executeUpdate(TableSpace.DEFAULT, "CREATE TABLESPACE '" + schema + "','leader:" + node + "','wait:60000'", 0, Collections.emptyList());

            TableSpaceRestoreSource source = new TableSpaceRestoreSource() {

                @Override
                public List<KeyValue> nextTableDataChunk() throws DataStorageManagerException {
                    try {
                        int numRecords = ii.readVInt();
                        if (Integer.MIN_VALUE == numRecords) {
                            // EndOfTableMarker
                            return null;
                        }
                        List<KeyValue> records = new ArrayList<>(numRecords);
                        for (int i = 0; i < numRecords; i++) {
                            byte[] key = ii.readArray();
                            byte[] value = ii.readArray();
                            records.add(new KeyValue(key, value));
                        }
                        return records;
                    } catch (IOException err) {
                        throw new DataStorageManagerException(err);
                    }
                }

                @Override
                public Table nextTable() throws DataStorageManagerException {
                    try {
                        byte[] table = ii.readArray();
                        return Table.deserialize(table);
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
