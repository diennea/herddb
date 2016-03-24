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
package herddb.log;

import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.utils.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Factory for entries
 *
 * @author enrico.olivelli
 */
public class LogEntryFactory {

    private static byte[] bytes(String name) {
        return name.getBytes(StandardCharsets.UTF_8);
    }

    public static LogEntry createTable(Table table, Transaction transaction) {
        byte[] payload = table.serialize();
        return new LogEntry(System.currentTimeMillis(), LogEntryType.CREATE_TABLE, bytes(table.tablespace), transaction != null ? transaction.transactionId : 0, payload);
    }

    public static LogEntry insert(Table table, Record record, Transaction transaction) {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try {
            // TODO: better serialization
            DataOutputStream dataOutputStream = new DataOutputStream(oo);
            byte[] tableName = bytes(table.name);
            dataOutputStream.writeShort((short) tableName.length);
            dataOutputStream.write(tableName);
            dataOutputStream.writeInt(record.key.data.length);
            dataOutputStream.write(record.key.data);
            dataOutputStream.writeInt(record.value.data.length);
            dataOutputStream.write(record.value.data);
            dataOutputStream.close();
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
        return new LogEntry(System.currentTimeMillis(), LogEntryType.INSERT, bytes(table.tablespace), transaction != null ? transaction.transactionId : 0, oo.toByteArray());
    }

    public static LogEntry update(Table table, Record record, Transaction transaction) {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try {
            // TODO: better serialization
            DataOutputStream dataOutputStream = new DataOutputStream(oo);
            byte[] tableName = bytes(table.name);
            dataOutputStream.writeShort((short) tableName.length);
            dataOutputStream.write(tableName);
            dataOutputStream.writeInt(record.key.data.length);
            dataOutputStream.write(record.key.data);
            dataOutputStream.writeInt(record.value.data.length);
            dataOutputStream.write(record.value.data);
            dataOutputStream.close();
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
        return new LogEntry(System.currentTimeMillis(), LogEntryType.UPDATE, bytes(table.tablespace), transaction != null ? transaction.transactionId : 0, oo.toByteArray());
    }

    public static LogEntry delete(Table table, Bytes key, Transaction transaction) {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try {
            // TODO: better serialization
            DataOutputStream dataOutputStream = new DataOutputStream(oo);
            byte[] tableName = bytes(table.name);
            dataOutputStream.writeShort((short) tableName.length);
            dataOutputStream.write(tableName);
            dataOutputStream.writeInt(key.data.length);
            dataOutputStream.write(key.data);
            dataOutputStream.close();
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DELETE, bytes(table.tablespace), transaction != null ? transaction.transactionId : 0, oo.toByteArray());
    }

}
