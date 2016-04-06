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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * An entry on the log
 *
 * @author enrico.olivelli
 */
public class LogEntry {

    public final short type;
    public final byte[] tableSpace;
    public final long transactionId;
    public final byte[] tableName;
    public final byte[] key;
    public final byte[] value;
    public final long timestamp;

    public LogEntry(long timestamp, short type, byte[] tableSpace, long transactionId, byte[] tableName, byte[] key, byte[] value) {
        this.timestamp = timestamp;
        this.type = type;
        this.tableSpace = tableSpace;
        this.transactionId = transactionId;
        this.key = key;
        this.value = value;
        this.tableName = tableName;
    }

    public byte[] serialize() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (DataOutputStream doo = new DataOutputStream(out)) {
                doo.writeLong(timestamp);
                doo.writeShort(type);
                doo.writeLong(transactionId);
                doo.writeShort(tableSpace.length);
                doo.write(tableSpace);
                switch (type) {
                    case LogEntryType.UPDATE:
                        doo.writeInt(tableName.length);
                        doo.write(tableName);
                        doo.writeInt(key.length);
                        doo.write(key);
                        doo.writeInt(value.length);
                        doo.write(value);
                        break;
                    case LogEntryType.INSERT:
                        doo.writeInt(tableName.length);
                        doo.write(tableName);
                        doo.writeInt(key.length);
                        doo.write(key);
                        doo.writeInt(value.length);
                        doo.write(value);
                        break;
                    case LogEntryType.DELETE:
                        doo.writeInt(tableName.length);
                        doo.write(tableName);
                        doo.writeInt(key.length);
                        doo.write(key);
                        break;
                    case LogEntryType.CREATE_TABLE:
                        // value contains the table definition
                        doo.writeInt(value.length);
                        doo.write(value);
                        break;
                    default:
                        throw new IllegalArgumentException("unsupported type " + type);
                }
            }
            return out.toByteArray();
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    public static LogEntry deserialize(byte[] data) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(in);
            long timestamp = dis.readLong();
            short type = dis.readShort();
            long transactionId = dis.readLong();
            int tableSpaceLen = dis.readShort();
            byte[] tableSpace = new byte[tableSpaceLen];
            dis.readFully(tableSpace);
            byte[] key = null;
            byte[] value = null;
            byte[] tableName = null;
            switch (type) {
                case LogEntryType.UPDATE:
                    tableName = readArray(dis);
                    key = readArray(dis);
                    value = readArray(dis);
                    break;
                case LogEntryType.INSERT:
                    tableName = readArray(dis);
                    key = readArray(dis);
                    value = readArray(dis);
                    break;
                case LogEntryType.DELETE:
                    tableName = readArray(dis);
                    key = readArray(dis);
                    break;
                case LogEntryType.CREATE_TABLE:
                    // value contains the table definition                                        
                    value = readArray(dis);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported type " + type);
            }
            return new LogEntry(timestamp, type, tableSpace, transactionId, tableName, key, value);
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    private static byte[] readArray(DataInputStream doo) throws IOException {
        int len = doo.readInt();
        byte[] res = new byte[len];
        doo.readFully(res);
        return res;
    }

    @Override
    public String toString() {
        return "LogEntry{" + "type=" + type + ", tableSpace=" + new String(tableSpace, StandardCharsets.UTF_8) + ", transactionId=" + transactionId + ", tableName=" + tableName + '}';
    }

}
