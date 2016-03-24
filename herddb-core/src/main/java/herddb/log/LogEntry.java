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

/**
 * An entry on the log
 *
 * @author enrico.olivelli
 */
public class LogEntry {

    public final short type;
    public final byte[] tableSpace;
    public final long transactionId;
    public final byte[] payload;
    public final long timestamp;

    public LogEntry(long timestamp, short type, byte[] tableSpace, long transactionId, byte[] payload) {
        this.timestamp = timestamp;
        this.type = type;
        this.tableSpace = tableSpace;
        this.transactionId = transactionId;
        this.payload = payload;
    }

    public byte[] serialize() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(8 + 2 + 2 + tableSpace.length + 8 + 4 + payload.length);
            DataOutputStream doo = new DataOutputStream(out);
            doo.writeLong(timestamp);
            doo.writeShort(type);
            doo.writeLong(transactionId);
            doo.writeShort(tableSpace.length);
            doo.write(tableSpace);
            doo.writeInt(payload.length);
            doo.write(payload);
            doo.close();
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
            int payloadlen = dis.readInt();
            byte[] payload = new byte[payloadlen];
            dis.readFully(payload);
            return new LogEntry(timestamp, type, tableSpace, transactionId, payload);
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

}
