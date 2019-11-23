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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.SystemProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * An entry on the log
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class LogEntry {

    private static final int EMPTY_STRING_LEN;
    private static final byte[] EMPTY_STRING_SERIALIZED;

    static {
        try (ByteArrayOutputStream oo = new ByteArrayOutputStream()) {
            try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(oo)) {
                doo.writeUTF("");
            }
            EMPTY_STRING_LEN = oo.size();
            EMPTY_STRING_SERIALIZED = oo.toByteArray();
        } catch (IOException impossible) {
            throw new RuntimeException(impossible);
        }

    }

    private static final int WRITE_UTF_HEADER_LEN = EMPTY_STRING_LEN;

    public final short type;
    public final long transactionId;
    public final String tableName;
    public final Bytes key;
    public final Bytes value;
    public final long timestamp;

    public LogEntry(long timestamp, short type, long transactionId, String tableName, Bytes key, Bytes value) {
        this.timestamp = timestamp;
        this.type = type;
        this.transactionId = transactionId;
        this.key = key;
        this.value = value;
        this.tableName = tableName;
    }

    public byte[] serialize() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(out)) {
            serialize(doo);
            return out.toByteArray();
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    private static final int DEFAULT_BUFFER_SIZE = SystemProperties.getIntSystemProperty("herddb.log.initentrysize", 2024);

    public ByteBuf serializeAsByteBuf() {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(DEFAULT_BUFFER_SIZE);
        try (ExtendedDataOutputStream doo =
                     new ExtendedDataOutputStream(new ByteBufOutputStream(buffer))) {
            serialize(doo);
            return buffer;
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    /**
     * @param doo
     * @return an estimate on the number of written bytes
     * @throws IOException
     */
    public int serialize(ExtendedDataOutputStream doo) throws IOException {
        int startingsize = doo.size();
        doo.writeLong(timestamp); // 8
        doo.writeShort(type);  // 2
        doo.writeLong(transactionId); // 8
        doo.write(EMPTY_STRING_SERIALIZED); // keep compatibility with v 0.2
        switch (type) {
            case LogEntryType.UPDATE:
                doo.writeUTF(tableName);
                doo.writeArray(key);
                doo.writeArray(value);
                break;
            case LogEntryType.INSERT:
                doo.writeUTF(tableName);
                doo.writeArray(key);
                doo.writeArray(value);
                break;
            case LogEntryType.DELETE:
                doo.writeUTF(tableName);
                doo.writeArray(key);
                break;
            case LogEntryType.CREATE_TABLE:
            case LogEntryType.ALTER_TABLE:
                // value contains the table definition
                doo.writeUTF(tableName);
                doo.writeArray(value);
                break;
            case LogEntryType.CREATE_INDEX:
                // value contains the index definition
                doo.writeUTF(tableName);
                doo.writeArray(value);
                break;
            case LogEntryType.DROP_TABLE:
            case LogEntryType.TRUNCATE_TABLE:
                doo.writeUTF(tableName);
                break;
            case LogEntryType.DROP_INDEX:
                doo.writeArray(this.value);
                break;
            case LogEntryType.BEGINTRANSACTION:
            case LogEntryType.COMMITTRANSACTION:
            case LogEntryType.ROLLBACKTRANSACTION:
                break;
            case LogEntryType.NOOP:
                break;
            case LogEntryType.TABLE_INTEGRITY_CHECKSUM:
                break;
            default:
                throw new IllegalArgumentException("unsupported type " + type);
        }
        return doo.size() - startingsize;
    }

    public static LogEntry deserialize(byte[] data) throws EOFException {
        SimpleByteArrayInputStream in = new SimpleByteArrayInputStream(data);
        ExtendedDataInputStream dis = new ExtendedDataInputStream(in);
        return deserialize(dis);
    }

    public static LogEntry deserialize(ExtendedDataInputStream dis) throws EOFException {
        try {
            long timestamp = dis.readLong();
            short type = dis.readShort();
            long transactionId = dis.readLong();
            dis.readUTF(); // in 0.2 it was 'tablespace uuid'

            Bytes key = null;
            Bytes value = null;
            String tableName = null;
            switch (type) {
                case LogEntryType.UPDATE:
                    tableName = dis.readUTF();
                    key = dis.readBytes();
                    value = dis.readBytes();
                    break;
                case LogEntryType.INSERT:
                    tableName = dis.readUTF();
                    key = dis.readBytes();
                    value = dis.readBytes();
                    break;
                case LogEntryType.DELETE:
                    tableName = dis.readUTF();
                    key = dis.readBytes();
                    break;
                case LogEntryType.DROP_TABLE:
                case LogEntryType.TRUNCATE_TABLE:
                    tableName = dis.readUTF();
                    break;
                case LogEntryType.DROP_INDEX:
                    value = dis.readBytes();
                    break;
                case LogEntryType.CREATE_INDEX:
                    tableName = dis.readUTF();
                    value = dis.readBytes();
                    break;
                case LogEntryType.CREATE_TABLE:
                case LogEntryType.ALTER_TABLE:
                    // value contains the table definition
                    tableName = dis.readUTF();
                    value = dis.readBytes();
                    break;
                case LogEntryType.BEGINTRANSACTION:
                case LogEntryType.COMMITTRANSACTION:
                case LogEntryType.ROLLBACKTRANSACTION:
                    break;
                case LogEntryType.NOOP:
                    break;
                case LogEntryType.TABLE_INTEGRITY_CHECKSUM:
                break;
                default:
                    throw new IllegalArgumentException("unsupported type " + type);
            }
            return new LogEntry(timestamp, type, transactionId, tableName, key, value);
        } catch (EOFException e) {
            /* Entry "corrupted" cause stream premature end, propagate */
            throw e;
        } catch (IOException err) {
            /* Stream read error */
            throw new RuntimeException(err);
        }
    }

    @Override
    public String toString() {
        return "LogEntry{" + "type=" + type + ", transactionId=" + transactionId + ", tableName=" + tableName + ", key=" + key + ", value=" + value + ", timestamp=" + timestamp + '}';
    }

}
