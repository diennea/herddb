/*
 * Copyright 2018 enrico.olivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.proto;

import herddb.utils.ByteBufUtils;
import herddb.utils.DataAccessor;
import herddb.utils.IntHolder;
import herddb.utils.RawString;
import herddb.utils.RecordsBatch;
import herddb.utils.TuplesList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Codec for PDUs
 *
 * @author enrico.olivelli
 */
public abstract class PduCodec {

    public static final byte VERSION_3 = 3;

    public static Pdu decodePdu(ByteBuf in) throws IOException {
        byte version = in.getByte(0);
        if (version == VERSION_3) {
            byte flags = in.getByte(1);
            byte type = in.getByte(2);
            long messageId = in.getLong(3);
            Pdu pdu = new Pdu();
            pdu.buffer = in;
            pdu.type = type;
            pdu.flags = flags;
            pdu.messageId = messageId;
            return pdu;
        }
        throw new IOException("Cannot decode version " + version);
    }

    private static final int ONE_BYTE = 1;
    private static final int ONE_INT = 4;
    private static final int ONE_LONG = 8;
    private static final int MSGID_SIZE = 8;
    private static final int TYPE_SIZE = 1;
    private static final int FLAGS_SIZE = 1;
    private static final int VERSION_SIZE = 1;

    private static final int NULLABLE_FIELD_PRESENT = 1;
    private static final int NULLABLE_FIELD_ABSENT = 0;

    public static final byte TYPE_STRING = 0;
    public static final byte TYPE_LONG = 1;
    public static final byte TYPE_INTEGER = 2;
    public static final byte TYPE_BYTEARRAY = 3;
    public static final byte TYPE_TIMESTAMP = 4;
    public static final byte TYPE_NULL = 5;
    public static final byte TYPE_DOUBLE = 6;
    public static final byte TYPE_BOOLEAN = 7;
    public static final byte TYPE_SHORT = 8;

    public static abstract class ExecuteStatementResult {

        public static ByteBuf write(
                long messageId, long updateCount, long tx, Map<String, Object> newRecord) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + ONE_LONG
                            + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENT_RESULT);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(updateCount);
            byteBuf.writeLong(tx);

            // the Map is serialized as a list of objects (k1,v1,k2,v2...)
            int size = newRecord != null ? newRecord.size() : 0;
            ByteBufUtils.writeVInt(byteBuf, size * 2);
            if (newRecord != null) {
                for (Map.Entry<String, Object> entry : newRecord.entrySet()) {
                    writeObject(byteBuf, entry.getKey());
                    writeObject(byteBuf, entry.getValue());
                }
            }
            return byteBuf;
        }

        public static boolean hasRecord(Pdu pdu) {
            return pdu.buffer.writerIndex() > VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG;
        }

        public static ObjectListReader readRecord(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
            );
            int numParams = ByteBufUtils.readVInt(buffer);
            return new ObjectListReader(pdu, numParams);
        }

        public static long readUpdateCount(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static long readTx(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG /* update count */);
        }

    }

    public static abstract class SaslTokenMessageRequest {

        public static ByteBuf write(long messageId, String saslMech, byte[] firstToken) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + 64);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_SASL_TOKEN_MESSAGE_REQUEST);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, saslMech);
            ByteBufUtils.writeArray(byteBuf, firstToken);
            return byteBuf;
        }

        public static String readMech(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return new String(ByteBufUtils.readArray(buffer), StandardCharsets.UTF_8);
        }

        public static byte[] readToken(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            ByteBufUtils.skipArray(buffer);
            return ByteBufUtils.readArray(buffer);
        }
    }

    public static abstract class SaslTokenMessageToken {

        public static ByteBuf write(long messageId, byte[] token) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + 1 + (token != null ? token.length : 0));
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_SASL_TOKEN_MESSAGE_TOKEN);
            byteBuf.writeLong(messageId);
            if (token == null) {
                byteBuf.writeByte(NULLABLE_FIELD_ABSENT);
            } else {
                byteBuf.writeByte(NULLABLE_FIELD_PRESENT);
                ByteBufUtils.writeArray(byteBuf, token);
            }
            return byteBuf;
        }

        public static byte[] readToken(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            byte tokenPresent = buffer.readByte();
            if (tokenPresent == NULLABLE_FIELD_PRESENT) {
                return ByteBufUtils.readArray(buffer);
            } else {
                return null;
            }
        }
    }

    public static class SaslTokenServerResponse {

        public static ByteBuf write(long messageId, byte[] token) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + 64);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_SASL_TOKEN_SERVER_RESPONSE);
            byteBuf.writeLong(messageId);
            if (token != null) {
                ByteBufUtils.writeArray(byteBuf, token);
            }
            return byteBuf;
        }

        public static byte[] readToken(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            if (buffer.writerIndex() > VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE) {
                buffer.readerIndex(0);
                buffer.skipBytes(VERSION_SIZE
                        + FLAGS_SIZE
                        + TYPE_SIZE
                        + MSGID_SIZE);
                return ByteBufUtils.readArray(buffer);
            } else {
                return null;
            }
        }

    }

    public static class AckResponse {

        public static ByteBuf write(long messageId) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_ERROR);
            byteBuf.writeLong(messageId);
            return byteBuf;
        }
    }

    public static class ErrorResponse {

        public static ByteBuf write(long messageId, String error) {
            return write(messageId, error, false);
        }

        public static ByteBuf write(long messageId, String error, boolean notLeader) {
            if (error == null) {
                error = "";
            }
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + ONE_BYTE
                            + error.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_ERROR);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(notLeader ? 1 : 0);
            ByteBufUtils.writeString(byteBuf, error);
            return byteBuf;
        }

        public static ByteBuf write(long messageId, Throwable error, boolean notLeader) {
            StringWriter writer = new StringWriter();
            error.printStackTrace(new PrintWriter(writer));
            return write(messageId, writer.toString(), notLeader);
        }

        public static ByteBuf write(long messageId, Throwable error) {
            return write(messageId, error, false);
        }

        public static String readError(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE);
            return ByteBufUtils.readString(buffer);
        }

        public static boolean readIsNotLeader(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE) == 1;
        }

    }

    public static abstract class TxCommand {

        public static final byte TX_COMMAND_ROLLBACK_TRANSACTION = 1;
        public static final byte TX_COMMAND_COMMIT_TRANSACTION = 2;
        public static final byte TX_COMMAND_BEGIN_TRANSACTION = 3;

        public static ByteBuf write(long messageId, byte command, long tx, String tableSpace) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + ONE_BYTE
                            + ONE_LONG
                            + tableSpace.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_TX_COMMAND);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(command);
            byteBuf.writeLong(tx);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            return byteBuf;
        }

        public static byte readCommand(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);

        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE);

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;

            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG);
            return ByteBufUtils.readString(buffer);

        }
    }

    public static abstract class TxCommandResult {

        public static ByteBuf write(long messageId, long tx) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_TX_COMMAND_RESULT);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(tx);
            return byteBuf;
        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);

        }
    }

    public static class OpenScanner {

        public static ByteBuf write(long messageId, String tableSpace, String query,
                long scannerId, long tx, List<Object> params, int fetchSize, int maxRows) {

            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + ONE_LONG
                            + ONE_INT
                            + ONE_INT
                            + ONE_LONG
                            + 1 + tableSpace.length()
                            + 2 + query.length()
                            + 1 + params.size() * 8);

            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_OPENSCANNER);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(tx);
            byteBuf.writeInt(fetchSize);
            byteBuf.writeInt(maxRows);
            byteBuf.writeLong(scannerId);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, query);

            ByteBufUtils.writeVInt(byteBuf, params.size());
            for (Object p : params) {
                writeObject(byteBuf, p);
            }

            return byteBuf;

        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static int readFetchSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG);
        }

        public static int readMaxRows(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_INT);
        }

        public static long readScannerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_INT
                    + ONE_INT
            );
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_INT
                    + ONE_INT
                    + ONE_LONG);
            return ByteBufUtils.readString(buffer);
        }

        public static String readQuery(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_INT
                    + ONE_INT
                    + ONE_LONG);
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static ObjectListReader startReadParameters(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_INT
                    + ONE_INT
                    + ONE_LONG
            );
            
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // query
            int numParams = ByteBufUtils.readVInt(buffer);
            return new ObjectListReader(pdu, numParams);
        }
    }

    public static class ResultSetChunk {

        public static ByteBuf write(long messageId, TuplesList tuplesList, boolean last, long tx) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + ONE_LONG
                            + ONE_BYTE);

            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_RESULTSET_CHUNK);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(tx);
            byteBuf.writeByte(last ? 1 : 0);

            int numColumns = tuplesList.columnNames.length;
            byteBuf.writeInt(numColumns);
            for (String columnName : tuplesList.columnNames) {
                ByteBufUtils.writeString(byteBuf, columnName);
            }

            // num records
            byteBuf.writeInt(tuplesList.tuples.size());
            System.out.println("WRITING a BATCH of " + tuplesList.tuples.size() + " records");
            for (DataAccessor da : tuplesList.tuples) {
                IntHolder currentColumn = new IntHolder();
                da.forEach((String key, Object value) -> {
                    String expectedColumnName = tuplesList.columnNames[currentColumn.value];
                    while (!key.equals(expectedColumnName)) {
                        // nulls are not returned for some special accessors, like DataAccessorForFullRecord
                        writeObject(byteBuf, null);
                        currentColumn.value++;
                        expectedColumnName = tuplesList.columnNames[currentColumn.value];
                    }
                    writeObject(byteBuf, value);
                    currentColumn.value++;
                });
                // fill with nulls
                while (currentColumn.value < numColumns) {
                    writeObject(byteBuf, null);
                    currentColumn.value++;
                }
                if (currentColumn.value > numColumns) {
                    throw new RuntimeException("unexpected number of columns " + currentColumn.value + " > " + numColumns);
                }
            }
            return byteBuf;
        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static boolean readIsLast(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
            ) == 1;
        }

        public static RecordsBatch startReadingData(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_BYTE);
            return new RecordsBatch(pdu);
        }
    }

    public static class FetchScannerData {

        public static ByteBuf write(long messageId, long scannerId, int fetchSize) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + ONE_LONG
                            + ONE_INT);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_FETCHSCANNERDATA);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(scannerId);
            byteBuf.writeInt(fetchSize);
            return byteBuf;
        }

        public static long readScannerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static int readFetchSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG);
        }
    }

    public static class CloseScanner {

        public static ByteBuf write(long messageId, long scannerId) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_CLOSESCANNER);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(scannerId);
            return byteBuf;
        }

        public static long readScannerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }
    }

    public static class ExecuteStatement {

        public static ByteBuf write(long messageId, String tableSpace, String query, long tx,
                boolean returnValues,
                List<Object> params) {

            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENT);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(returnValues ? 1 : 0);
            byteBuf.writeLong(tx);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, query);

            ByteBufUtils.writeVInt(byteBuf, params.size());
            for (Object p : params) {
                writeObject(byteBuf, p);
            }

            return byteBuf;

        }

        public static boolean readReturnValues(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE) == 1;
        }

        public static long readTx(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE);
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readQuery(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static ObjectListReader startReadParameters(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // query
            int numParams = ByteBufUtils.readVInt(buffer);
            return new ObjectListReader(pdu, numParams);
        }

    }

    public static class ObjectListReader {

        private final Pdu pdu;
        private final int numParams;

        public ObjectListReader(Pdu pdu, int numParams) {
            this.pdu = pdu;
            this.numParams = numParams;
        }

        public int getNumParams() {
            return numParams;
        }

        public Object nextObject() {
            // assuming that the readerIndex is not altered but other direct accesses to the ByteBuf
            return readObject(pdu.buffer);
        }

    }

    private static void writeObject(ByteBuf byteBuf, Object v) {
        if (v == null) {
            byteBuf.writeByte(TYPE_NULL);
        } else if (v instanceof RawString) {
            byteBuf.writeByte(TYPE_STRING);
            ByteBufUtils.writeRawString(byteBuf, (RawString) v);
        } else if (v instanceof String) {
            byteBuf.writeByte(TYPE_STRING);
            ByteBufUtils.writeString(byteBuf, (String) v);
        } else if (v instanceof Long) {
            byteBuf.writeByte(TYPE_LONG);
            byteBuf.writeLong((Long) v);
        } else if (v instanceof Integer) {
            byteBuf.writeByte(TYPE_INTEGER);
            byteBuf.writeInt((Integer) v);
        } else if (v instanceof Boolean) {
            byteBuf.writeByte(TYPE_BOOLEAN);
            byteBuf.writeBoolean((Boolean) v);
        } else if (v instanceof java.util.Date) {
            byteBuf.writeByte(TYPE_TIMESTAMP);
            byteBuf.writeLong(((java.util.Date) v).getTime());
        } else if (v instanceof Double) {
            byteBuf.writeByte(TYPE_DOUBLE);
            byteBuf.writeDouble((Double) v);
        } else if (v instanceof Float) {
            byteBuf.writeByte(TYPE_DOUBLE);
            byteBuf.writeDouble((Float) v);
        } else if (v instanceof Short) {
            byteBuf.writeByte(TYPE_SHORT);
            byteBuf.writeLong((Integer) v);
        } else if (v instanceof byte[]) {
            byteBuf.writeByte(TYPE_BYTEARRAY);
            ByteBufUtils.writeArray(byteBuf, (byte[]) v);
        } else {
            throw new IllegalArgumentException("bad data type " + v.getClass());
        }

    }

    public static Object readObject(ByteBuf dii) {

        int type = ByteBufUtils.readVInt(dii);

        switch (type) {
            case TYPE_BYTEARRAY:
                return ByteBufUtils.readArray(dii);
            case TYPE_INTEGER:
                return dii.readInt();
            case TYPE_LONG:
                return dii.readLong();
            case TYPE_STRING:
                return ByteBufUtils.readRawString(dii);
            case TYPE_TIMESTAMP:
                return new java.sql.Timestamp(dii.readLong());
            case TYPE_NULL:
                return null;
            case TYPE_BOOLEAN:
                return dii.readBoolean();
            case TYPE_DOUBLE:
                return dii.readDouble();
            default:
                throw new IllegalArgumentException("bad column type " + type);
        }
    }

}
