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

package herddb.proto;

import herddb.utils.ByteBufUtils;
import herddb.utils.DataAccessor;
import herddb.utils.IntHolder;
import herddb.utils.KeyValue;
import herddb.utils.RawString;
import herddb.utils.RecordsBatch;
import herddb.utils.TuplesList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
            return Pdu.newPdu(in, type, flags, messageId);
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
    public static final byte TYPE_BYTE = 9;

    public abstract static class ExecuteStatementsResult {

        public static ByteBuf write(long replyId, List<Long> updateCounts, List<Map<String, Object>> otherdata, long tx) {
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
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENTS_RESULT);
            byteBuf.writeLong(replyId);
            byteBuf.writeLong(tx);
            byteBuf.writeInt(updateCounts.size());
            for (Long updateCount : updateCounts) {
                byteBuf.writeLong(updateCount);
            }
            byteBuf.writeInt(otherdata.size());
            for (Map<String, Object> record : otherdata) {
                // the Map is serialized as a list of objects (k1,v1,k2,v2...)
                int size = record != null ? record.size() : 0;
                ByteBufUtils.writeVInt(byteBuf, size * 2);
                if (record != null) {
                    for (Map.Entry<String, Object> entry : record.entrySet()) {
                        writeObject(byteBuf, entry.getKey());
                        writeObject(byteBuf, entry.getValue());
                    }
                }
            }

            return byteBuf;
        }

        public static long readTx(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE);
        }

        public static List<Long> readUpdateCounts(Pdu pdu) {
            pdu.buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG);
            int numStatements = pdu.buffer.readInt();
            List<Long> res = new ArrayList<>(numStatements);
            for (int i = 0; i < numStatements; i++) {
                res.add(pdu.buffer.readLong());
            }
            return res;
        }

        public static ListOfListsReader startResultRecords(Pdu pdu) {
            final ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG);
            int numStatements = buffer.readInt();
            for (int i = 0; i < numStatements; i++) {
                buffer.skipBytes(ONE_LONG);
            }
            int numLists = ByteBufUtils.readVInt(buffer);
            return new ListOfListsReader(pdu, numLists);
        }
    }

    public abstract static class ExecuteStatementResult {

        public static ByteBuf write(
                long messageId, long updateCount, long tx, Map<String, Object> record
        ) {
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
            int size = record != null ? record.size() : 0;
            ByteBufUtils.writeVInt(byteBuf, size * 2);
            if (record != null) {
                for (Map.Entry<String, Object> entry : record.entrySet()) {
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

    public abstract static class PrepareStatementResult {

        public static ByteBuf write(
                long messageId, long statementId
        ) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISRESPONSE);
            byteBuf.writeByte(Pdu.TYPE_PREPARE_STATEMENT_RESULT);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(statementId);
            return byteBuf;
        }

        public static long readStatementId(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE);
        }

    }

    public abstract static class SaslTokenMessageRequest {

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

    public abstract static class SaslTokenMessageToken {

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
            byteBuf.writeByte(Pdu.TYPE_ACK);
            byteBuf.writeLong(messageId);
            return byteBuf;
        }
    }

    public static class ErrorResponse {

        public static final byte FLAG_NONE = 0;
        public static final byte FLAG_NOT_LEADER = 1;
        public static final byte FLAG_MISSING_PREPARED_STATEMENT = 2;
        public static final byte FLAG_DUPLICATEPRIMARY_KEY_ERROR = 4;

        public static ByteBuf write(long messageId, String error) {
            return write(messageId, error, false, false, false);
        }

        public static ByteBuf writeNotLeaderError(long messageId, String message) {
            return write(messageId, message, true, false, false);
        }

        public static ByteBuf writeMissingPreparedStatementError(long messageId, String message) {
            return write(messageId, message, false, true, false);
        }

        public static ByteBuf writeNotLeaderError(long messageId, Throwable message) {
            return write(messageId, message.toString(), true, false, false);
        }

        public static ByteBuf writeSqlIntegrityConstraintsViolation(long messageId, Throwable message) {
            return write(messageId, message.toString(), false, false, true);
        }

        private static ByteBuf write(long messageId, String error, boolean notLeader, boolean missingPreparedStatement, boolean sqlIntegrityConstraintViolation) {
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
            byte flags = FLAG_NONE;
            if (notLeader) {
                flags = (byte) (flags | FLAG_NOT_LEADER);
            }
            if (missingPreparedStatement) {
                flags = (byte) (flags | FLAG_MISSING_PREPARED_STATEMENT);
            }
            if (sqlIntegrityConstraintViolation) {
                flags = (byte) (flags | FLAG_DUPLICATEPRIMARY_KEY_ERROR);
            }
            byteBuf.writeByte(flags);
            ByteBufUtils.writeString(byteBuf, error);
            return byteBuf;
        }

        public static ByteBuf write(long messageId, Throwable error, boolean notLeader, boolean missingPreparedStatementError) {
            StringWriter writer = new StringWriter();
            error.printStackTrace(new PrintWriter(writer));
            return write(messageId, writer.toString(), notLeader, missingPreparedStatementError, false);
        }

        public static ByteBuf write(long messageId, Throwable error) {
            return write(messageId, error, false, false);
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
            byte read = buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return (read & FLAG_NOT_LEADER) == FLAG_NOT_LEADER;
        }

        public static boolean readIsMissingPreparedStatementError(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            byte read = buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return (read & FLAG_MISSING_PREPARED_STATEMENT) == FLAG_MISSING_PREPARED_STATEMENT;
        }

        public static boolean readIsSqlIntegrityViolationError(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            byte read = buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return (read & FLAG_DUPLICATEPRIMARY_KEY_ERROR) == FLAG_DUPLICATEPRIMARY_KEY_ERROR;
        }
    }

    public abstract static class TxCommand {

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

    public abstract static class TxCommandResult {

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

        public static ByteBuf write(
                long messageId, String tableSpace, String query,
                long scannerId, long tx, List<Object> params, long statementId, int fetchSize, int maxRows
        ) {

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
                                    + ONE_LONG
                                    + 1 + tableSpace.length()
                                    + 2 + query.length()
                                    + 1 + params.size() * 8);

            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_OPENSCANNER);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(tx);
            byteBuf.writeLong(statementId);
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

        public static long readStatementId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG);
        }

        public static int readFetchSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG);
        }

        public static int readMaxRows(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
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

        private static int estimateTupleListSize(TuplesList data) {
            return data.tuples.size() * 1024 + data.columnNames.length * 64;
        }

        public static ByteBuf write(long messageId, TuplesList tuplesList, boolean last, long tx) {
            int dataSize = estimateTupleListSize(tuplesList);
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG
                                    + ONE_BYTE
                                    + dataSize);

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

    public static class ExecuteStatements {

        public static ByteBuf write(
                long messageId, String tableSpace, String query,
                long tx, boolean returnValues, long statementId, List<List<Object>> statements
        ) {

            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG
                                    + ONE_BYTE
                                    + ONE_LONG
                                    + 1 + statements.size() * 64);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENTS);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(returnValues ? 1 : 0);
            byteBuf.writeLong(tx);
            byteBuf.writeLong(statementId);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, query);

            // number of statements
            ByteBufUtils.writeVInt(byteBuf, statements.size());
            for (List<Object> list : statements) {

                // number of params
                ByteBufUtils.writeVInt(byteBuf, list.size());
                for (Object param : list) {
                    writeObject(byteBuf, param);
                }
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

        public static long readStatementId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG);
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
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
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static ListOfListsReader startReadStatementsParameters(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // query
            int numLists = ByteBufUtils.readVInt(buffer);
            return new ListOfListsReader(pdu, numLists);
        }

    }

    public static class ExecuteStatement {

        public static ByteBuf write(
                long messageId, String tableSpace, String query, long tx,
                boolean returnValues, long statementId,
                List<Object> params
        ) {

            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_BYTE
                                    + ONE_LONG
                                    + tableSpace.length()
                                    + query.length()
                                    + ONE_BYTE
                                    + params.size() * 8);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENT);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(returnValues ? 1 : 0);
            byteBuf.writeLong(tx);
            byteBuf.writeLong(statementId);
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

        public static long readStatementId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG);
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_LONG
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
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // query
            int numParams = ByteBufUtils.readVInt(buffer);
            return new ObjectListReader(pdu, numParams);
        }

    }

    public static class PrepareStatement {

        public static ByteBuf write(long messageId, String tableSpace, String query) {

            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_PREPARE_STATEMENT);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, query);

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readQuery(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

    }

    public static class RequestTablespaceDump {

        public static ByteBuf write(long messageId, String tableSpace, String dumpId, int fetchSize, boolean includeTransactionLog) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_BYTE
                                    + ONE_INT
                                    + tableSpace.length()
                                    + dumpId.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_REQUEST_TABLESPACE_DUMP);
            byteBuf.writeLong(messageId);
            byteBuf.writeByte(includeTransactionLog ? 1 : 0);
            byteBuf.writeInt(fetchSize);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, dumpId);

            return byteBuf;

        }

        public static boolean readInludeTransactionLog(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getByte(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE) == 1;
        }

        public static int readFetchSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getInt(VERSION_SIZE
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
                    + ONE_INT
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readDumpId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_BYTE
                    + ONE_INT
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }
    }

    public static class TablespaceDumpData {

        public static ByteBuf write(
                long messageId, String tableSpace, String dumpId,
                String command, byte[] tableDefinition, long estimatedSize,
                long dumpLedgerid, long dumpOffset, List<byte[]> indexesDefinition,
                List<KeyValue> records
        ) {
            if (tableDefinition == null) {
                tableDefinition = new byte[0];
            }
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_BYTE
                                    + ONE_INT
                                    + tableDefinition.length
                                    + tableSpace.length()
                                    + dumpId.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_TABLESPACE_DUMP_DATA);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(dumpLedgerid);
            byteBuf.writeLong(dumpOffset);
            byteBuf.writeLong(estimatedSize);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, dumpId);
            ByteBufUtils.writeString(byteBuf, command);
            ByteBufUtils.writeArray(byteBuf, tableDefinition);

            if (indexesDefinition == null) {
                byteBuf.writeInt(0);
            } else {
                byteBuf.writeInt(indexesDefinition.size());
                for (int i = 0; i < indexesDefinition.size(); i++) {
                    ByteBufUtils.writeArray(byteBuf, indexesDefinition.get(i));
                }
            }

            if (records == null) {
                byteBuf.writeInt(0);
            } else {
                byteBuf.writeInt(records.size());
                for (KeyValue kv : records) {
                    ByteBufUtils.writeArray(byteBuf, kv.key);
                    ByteBufUtils.writeArray(byteBuf, kv.value);
                }
            }

            return byteBuf;

        }

        public static long readLedgerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static long readOffset(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
            );
        }

        public static long readEstimatedSize(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
            );
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readDumpId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static String readCommand(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // dumpId
            return ByteBufUtils.readString(buffer);
        }

        public static byte[] readTableDefinition(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // dumpId
            ByteBufUtils.skipArray(buffer); // command
            return ByteBufUtils.readArray(buffer);
        }

        public static List<byte[]> readIndexesDefinition(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // dumpId
            ByteBufUtils.skipArray(buffer); // command
            ByteBufUtils.skipArray(buffer); // tableDefinition
            int num = buffer.readInt();
            List<byte[]> res = new ArrayList<>();
            for (int i = 0; i < num; i++) {
                res.add(ByteBufUtils.readArray(buffer));
            }
            return res;
        }

        public static void readRecords(Pdu pdu, BiConsumer<byte[], byte[]> consumer) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // dumpId
            ByteBufUtils.skipArray(buffer); // command
            ByteBufUtils.skipArray(buffer); // tableDefinition
            int num = buffer.readInt();
            for (int i = 0; i < num; i++) {
                ByteBufUtils.skipArray(buffer);
            }
            int numRecords = buffer.readInt();
            for (int i = 0; i < numRecords; i++) {
                byte[] key = ByteBufUtils.readArray(buffer);
                byte[] value = ByteBufUtils.readArray(buffer);
                consumer.accept(key, value);
            }
        }
    }

    public static class RequestTableRestore {

        public static ByteBuf write(
                long messageId, String tableSpace, byte[] tableDefinition,
                long dumpLedgerId, long dumpOffset
        ) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + ONE_LONG
                                    + ONE_LONG
                                    + tableSpace.length()
                                    + tableDefinition.length
                    );
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_REQUEST_TABLE_RESTORE);
            byteBuf.writeLong(messageId);
            byteBuf.writeLong(dumpLedgerId);
            byteBuf.writeLong(dumpOffset);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeArray(byteBuf, tableDefinition);
            return byteBuf;

        }

        public static long readLedgerId(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
        }

        public static long readOffset(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            return buffer.getLong(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
            );
        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
            );
            return ByteBufUtils.readString(buffer);
        }

        public static byte[] readTableDefinition(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
                    + ONE_LONG
                    + ONE_LONG
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readArray(buffer);
        }

    }

    public static class TableRestoreFinished {

        public static ByteBuf write(
                long messageId, String tableSpace, String tableName,
                List<byte[]> indexesDefinition
        ) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length()
                                    + tableName.length()
                                    + (indexesDefinition == null ? 0 : (indexesDefinition.size() * 64)));
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_TABLE_RESTORE_FINISHED);
            byteBuf.writeLong(messageId);

            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, tableName);

            if (indexesDefinition == null) {
                byteBuf.writeInt(0);
            } else {
                byteBuf.writeInt(indexesDefinition.size());
                for (int i = 0; i < indexesDefinition.size(); i++) {
                    ByteBufUtils.writeArray(byteBuf, indexesDefinition.get(i));
                }
            }

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readTableName(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static List<byte[]> readIndexesDefinition(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // tableName
            int num = buffer.readInt();
            List<byte[]> res = new ArrayList<>();
            for (int i = 0; i < num; i++) {
                res.add(ByteBufUtils.readArray(buffer));
            }
            return res;
        }

    }

    public static class RestoreFinished {

        public static ByteBuf write(long messageId, String tableSpace) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length());
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_RESTORE_FINISHED);
            byteBuf.writeLong(messageId);

            ByteBufUtils.writeString(byteBuf, tableSpace);

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

    }

    public static class PushTableData {

        public static ByteBuf write(long messageId, String tableSpace, String tableName, List<KeyValue> records) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length()
                                    + tableName.length()
                                    + records.size() * 512);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_PUSH_TABLE_DATA);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, tableSpace);
            ByteBufUtils.writeString(byteBuf, tableName);

            byteBuf.writeInt(records.size());
            for (KeyValue kv : records) {
                ByteBufUtils.writeArray(byteBuf, kv.key);
                ByteBufUtils.writeArray(byteBuf, kv.value);
            }

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static String readTablename(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            return ByteBufUtils.readString(buffer);
        }

        public static void readRecords(Pdu pdu, BiConsumer<byte[], byte[]> consumer) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace
            ByteBufUtils.skipArray(buffer); // tablename
            int numRecords = buffer.readInt();
            for (int i = 0; i < numRecords; i++) {
                byte[] key = ByteBufUtils.readArray(buffer);
                byte[] value = ByteBufUtils.readArray(buffer);
                consumer.accept(key, value);
            }
        }
    }

    public static class PushTxLogChunk {

        public static ByteBuf write(long messageId, String tableSpace, List<KeyValue> records) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length()
                                    + records.size() * 512);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_PUSH_TXLOGCHUNK);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, tableSpace);

            byteBuf.writeInt(records.size());
            for (KeyValue kv : records) {
                ByteBufUtils.writeArray(byteBuf, kv.key);
                ByteBufUtils.writeArray(byteBuf, kv.value);
            }

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static void readRecords(Pdu pdu, BiConsumer<byte[], byte[]> consumer) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace

            int numRecords = buffer.readInt();
            for (int i = 0; i < numRecords; i++) {
                byte[] key = ByteBufUtils.readArray(buffer);
                byte[] value = ByteBufUtils.readArray(buffer);
                consumer.accept(key, value);
            }
        }
    }

    public static class PushTransactionsBlock {

        public static ByteBuf write(long messageId, String tableSpace, List<byte[]> records) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                                    + FLAGS_SIZE
                                    + TYPE_SIZE
                                    + MSGID_SIZE
                                    + tableSpace.length()
                                    + records.size() * 512);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.FLAGS_ISREQUEST);
            byteBuf.writeByte(Pdu.TYPE_PUSH_TRANSACTIONSBLOCK);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeString(byteBuf, tableSpace);

            byteBuf.writeInt(records.size());
            for (byte[] tx : records) {
                ByteBufUtils.writeArray(byteBuf, tx);
            }

            return byteBuf;

        }

        public static String readTablespace(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            return ByteBufUtils.readString(buffer);
        }

        public static void readTransactions(Pdu pdu, Consumer<byte[]> consumer) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE
            );
            ByteBufUtils.skipArray(buffer); // tablespace

            int numRecords = buffer.readInt();
            for (int i = 0; i < numRecords; i++) {
                byte[] key = ByteBufUtils.readArray(buffer);
                consumer.accept(key);
            }
        }
    }

    public static class ListOfListsReader {

        private final Pdu pdu;
        private final int numLists;

        public ListOfListsReader(Pdu pdu, int numLists) {
            this.pdu = pdu;
            this.numLists = numLists;
        }

        public int getNumLists() {
            return numLists;
        }

        public ObjectListReader nextList() {
            // assuming that the readerIndex is not altered but other direct accesses to the ByteBuf
            int numValues = ByteBufUtils.readVInt(pdu.buffer);
            return new ObjectListReader(pdu, numValues);
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
            byteBuf.writeLong((Short) v);
        } else if (v instanceof byte[]) {
            byteBuf.writeByte(TYPE_BYTEARRAY);
            ByteBufUtils.writeArray(byteBuf, (byte[]) v);
        } else if (v instanceof Byte) {
            byteBuf.writeByte(TYPE_BYTE);
            byteBuf.writeByte((Byte) v);
        } else {
            throw new IllegalArgumentException("bad data type " + v.getClass());
        }

    }

    public static Object readObject(ByteBuf dii) {

        int type = ByteBufUtils.readVInt(dii);

        switch (type) {
            case TYPE_BYTEARRAY:
                return ByteBufUtils.readArray(dii);
            case TYPE_LONG:
                return dii.readLong();
            case TYPE_INTEGER:
                return dii.readInt();
            case TYPE_SHORT:
                return dii.readShort();
            case TYPE_BYTE:
                return dii.readByte();
            case TYPE_STRING:
                return ByteBufUtils.readUnpooledRawString(dii);
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
