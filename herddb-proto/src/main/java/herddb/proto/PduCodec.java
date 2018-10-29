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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

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
    private static final int REPLYID_SIZE = 8;
    private static final int MSGID_SIZE = 8;
    private static final int TYPE_SIZE = 1;
    private static final int FLAGS_SIZE = 1;
    private static final int VERSION_SIZE = 1;

    private static final int NULLABLE_FIELD_PRESENT = 1;
    private static final int NULLABLE_FIELD_ABSENT = 0;

    public static abstract class ExecuteStatementResult {

        public static long readUpdateCount(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + REPLYID_SIZE);
        }

        public static long readTx(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + REPLYID_SIZE
                    + ONE_LONG /* update count */);
        }

        public static ByteBuf write(
                long replyId, long updateCount, long tx) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + REPLYID_SIZE
                            + ONE_LONG
                            + ONE_LONG);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.TYPE_EXECUTE_STATEMENT_RESULT);
            byteBuf.writeLong(replyId);
            byteBuf.writeLong(updateCount);
            byteBuf.writeLong(tx);
            return byteBuf;
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

        public static ByteBuf write(long messageId, byte[] token, boolean request) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + FLAGS_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + 1 + (token != null ? token.length : 0));
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(request ? Pdu.FLAGS_ISREQUEST : Pdu.FLAGS_ISRESPONSE);
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
            ByteBufUtils.writeArray(byteBuf, token);
            return byteBuf;
        }

        public static byte[] readToken(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + FLAGS_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return ByteBufUtils.readArray(buffer);
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
            byteBuf.writeByte(notLeader ? 0 : 1);
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
            buffer.readerIndex(0);
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

}
