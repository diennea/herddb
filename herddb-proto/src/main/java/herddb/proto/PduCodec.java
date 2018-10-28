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

import herddb.proto.flatbuf.MessageType;
import herddb.proto.flatbuf.Request;
import herddb.proto.flatbuf.Response;
import herddb.utils.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
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
            byte type = in.getByte(0);
            long messageId = in.getLong(1);
            Pdu pdu = new Pdu();
            pdu.buffer = in;
            pdu.type = type;
            pdu.messageId = messageId;
            return pdu;
        } else {
            ReferenceCountUtil.release(in);
            throw new IOException("Cannot decode version " + version);
        }
    }

    private static final int ONE_LONG = 8;
    private static final int REPLYID_SIZE = 8;
    private static final int MSGID_SIZE = 8;
    private static final int TYPE_SIZE = 1;
    private static final int VERSION_SIZE = 1;

    public static abstract class ExecuteStatementResult {

        public static long readUpdateCount(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                    + TYPE_SIZE
                    + REPLYID_SIZE);
        }

        public static long readTx(Pdu pdu) {
            return pdu.buffer.getLong(
                    VERSION_SIZE
                    + TYPE_SIZE
                    + REPLYID_SIZE
                    + ONE_LONG /* update count */);
        }

        public static ByteBuf write(
                long replyId, long updateCount, long tx) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
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
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + 64);
            byteBuf.writeByte(VERSION_3);
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
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return new String(ByteBufUtils.readArray(buffer), StandardCharsets.UTF_8);
        }
        
        public static byte[] readToken(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return ByteBufUtils.readArray(buffer);
        }
    }

    public static class SaslTokenServerResponse {

        public static ByteBuf write(long messageId, byte[] token) {
            ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT
                    .directBuffer(
                            VERSION_SIZE
                            + TYPE_SIZE
                            + MSGID_SIZE
                            + 64);
            byteBuf.writeByte(VERSION_3);
            byteBuf.writeByte(Pdu.TYPE_SASL_TOKEN_SERVER_RESPONSE);
            byteBuf.writeLong(messageId);
            ByteBufUtils.writeArray(byteBuf, token);
            return byteBuf;
        }

        public static byte[] readToken(Pdu pdu) {
            ByteBuf buffer = pdu.buffer;
            buffer.readerIndex(0);
            buffer.skipBytes(VERSION_SIZE
                    + TYPE_SIZE
                    + MSGID_SIZE);
            return ByteBufUtils.readArray(buffer);
        }

    }

//    
//    public static ByteBuf SASL_TOKEN_MESSAGE_REQUEST(long id, String saslMech, byte[] firstToken) {
//        try (ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();) {
//            int mechOffset = addString(builder, saslMech);
//            int tokenOffset = builder.createByteVector(firstToken);
//            Request.startRequest(builder);
//            Request.addId(builder, id);
//            Request.addMech(builder, mechOffset);
//            Request.addToken(builder, tokenOffset);
//            Request.addType(builder, MessageType.TYPE_SASL_TOKEN_MESSAGE_REQUEST);
//            return finishAsRequest(builder);
//        }
//    }
//
//    public static ByteBuf SASL_TOKEN_SERVER_RESPONSE(long replyId, byte[] saslTokenChallenge) {
//        try (ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();) {
//            int tokenOffset = -1;
//            if (saslTokenChallenge != null) {
//                tokenOffset = builder.createByteVector(saslTokenChallenge);
//            }
//            Response.startResponse(builder);
//            Response.addReplyMessageId(builder, replyId);
//            if (tokenOffset >= 0) {
//                Response.addToken(builder, tokenOffset);
//            }
//            Response.addType(builder, MessageType.TYPE_SASL_TOKEN_SERVER_RESPONSE);
//            return finishAsResponse(builder);
//        }
//    }
//
//    public static ByteBuf SASL_TOKEN_MESSAGE_TOKEN(long id, byte[] token) {
//        try (ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();) {
//            int tokenOffset = 0;
//            if (token != null) {
//                tokenOffset = builder.createByteVector(token);
//            }
//            Request.startRequest(builder);
//            Request.addId(builder, id);
//            if (token != null) {
//                Request.addToken(builder, tokenOffset);
//            }
//            Request.addType(builder, MessageType.TYPE_SASL_TOKEN_MESSAGE_TOKEN);
//            return finishAsRequest(builder);
//        }
//    }
}
