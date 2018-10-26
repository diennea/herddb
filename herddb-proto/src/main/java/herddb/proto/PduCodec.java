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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;

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
            int pos = 1;
            byte type = in.getByte(pos++);
            switch (type) {
                case Pdu.TYPE_EXECUTE_STATEMENT_RESULT:
                    return readExecuteStatementResultV3(in, pos);
                default: {
                    ReferenceCountUtil.release(in);
                    throw new IOException("Cannot decode version " + version + " type " + type);
                }
            }

        } else {
            ReferenceCountUtil.release(in);
            throw new IOException("Cannot decode version " + version);
        }
    }

    public static ByteBuf encodePdu(ByteBufAllocator alloc, Pdu pdu) throws IOException {
        switch (pdu.type) {
            case Pdu.TYPE_EXECUTE_STATEMENT_RESULT:
                return writeExecuteStatementResultV3(alloc, pdu);
            default:
                throw new IOException("Cannot encode type " + pdu.type);
        }

    }

    private static Pdu readExecuteStatementResultV3(ByteBuf in, int pos) {
        ExecuteStatementResult res = new ExecuteStatementResult();
        res.buffer = in;
        res.replyId = in.getLong(pos += 8);
        res.tx = in.getLong(pos += 8);
        res.updateCount = in.getLong(pos += 8);
        return res;
    }

    private static ByteBuf writeExecuteStatementResultV3(ByteBufAllocator alloc, Pdu pdu) {
        ByteBuf byteBuf = alloc.directBuffer(Pdu.OWN_SIZE + Response.OWN_SIZE + ExecuteStatementResult.OWN_SIZE);
        ExecuteStatementResult res = (ExecuteStatementResult) pdu;
        writeStandardHeaderResponse(res, byteBuf);
        byteBuf.writeLong(res.tx);
        byteBuf.writeLong(res.updateCount);
        return byteBuf;
    }

    private static void writeStandardHeader(Pdu pdu, ByteBuf byteBuf) {
        byteBuf.writeByte(VERSION_3);
        byteBuf.writeByte(pdu.type);
    }

    private static void writeStandardHeaderResponse(Response response, ByteBuf byteBuf) {
        writeStandardHeader(response, byteBuf);
        byteBuf.writeLong(response.replyId);
    }
}
