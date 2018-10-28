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
import io.netty.util.ReferenceCountUtil;

/**
 * This represent a generic PDU in the protocol.
 *
 * @author enrico.olivelli
 */
public class Pdu implements AutoCloseable {

    public static final byte TYPE_ACK = 0;
    public static final byte TYPE_CLIENT_SHUTDOWN = 3;
    public static final byte TYPE_ERROR = 4;
    public static final byte TYPE_EXECUTE_STATEMENT = 5;
    public static final byte TYPE_EXECUTE_STATEMENT_RESULT = 6;
    public static final byte TYPE_OPENSCANNER = 7;
    public static final byte TYPE_RESULTSET_CHUNK = 8;
    public static final byte TYPE_CLOSESCANNER = 9;
    public static final byte TYPE_FETCHSCANNERDATA = 10;
    public static final byte TYPE_REQUEST_TABLESPACE_DUMP = 11;
    public static final byte TYPE_TABLESPACE_DUMP_DATA = 12;
    public static final byte TYPE_REQUEST_TABLE_RESTORE = 13;
    public static final byte TYPE_PUSH_TABLE_DATA = 14;
    public static final byte TYPE_EXECUTE_STATEMENTS = 15;
    public static final byte TYPE_EXECUTE_STATEMENTS_RESULT = 16;
    public static final byte TYPE_PUSH_TXLOGCHUNK = 17;
    public static final byte TYPE_TABLE_RESTORE_FINISHED = 19;
    public static final byte TYPE_PUSH_TRANSACTIONSBLOCK = 20;
    public static final byte TYPE_RESTORE_FINISHED = 23;
    public static final byte TYPE_TX_COMMAND = 24;
    public static final byte TYPE_SASL_TOKEN_MESSAGE_REQUEST = 100;
    public static final byte TYPE_SASL_TOKEN_SERVER_RESPONSE = 101;
    public static final byte TYPE_SASL_TOKEN_MESSAGE_TOKEN = 102;
    static final int OWN_SIZE = 1 + 1;

    public ByteBuf buffer;
    public byte type;
    public long messageId;

    @Override
    public void close() {
        ReferenceCountUtil.release(buffer);
        buffer = null;
    }

}
