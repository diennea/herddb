
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
package herddb.network.netty;

import herddb.network.KeyValue;
import herddb.network.Message;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author enrico.olivelli
 */
public class MessageUtils {

    private static final byte VERSION = 'a';

    private static final byte OPCODE_REPLYMESSAGEID = 1;
    private static final byte OPCODE_WORKERPROCESSID = 2;
    private static final byte OPCODE_PARAMETERS = 3;

    private static final byte OPCODE_SET_VALUE = 6;
    private static final byte OPCODE_MAP_VALUE = 7;
    private static final byte OPCODE_STRING_VALUE = 8;
    private static final byte OPCODE_LONG_VALUE = 9;
    private static final byte OPCODE_INT_VALUE = 10;
    private static final byte OPCODE_NULL_VALUE = 11;
    private static final byte OPCODE_LIST_VALUE = 12;
    private static final byte OPCODE_BYTEARRAY_VALUE = 13;
    private static final byte OPCODE_TIMESTAMP_VALUE = 14;
    private static final byte OPCODE_BYTE_VALUE = 15;
    private static final byte OPCODE_KEYVALUE_VALUE = 16;
    private static final byte OPCODE_BOOLEAN_VALUE = 17;

    private static void writeUTF8String(ByteBuf buf, String s) {
        byte[] asarray = s.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(asarray.length);
        buf.writeBytes(asarray);
    }

    private static String readUTF8String(ByteBuf buf) {
        int len = buf.readInt();
        byte[] s = new byte[len];
        buf.readBytes(s);
        return new String(s, StandardCharsets.UTF_8);
    }

    public static void encodeMessage(ByteBuf encoded, Message m) {
        encoded.writeByte(VERSION);
        encoded.writeInt(m.type);
        writeUTF8String(encoded, m.messageId);
        if (m.replyMessageId != null) {
            encoded.writeByte(OPCODE_REPLYMESSAGEID);
            writeUTF8String(encoded, m.replyMessageId);
        }
        if (m.clientId != null) {
            encoded.writeByte(OPCODE_WORKERPROCESSID);
            writeUTF8String(encoded, m.clientId);
        }
        if (m.parameters != null) {
            encoded.writeByte(OPCODE_PARAMETERS);
            encoded.writeInt(m.parameters.size());
            for (Map.Entry<String, Object> p : m.parameters.entrySet()) {
                writeEncodedSimpleValue(encoded, p.getKey());
                writeEncodedSimpleValue(encoded, p.getValue());
            }
        }

    }

    private static void writeEncodedSimpleValue(ByteBuf encoded, Object o) {
        if (o == null) {
            encoded.writeByte(OPCODE_NULL_VALUE);
        } else if (o instanceof String) {
            encoded.writeByte(OPCODE_STRING_VALUE);
            writeUTF8String(encoded, (String) o);
        } else if (o instanceof java.sql.Timestamp) {
            encoded.writeByte(OPCODE_TIMESTAMP_VALUE);
            encoded.writeLong(((java.sql.Timestamp) o).getTime());
        } else if (o instanceof java.lang.Byte) {
            encoded.writeByte(OPCODE_BYTE_VALUE);
            encoded.writeByte(((Byte) o));
        } else if (o instanceof KeyValue) {
            KeyValue kv = (KeyValue) o;
            encoded.writeByte(OPCODE_KEYVALUE_VALUE);
            encoded.writeInt(kv.key.length);
            encoded.writeBytes(kv.key);
            encoded.writeInt(kv.value.length);
            encoded.writeBytes(kv.value);
        } else if (o instanceof Integer) {
            encoded.writeByte(OPCODE_INT_VALUE);
            encoded.writeInt((Integer) o);
        } else if (o instanceof Boolean) {
            encoded.writeByte(OPCODE_BOOLEAN_VALUE);
            encoded.writeByte(((Boolean) o).booleanValue() ? 1 : 0);
        } else if (o instanceof Long) {
            encoded.writeByte(OPCODE_LONG_VALUE);
            encoded.writeLong((Long) o);
        } else if (o instanceof Set) {
            Set set = (Set) o;
            encoded.writeByte(OPCODE_SET_VALUE);
            encoded.writeInt(set.size());
            for (Object o2 : set) {
                writeEncodedSimpleValue(encoded, o2);
            }
        } else if (o instanceof List) {
            List set = (List) o;
            encoded.writeByte(OPCODE_LIST_VALUE);
            encoded.writeInt(set.size());
            for (Object o2 : set) {
                writeEncodedSimpleValue(encoded, o2);
            }

        } else if (o instanceof byte[]) {
            byte[] set = (byte[]) o;
            encoded.writeByte(OPCODE_BYTEARRAY_VALUE);
            encoded.writeInt(set.length);
            encoded.writeBytes(set);
        } else if (o instanceof Map) {
            Map set = (Map) o;
            encoded.writeByte(OPCODE_MAP_VALUE);
            encoded.writeInt(set.size());
            for (Map.Entry entry : (Iterable<Entry>) set.entrySet()) {
                writeEncodedSimpleValue(encoded, entry.getKey());
                writeEncodedSimpleValue(encoded, entry.getValue());
            }
        } else {
            throw new RuntimeException("unsupported class " + o.getClass());
        }
    }

    private static Object readEncodedSimpleValue(ByteBuf encoded) {
        byte _opcode = encoded.readByte();
        switch (_opcode) {
            case OPCODE_NULL_VALUE:
                return null;
            case OPCODE_STRING_VALUE:
                return readUTF8String(encoded);
            case OPCODE_INT_VALUE:
                return encoded.readInt();
            case OPCODE_BOOLEAN_VALUE:
                return encoded.readByte() == 1 ? true : false;
            case OPCODE_MAP_VALUE: {
                int len = encoded.readInt();
                Map<Object, Object> ret = new HashMap<>();
                for (int i = 0; i < len; i++) {
                    Object mapkey = readEncodedSimpleValue(encoded);
                    Object value = readEncodedSimpleValue(encoded);
                    ret.put(mapkey, value);
                }
                return ret;
            }
            case OPCODE_SET_VALUE: {
                int len = encoded.readInt();
                Set<Object> ret = new HashSet<>();
                for (int i = 0; i < len; i++) {
                    Object o = readEncodedSimpleValue(encoded);
                    ret.add(o);
                }
                return ret;
            }
            case OPCODE_LIST_VALUE: {
                int len = encoded.readInt();
                List<Object> ret = new ArrayList<>(len);
                for (int i = 0; i < len; i++) {
                    Object o = readEncodedSimpleValue(encoded);
                    ret.add(o);
                }
                return ret;
            }
            case OPCODE_BYTEARRAY_VALUE: {
                int len = encoded.readInt();
                byte[] ret = new byte[len];
                encoded.readBytes(ret);
                return ret;
            }
            case OPCODE_LONG_VALUE:
                return encoded.readLong();
            case OPCODE_TIMESTAMP_VALUE:
                return new java.sql.Timestamp(encoded.readLong());
            case OPCODE_BYTE_VALUE:
                return encoded.readByte();
            case OPCODE_KEYVALUE_VALUE:
                int len_key = encoded.readInt();
                byte[] key = new byte[len_key];
                encoded.readBytes(key);
                int len_value = encoded.readInt();
                byte[] value = new byte[len_value];
                encoded.readBytes(value);
                return new KeyValue(key, value);
            default:
                throw new RuntimeException("invalid opcode: " + _opcode);
        }
    }

    public static Message decodeMessage(ByteBuf encoded) {
        byte version = encoded.readByte();
        if (version != VERSION) {
            throw new RuntimeException("bad protocol version " + version);
        }
        int type = encoded.readInt();
        String messageId = readUTF8String(encoded);
        String replyMessageId = null;
        String workerProcessId = null;
        Map<String, Object> params = new HashMap<>();
        while (encoded.isReadable()) {
            byte opcode = encoded.readByte();
            switch (opcode) {
                case OPCODE_REPLYMESSAGEID:
                    replyMessageId = readUTF8String(encoded);
                    break;
                case OPCODE_WORKERPROCESSID:
                    workerProcessId = readUTF8String(encoded);
                    break;
                case OPCODE_PARAMETERS:
                    int size = encoded.readInt();
                    for (int i = 0; i < size; i++) {
                        Object key = readEncodedSimpleValue(encoded);
                        Object value = readEncodedSimpleValue(encoded);
                        params.put((String) key, value);
                    }
                    break;
                default:
                    throw new RuntimeException("invalid opcode: " + opcode);
            }
        }
        Message m = new Message(workerProcessId, type, params);
        if (replyMessageId != null) {
            m.replyMessageId = replyMessageId;
        }
        m.messageId = messageId;
        return m;

    }

}
