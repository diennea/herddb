
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import herddb.network.KeyValue;
import herddb.network.Message;
import herddb.utils.ByteBufUtils;
import io.netty.buffer.ByteBuf;

/**
 *
 * @author enrico.olivelli
 * @author diego.salvi
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
    private static final byte OPCODE_V_LONG_VALUE = 10;
    private static final byte OPCODE_Z_LONG_VALUE = 11;
    private static final byte OPCODE_INT_VALUE = 12;
    private static final byte OPCODE_V_INT_VALUE = 13;
    private static final byte OPCODE_Z_INT_VALUE = 14;
    private static final byte OPCODE_NULL_VALUE = 15;
    private static final byte OPCODE_LIST_VALUE = 16;
    private static final byte OPCODE_BYTEARRAY_VALUE = 17;
    private static final byte OPCODE_TIMESTAMP_VALUE = 18;
    private static final byte OPCODE_BYTE_VALUE = 19;
    private static final byte OPCODE_KEYVALUE_VALUE = 20;
    private static final byte OPCODE_BOOLEAN_VALUE = 21;

    /**
     * When writing int <b>greater than this</b> value are better written directly as int because in vint
     * encoding will use at least 4 bytes
     */
    private static final int WRITE_MAX_V_INT_LIMIT = -1 >>> 11;

    /**
     * When writing negative int <b>smaller than this</b> value are better written directly as int because in
     * zint encoding will use at least 8 bytes
     */
    private static final int WRITE_MIN_Z_INT_LIMIT = -1 << 20;

    /**
     * When writing long <b>greater than this</b> value are better written directly as long because in vint
     * encoding will use at least 4 bytes
     */
    private static final long WRITE_MAX_V_LONG_LIMIT = -1L >>> 15;

    /**
     * When writing negative long <b>smaller than this</b> value are better written directly as long because
     * in zlong encoding will use at least 8 bytes
     */
    private static final long WRITE_MIN_Z_LONG_LIMIT = -1L << 48;
    
    public static void encodeMessage(ByteBuf buffer, Message m) {
        
        buffer.writeByte(VERSION);
        ByteBufUtils.writeVInt(buffer, m.type);
        writeUTF8String(buffer, m.messageId);
        if (m.replyMessageId != null) {
            buffer.writeByte(OPCODE_REPLYMESSAGEID);
            writeUTF8String(buffer, m.replyMessageId);
        }
        if (m.clientId != null) {
            buffer.writeByte(OPCODE_WORKERPROCESSID);
            writeUTF8String(buffer, m.clientId);
        }
        if (m.parameters != null) {
            buffer.writeByte(OPCODE_PARAMETERS);
            ByteBufUtils.writeVInt(buffer, m.parameters.size());
            for (Map.Entry<String, Object> p : m.parameters.entrySet()) {
                writeEncodedSimpleValue(buffer, p.getKey());
                writeEncodedSimpleValue(buffer, p.getValue());
            }
        }
    }
    
    public static Message decodeMessage(ByteBuf encoded) {
        byte version = encoded.readByte();
        if (version != VERSION) {
            throw new RuntimeException("bad protocol version " + version);
        }
        
        int type = ByteBufUtils.readVInt(encoded);
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
                    int size = ByteBufUtils.readVInt(encoded);
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

    private static void writeUTF8String(ByteBuf buffer, String s) {
        byte[] array = s.getBytes(StandardCharsets.UTF_8);
        ByteBufUtils.writeArray(buffer, array);
    }

    private static String readUTF8String(ByteBuf buffer) {
        byte[] array = ByteBufUtils.readArray(buffer);
        return new String(array, StandardCharsets.UTF_8);
    }
    
    @SuppressWarnings("rawtypes")
    private static void writeEncodedSimpleValue(ByteBuf encoded, Object o) {
        if (o == null) {
            encoded.writeByte(OPCODE_NULL_VALUE);
        } else if (o instanceof String) {
            encoded.writeByte(OPCODE_STRING_VALUE);
            writeUTF8String(encoded, (String) o);
        } else if (o instanceof java.sql.Timestamp) {
            encoded.writeByte(OPCODE_TIMESTAMP_VALUE);
            ByteBufUtils.writeVLong(encoded, ((java.sql.Timestamp) o).getTime());
        } else if (o instanceof java.lang.Byte) {
            encoded.writeByte(OPCODE_BYTE_VALUE);
            encoded.writeByte(((Byte) o));
        } else if (o instanceof KeyValue) {
            KeyValue kv = (KeyValue) o;
            encoded.writeByte(OPCODE_KEYVALUE_VALUE);
            ByteBufUtils.writeArray(encoded, kv.key);
            ByteBufUtils.writeArray(encoded, kv.value);
            
        } else if (o instanceof Integer) {
            
            int i = (int) o;
            
            if ( i < 0 ) {
                if (i < WRITE_MIN_Z_INT_LIMIT) {
                    encoded.writeByte(OPCODE_INT_VALUE);
                    encoded.writeInt(i);
                } else {
                    encoded.writeByte(OPCODE_Z_INT_VALUE);
                    ByteBufUtils.writeZInt(encoded, i);
                }
            } else {
                if ( i > WRITE_MAX_V_INT_LIMIT ) {
                    encoded.writeByte(OPCODE_INT_VALUE);
                    encoded.writeInt(i);
                } else {
                    encoded.writeByte(OPCODE_V_INT_VALUE);
                    ByteBufUtils.writeVInt(encoded, i);
                }
            }
            
        } else if (o instanceof Long) {
            
            long l = (long) o;
            
            if ( l < 0 ) {
                if (l < WRITE_MIN_Z_LONG_LIMIT) {
                    encoded.writeByte(OPCODE_LONG_VALUE);
                    encoded.writeLong(l);
                } else {
                    encoded.writeByte(OPCODE_Z_LONG_VALUE);
                    ByteBufUtils.writeZLong(encoded, l);
                }
            } else {
                if ( l > WRITE_MAX_V_LONG_LIMIT ) {
                    encoded.writeByte(OPCODE_LONG_VALUE);
                    encoded.writeLong(l);
                } else {
                    encoded.writeByte(OPCODE_V_LONG_VALUE);
                    ByteBufUtils.writeVLong(encoded, l);
                }
            }
            
        } else if (o instanceof Boolean) {
            encoded.writeByte(OPCODE_BOOLEAN_VALUE);
            encoded.writeByte(((Boolean) o).booleanValue() ? 1 : 0);
        } else if (o instanceof Set) {
            Set set = (Set) o;
            encoded.writeByte(OPCODE_SET_VALUE);
            ByteBufUtils.writeVInt(encoded, set.size());
            for (Object o2 : set) {
                writeEncodedSimpleValue(encoded, o2);
            }
        } else if (o instanceof List) {
            List set = (List) o;
            encoded.writeByte(OPCODE_LIST_VALUE);
            ByteBufUtils.writeVInt(encoded, set.size());
            for (Object o2 : set) {
                writeEncodedSimpleValue(encoded, o2);
            }
        } else if (o instanceof Map) {
            Map set = (Map) o;
            encoded.writeByte(OPCODE_MAP_VALUE);
            ByteBufUtils.writeVInt(encoded, set.size());
            for (Map.Entry entry : (Iterable<Map.Entry>) set.entrySet()) {
                writeEncodedSimpleValue(encoded, entry.getKey());
                writeEncodedSimpleValue(encoded, entry.getValue());
            }
        } else if (o instanceof byte[]) {
            byte[] set = (byte[]) o;
            encoded.writeByte(OPCODE_BYTEARRAY_VALUE);
            ByteBufUtils.writeArray(encoded, set);
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
            case OPCODE_V_INT_VALUE:
                return ByteBufUtils.readVInt(encoded);
            case OPCODE_Z_INT_VALUE:
                return ByteBufUtils.readZInt(encoded);

            case OPCODE_LONG_VALUE:
                return encoded.readLong();
            case OPCODE_V_LONG_VALUE:
                return ByteBufUtils.readVLong(encoded);
            case OPCODE_Z_LONG_VALUE:
                return ByteBufUtils.readZLong(encoded);
                
            case OPCODE_BOOLEAN_VALUE:
                return encoded.readByte() == 1 ? true : false;
                
            case OPCODE_MAP_VALUE: {
                int len = ByteBufUtils.readVInt(encoded);
                Map<Object, Object> ret = new HashMap<>();
                for (int i = 0; i < len; i++) {
                    Object mapkey = readEncodedSimpleValue(encoded);
                    Object value = readEncodedSimpleValue(encoded);
                    ret.put(mapkey, value);
                }
                return ret;
            }
            case OPCODE_SET_VALUE: {
                int len = ByteBufUtils.readVInt(encoded);
                Set<Object> ret = new HashSet<>();
                for (int i = 0; i < len; i++) {
                    Object o = readEncodedSimpleValue(encoded);
                    ret.add(o);
                }
                return ret;
            }
            case OPCODE_LIST_VALUE: {
                int len = ByteBufUtils.readVInt(encoded);
                List<Object> ret = new ArrayList<>(len);
                for (int i = 0; i < len; i++) {
                    Object o = readEncodedSimpleValue(encoded);
                    ret.add(o);
                }
                return ret;
            }
            case OPCODE_BYTEARRAY_VALUE: {
                return ByteBufUtils.readArray(encoded);
            }
                
            case OPCODE_TIMESTAMP_VALUE:
                return new java.sql.Timestamp(ByteBufUtils.readVLong(encoded));
            case OPCODE_BYTE_VALUE:
                return encoded.readByte();
            case OPCODE_KEYVALUE_VALUE:
                byte[] key = ByteBufUtils.readArray(encoded);
                byte[] value = ByteBufUtils.readArray(encoded);
                return new KeyValue(key, value);
            default:
                throw new RuntimeException("invalid opcode: " + _opcode);
        }
    }

}
