
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
package herddb.utils;

import herddb.proto.flatbuf.BooleanValue;
import herddb.proto.flatbuf.DoubleValue;
import herddb.proto.flatbuf.IntValue;
import herddb.proto.flatbuf.TimestampValue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import herddb.proto.flatbuf.AnyValue;
import herddb.proto.flatbuf.AnyValueList;
import herddb.proto.flatbuf.AnyValueWrapper;
import herddb.proto.flatbuf.LongValue;
import herddb.proto.flatbuf.MapEntry;
import herddb.proto.flatbuf.StringValue;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Collections;

/**
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class MessageUtils {

    private static final byte VERSION = 'a';

    private static final byte OPCODE_REPLYMESSAGEID = 1;
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
    private static final byte OPCODE_DOUBLE_VALUE = 22;
    private static final byte OPCODE_MAP2_VALUE = 23;
    private static final byte OPCODE_MAP2_VALUE_END = 24;
    private static final byte OPCODE_TUPLELIST_VALUE = 25;

    /**
     * When writing int <b>greater than this</b> value are better written
     * directly as int because in vint encoding will use at least 4 bytes
     */
    private static final int WRITE_MAX_V_INT_LIMIT = -1 >>> 11;

    /**
     * When writing negative int <b>smaller than this</b> value are better
     * written directly as int because in zint encoding will use at least 8
     * bytes
     */
    private static final int WRITE_MIN_Z_INT_LIMIT = -1 << 20;

    /**
     * When writing long <b>greater than this</b> value are better written
     * directly as long because in vint encoding will use at least 4 bytes
     */
    private static final long WRITE_MAX_V_LONG_LIMIT = -1L >>> 15;

    /**
     * When writing negative long <b>smaller than this</b> value are better
     * written directly as long because in zlong encoding will use at least 8
     * bytes
     */
    private static final long WRITE_MIN_Z_LONG_LIMIT = -1L << 48;

    private static void writeUTF8String(ByteBuf buffer, String s) {
        byte[] array = s.getBytes(StandardCharsets.UTF_8);
        ByteBufUtils.writeArray(buffer, array);
    }

    private static void writeUTF8String(ByteBuf buffer, RawString s) {
        ByteBufUtils.writeArray(buffer, s.data);
    }

    private static RawString readUTF8String(ByteBuf buffer) {
        byte[] array = ByteBufUtils.readArray(buffer);
        return new RawString(array);
    }

    @SuppressWarnings("rawtypes")
    private static void writeEncodedSimpleValue(ByteBuf output, Object o) {
        if (o == null) {
            output.writeByte(OPCODE_NULL_VALUE);
        } else if (o instanceof String) {
            output.writeByte(OPCODE_STRING_VALUE);
            writeUTF8String(output, (String) o);
        } else if (o instanceof RawString) {
            output.writeByte(OPCODE_STRING_VALUE);
            writeUTF8String(output, (RawString) o);
        } else if (o instanceof java.sql.Timestamp) {
            output.writeByte(OPCODE_TIMESTAMP_VALUE);
            ByteBufUtils.writeVLong(output, ((java.sql.Timestamp) o).getTime());
        } else if (o instanceof java.lang.Byte) {
            output.writeByte(OPCODE_BYTE_VALUE);
            output.writeByte(((Byte) o));
        } else if (o instanceof KeyValue) {
            KeyValue kv = (KeyValue) o;
            output.writeByte(OPCODE_KEYVALUE_VALUE);
            ByteBufUtils.writeArray(output, kv.key);
            ByteBufUtils.writeArray(output, kv.value);

        } else if (o instanceof Integer) {

            int i = (int) o;

            if (i < 0) {
                if (i < WRITE_MIN_Z_INT_LIMIT) {
                    output.writeByte(OPCODE_INT_VALUE);
                    output.writeInt(i);
                } else {
                    output.writeByte(OPCODE_Z_INT_VALUE);
                    ByteBufUtils.writeZInt(output, i);
                }
            } else {
                if (i > WRITE_MAX_V_INT_LIMIT) {
                    output.writeByte(OPCODE_INT_VALUE);
                    output.writeInt(i);
                } else {
                    output.writeByte(OPCODE_V_INT_VALUE);
                    ByteBufUtils.writeVInt(output, i);
                }
            }

        } else if (o instanceof Long) {

            long l = (long) o;

            if (l < 0) {
                if (l < WRITE_MIN_Z_LONG_LIMIT) {
                    output.writeByte(OPCODE_LONG_VALUE);
                    output.writeLong(l);
                } else {
                    output.writeByte(OPCODE_Z_LONG_VALUE);
                    ByteBufUtils.writeZLong(output, l);
                }
            } else {
                if (l > WRITE_MAX_V_LONG_LIMIT) {
                    output.writeByte(OPCODE_LONG_VALUE);
                    output.writeLong(l);
                } else {
                    output.writeByte(OPCODE_V_LONG_VALUE);
                    ByteBufUtils.writeVLong(output, l);
                }
            }

        } else if (o instanceof Boolean) {
            output.writeByte(OPCODE_BOOLEAN_VALUE);
            output.writeByte(((Boolean) o).booleanValue() ? 1 : 0);
        } else if (o instanceof Double) {
            output.writeByte(OPCODE_DOUBLE_VALUE);
            ByteBufUtils.writeDouble(output, ((Double) o).doubleValue());
        } else if (o instanceof Set) {
            Set set = (Set) o;
            output.writeByte(OPCODE_SET_VALUE);
            ByteBufUtils.writeVInt(output, set.size());
            for (Object o2 : set) {
                writeEncodedSimpleValue(output, o2);
            }
        } else if (o instanceof List) {
            List set = (List) o;
            output.writeByte(OPCODE_LIST_VALUE);
            ByteBufUtils.writeVInt(output, set.size());
            for (Object o2 : set) {
                writeEncodedSimpleValue(output, o2);
            }
        } else if (o instanceof Map) {
            Map set = (Map) o;
            output.writeByte(OPCODE_MAP_VALUE);
            ByteBufUtils.writeVInt(output, set.size());
            for (Map.Entry entry : (Iterable<Map.Entry>) set.entrySet()) {
                writeEncodedSimpleValue(output, entry.getKey());
                writeEncodedSimpleValue(output, entry.getValue());
            }
        } else if (o instanceof DataAccessor) {
            DataAccessor set = (DataAccessor) o;
            output.writeByte(OPCODE_MAP2_VALUE);
            // number of entries is not known
            set.forEach((key, value) -> {
                writeEncodedSimpleValue(output, key);
                writeEncodedSimpleValue(output, value);
            });
            output.writeByte(OPCODE_MAP2_VALUE_END);
        } else if (o instanceof TuplesList) {
            TuplesList set = (TuplesList) o;
            output.writeByte(OPCODE_TUPLELIST_VALUE);
            final int numColumns = set.columnNames.length;
            ByteBufUtils.writeVInt(output, numColumns);
            for (String columnName : set.columnNames) {
                writeUTF8String(output, columnName);
            }
            ByteBufUtils.writeVInt(output, set.tuples.size());
            for (DataAccessor da : set.tuples) {
                IntHolder currentColumn = new IntHolder();
                da.forEach((String key, Object value) -> {
                    String expectedColumnName = set.columnNames[currentColumn.value];
                    while (!key.equals(expectedColumnName)) {
                        // nulls are not returned for some special accessors, lie DataAccessorForFullRecord
                        writeEncodedSimpleValue(output, null);
                        currentColumn.value++;
                        expectedColumnName = set.columnNames[currentColumn.value];
                    }
                    writeEncodedSimpleValue(output, value);
                    currentColumn.value++;
                });

                // fill with nulls
                while (currentColumn.value < numColumns) {
                    writeEncodedSimpleValue(output, null);
                    currentColumn.value++;
                }
                if (currentColumn.value > numColumns) {
                    throw new RuntimeException("unexpected number of columns " + currentColumn.value + " > " + numColumns);
                }
            }
        } else if (o instanceof byte[]) {
            byte[] set = (byte[]) o;
            output.writeByte(OPCODE_BYTEARRAY_VALUE);
            ByteBufUtils.writeArray(output, set);
        } else {
            throw new RuntimeException("unsupported class " + o.getClass());
        }
    }

    public static Object readEncodedSimpleValue(ByteBuf encoded) {
        byte _opcode = encoded.readByte();
        return readEncodedSimpleValue(_opcode, encoded);
    }

    private static Object readEncodedSimpleValue(byte _opcode, ByteBuf encoded) {
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
                return encoded.readByte() == 1;
            case OPCODE_DOUBLE_VALUE:
                return ByteBufUtils.readDouble(encoded);
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
//            case OPCODE_TUPLELIST_VALUE: {
//                int numColumns = ByteBufUtils.readVInt(encoded);
//                String[] columns = new String[numColumns];
//                for (int i = 0; i < numColumns; i++) {
//                    columns[i] = readUTF8String(encoded).toString();
//                }
//                int numRecords = ByteBufUtils.readVInt(encoded);
//                ByteBuf slice = encoded.retainedSlice();
//                RecordsBatch recordsBatch = new RecordsBatch(numRecords, columns, slice);
//
//                return recordsBatch;
//            }
            case OPCODE_MAP2_VALUE: {
                Map<Object, Object> ret = new HashMap<>();
                while (true) {
                    byte sniff_opcode = encoded.readByte();
                    if (sniff_opcode == OPCODE_MAP2_VALUE_END) {
                        return ret;
                    }
                    Object mapkey = readEncodedSimpleValue(sniff_opcode, encoded);
                    Object value = readEncodedSimpleValue(encoded);
                    ret.put(mapkey, value);
                }
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

    public static byte[] bufferToArray(ByteBuffer data) {
        byte[] arr = new byte[data.remaining()];
        data.get(arr);
        return arr;
    }

    public static RawString readRawString(ByteBuffer data) {
        return new RawString(bufferToArray(data));
    }

    public static String readString(ByteBuffer data) {
        return new String(bufferToArray(data), StandardCharsets.UTF_8);
    }

    public static List<Object> decodeAnyValueList(AnyValueList params) {
        List<Object> res = new ArrayList<>();
        if (params != null) {
            for (int i = 0; i < params.itemsLength(); i++) {
                Object value = decodeObject(params.items(i));
                res.add(value);
            }
        }
        return res;
    }

    public static Object decodeObject(AnyValueWrapper item) throws IllegalArgumentException {
        Object value = null;
        switch (item.valueType()) {
            case AnyValue.NullValue:
                value = null;
                break;
            case AnyValue.BooleanValue:
                value = ((BooleanValue) item.value(new BooleanValue())).value();
                break;
            case AnyValue.LongValue:
                value = ((LongValue) item.value(new LongValue())).value();
                break;
            case AnyValue.IntValue:
                value = ((IntValue) item.value(new IntValue())).value();
                break;
            case AnyValue.DoubleValue:
                value = Double.longBitsToDouble(((DoubleValue) item.value(new DoubleValue())).value());
                break;
            case AnyValue.TimestampValue:
                value = new java.sql.Timestamp(((TimestampValue) item.value(new TimestampValue())).value());
                break;
            case AnyValue.StringValue:
                value = readRawString(((StringValue) item.value(new StringValue())).valueAsByteBuffer());
                break;
            default:
                throw new IllegalArgumentException("bad jdbc param type " + item.valueType());
        }
        return value;
    }

    public static Map<RawString, Object> decodeMap(herddb.proto.flatbuf.Map newValue) {
        if (newValue == null) {
            return Collections.emptyMap();
        }
        Map<RawString, Object> res = new HashMap<>();
        int size = newValue.entriesLength();
        for (int i = 0; i < size; i++) {
            MapEntry entry = newValue.entries(i);
            String key = entry.key();
            Object o = decodeObject(entry.value());
            res.put(RawString.of(key), o);
        }
        return res;
    }

}
