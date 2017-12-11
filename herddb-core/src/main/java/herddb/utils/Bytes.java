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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.internal.PlatformDependent;
import java.nio.ByteOrder;

/**
 * A wrapper for byte[], in order to use it as keys on HashMaps
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
public final class Bytes implements Comparable<Bytes>, SizeAwareObject {

    private static final boolean UNALIGNED = PlatformDependent.isUnaligned();
    private static final boolean HAS_UNSAFE = PlatformDependent.hasUnsafe();
    private static final boolean BIG_ENDIAN_NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    /**
     * <pre>
     * herddb.utils.Bytes object internals:
     *  OFFSET  SIZE               TYPE DESCRIPTION                               VALUE
     *       0    12                    (object header)                           N/A
     *      12     4                int Bytes.hashCode                            N/A
     *      16     4             byte[] Bytes.data                                N/A
     *      20     4   java.lang.Object Bytes.deserialized                        N/A
     * Instance size: 24 bytes
     * Space losses: 0 bytes internal + 0 bytes external = 0 bytes total
     * </pre>
     */
    private static final int CONSTANT_BYTE_SIZE = 24;

    public static final long estimateSize(byte[] value) {
        return value.length + CONSTANT_BYTE_SIZE;
    }

    public final byte[] data;
    private final int hashCode;

    public Object deserialized;

    @Override
    public long getEstimatedSize() {
        return data.length + CONSTANT_BYTE_SIZE;
    }

    public static byte[] string_to_array(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static Bytes from_string(String s) {
        return new Bytes(s.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] longToByteArray(long value) {
        byte[] res = new byte[8];
        putLong(res, 0, value);
        return res;
    }

    public static Bytes from_long(long value) {
        byte[] res = new byte[8];
        putLong(res, 0, value);
        return new Bytes(res);
    }

    public static Bytes from_array(byte[] data) {
        return new Bytes(data);
    }

    public byte[] to_array() {
        return data;
    }

    public static Bytes from_int(int value) {
        byte[] res = new byte[4];
        putInt(res, 0, value);
        return new Bytes(res);
    }

    public static Bytes from_timestamp(java.sql.Timestamp value) {
        byte[] res = new byte[8];
        putLong(res, 0, value.getTime());
        return new Bytes(res);
    }

    public static Bytes from_boolean(boolean value) {
        byte[] res = new byte[1];
        putBoolean(res, 0, value);
        return new Bytes(res);
    }

    public static Bytes from_double(double value) {
        byte[] res = new byte[8];
        putDouble(res, 0, value);
        return new Bytes(res);
    }

    public long to_long() {
        return toLong(data, 0);
    }

    public int to_int() {
        return toInt(data, 0);
    }

    public String to_string() {
        return new String(data, 0, data.length, StandardCharsets.UTF_8);
    }

    public static String to_string(byte[] data) {
        return new String(data, 0, data.length, StandardCharsets.UTF_8);
    }

    public static RawString to_rawstring(byte[] data) {
        return new RawString(data);
    }

    public java.sql.Timestamp to_timestamp() {
        return toTimestamp(data, 0, 8);
    }

    public boolean to_boolean() {
        return toBoolean(data, 0);
    }

    public double to_double() {
        return toDouble(data, 0);
    }

    public Bytes(byte[] data) {
        this.data = data;
        this.hashCode = Arrays.hashCode(this.data);;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        try {
            final Bytes other = (Bytes) obj;
            if (other.hashCode != this.hashCode) {
                return false;
            }
            if (data.length != other.data.length) {
                return false;
            }
            return PlatformDependent.equals(data, 0, other.data, 0, data.length);
        } catch (ClassCastException otherClass) {
            return false;
        }
    }

    public static void putLong(byte[] array, int index, long value) {
        if (HAS_UNSAFE && UNALIGNED) {
            PlatformDependent.putLong(array, index, BIG_ENDIAN_NATIVE_ORDER ? value : Long.reverseBytes(value));
        } else {
            array[index] = (byte) (value >>> 56);
            array[index + 1] = (byte) (value >>> 48);
            array[index + 2] = (byte) (value >>> 40);
            array[index + 3] = (byte) (value >>> 32);
            array[index + 4] = (byte) (value >>> 24);
            array[index + 5] = (byte) (value >>> 16);
            array[index + 6] = (byte) (value >>> 8);
            array[index + 7] = (byte) value;
        }
    }

    public static void putInt(byte[] array, int index, int value) {
        if (HAS_UNSAFE && UNALIGNED) {
            PlatformDependent.putInt(array, index, BIG_ENDIAN_NATIVE_ORDER ? value : Integer.reverseBytes(value));
        } else {
            array[index] = (byte) (value >>> 24);
            array[index + 1] = (byte) (value >>> 16);
            array[index + 2] = (byte) (value >>> 8);
            array[index + 3] = (byte) value;
        }
    }

    public static void putBoolean(byte[] bytes, int offset, boolean val) {
        if (val) {
            bytes[offset] = 1;
        } else {
            bytes[offset] = (byte) 0x00;
        }
    }

    public static void putDouble(byte[] bytes, int offset, double val) {
        putLong(bytes, offset, Double.doubleToRawLongBits(val));
    }

    public static long toLong(byte[] array, int index) {
        if (HAS_UNSAFE && UNALIGNED) {
            long v = PlatformDependent.getLong(array, index);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Long.reverseBytes(v);
        }

        return ((long) array[index] & 0xff) << 56
            | //
            ((long) array[index + 1] & 0xff) << 48
            | //
            ((long) array[index + 2] & 0xff) << 40
            | //
            ((long) array[index + 3] & 0xff) << 32
            | //
            ((long) array[index + 4] & 0xff) << 24
            | //
            ((long) array[index + 5] & 0xff) << 16
            | //
            ((long) array[index + 6] & 0xff) << 8
            | //
            (long) array[index + 7] & 0xff;
    }

    public static int toInt(byte[] array, int index) {
        if (HAS_UNSAFE && UNALIGNED) {
            int v = PlatformDependent.getInt(array, index);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Integer.reverseBytes(v);
        }

        return ((int) array[index] & 0xff) << 24
            | //
            ((int) array[index + 1] & 0xff) << 16
            | //
            ((int) array[index + 2] & 0xff) << 8
            | //
            (int) array[index + 3] & 0xff;
    }

    public static java.sql.Timestamp toTimestamp(byte[] bytes, int offset, final int length) {
        long l = toLong(bytes, offset);
        if (l < 0) {
            return null;
        }
        return new java.sql.Timestamp(l);
    }

    public static boolean toBoolean(byte[] bytes, int offset) {
        return bytes[offset] != 1;
    }

    public static double toDouble(byte[] bytes, int offset) {
        return Double.longBitsToDouble(toLong(bytes, offset));
    }

    @Override
    public int compareTo(Bytes o) {
        return compare(this.data, o.data);
    }

    public static int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

    public static boolean startsWith(byte[] left, int len, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length && i < len; i++, j++) {
            if (left[i] != right[j]) {
                return false;
            }
        }
        // equality
        return true;
    }

    @Override
    public String toString() {
        if (data == null) {
            return "null";
        }
        // ONLY FOR TESTS
        return arraytohexstring(data);
    }

    public static String arraytohexstring(byte[] bytes) {
        StringBuilder string = new StringBuilder();
        for (byte b : bytes) {
            String hexString = Integer.toHexString(0x00FF & b);
            string.append(hexString.length() == 1 ? "0" + hexString : hexString);
        }
        return string.toString();
    }

    public Bytes next() {
        BigInteger i = new BigInteger(this.data);
        i = i.add(BigInteger.ONE);
        return Bytes.from_array(i.toByteArray());
    }

}
