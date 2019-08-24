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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.internal.PlatformDependent;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * A wrapper for byte[], in order to use it as keys on HashMaps
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "EI_EXPOSE_REP", "UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD"})
public final class Bytes implements Comparable<Bytes>, SizeAwareObject {

    public static final Bytes POSITIVE_INFINITY = new Bytes(new byte[0]);

    public static final Bytes EMPTY_ARRAY = new Bytes(new byte[0]);

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

    public static long estimateSize(byte[] value) {
        return value.length + CONSTANT_BYTE_SIZE;
    }

    private final byte[] buffer;
    private final int offset;
    private final int length;
    private int hashCode = -1;

    public Object deserialized;

    @Override
    public long getEstimatedSize() {
        return length + CONSTANT_BYTE_SIZE;
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

    public static byte[] intToByteArray(int value) {
        byte[] res = new byte[4];
        putInt(res, 0, value);
        return res;
    }

    public static byte[] doubleToByteArray(double value) {
        byte[] res = new byte[8];
        putLong(res, 0, Double.doubleToLongBits(value));
        return res;
    }

    public static byte[] timestampToByteArray(java.sql.Timestamp value) {
        byte[] res = new byte[8];
        putLong(res, 0, value.getTime());
        return res;
    }

    private static final byte[] BOOLEAN_TRUE = {1};
    private static final byte[] BOOLEAN_FALSE = {0};

    @SuppressFBWarnings("MS_EXPOSE_REP")
    public static byte[] booleanToByteArray(boolean value) {
        return value ? BOOLEAN_TRUE : BOOLEAN_FALSE;
    }

    public static Bytes from_long(long value) {
        byte[] res = new byte[8];
        putLong(res, 0, value);
        return new Bytes(res);
    }

    public static Bytes from_array(byte[] data) {
        return new Bytes(data);
    }

    public static Bytes from_array(byte[] data, int offset, int len) {
        return new Bytes(data, offset, len);
    }

    public static Bytes from_nullable_array(byte[] data) {
        if (data == null) {
            return null;
        }
        return new Bytes(data);
    }

    public byte[] to_array() {
        if (offset == 0 && buffer.length == length) {
            return buffer;
        } else {
            byte[] copy = new byte[length];
            System.arraycopy(buffer, offset, copy, 0, length);
            return copy;
        }
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
        return new Bytes(booleanToByteArray(value));
    }

    public static Bytes from_double(double value) {
        byte[] res = new byte[8];
        putDouble(res, 0, value);
        return new Bytes(res);
    }

    public long to_long() {
        assert length == 8;
        return toLong(buffer, offset);
    }

    public RawString to_RawString() {
        return RawString.newUnpooledRawString(buffer, offset, length);
    }

    public int to_int() {
        assert length == 4;
        return toInt(buffer, offset);
    }

    public String to_string() {
        return new String(buffer, offset, length, StandardCharsets.UTF_8);
    }

    public static String to_string(byte[] data) {
        return new String(data, 0, data.length, StandardCharsets.UTF_8);
    }

    public static RawString to_rawstring(byte[] data) {
        return RawString.newUnpooledRawString(data, 0, data.length);
    }

    public java.sql.Timestamp to_timestamp() {
        assert length == 8;
        return toTimestamp(buffer, offset);
    }

    public boolean to_boolean() {
        assert length == 1;
        return toBoolean(buffer, offset);
    }

    public double to_double() {
        assert length == 8;
        return toDouble(buffer, offset);
    }

    private Bytes(byte[] data) {
        this.buffer = data;
        this.offset = 0;
        this.length = buffer.length;
    }

    private Bytes(byte[] data, int offset, int length) {
        this.buffer = data;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int hashCode() {
        if (hashCode == -1) {
            this.hashCode = CompareBytesUtils.hashCode(buffer, offset, length);
        }
        return hashCode;

    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        try {
            final Bytes other = (Bytes) obj;
            if (length != other.length) {
                return false;
            }
            if (other.hashCode() != this.hashCode()) {
                return false;
            }
            return CompareBytesUtils.arraysEquals(buffer, offset, offset + length,
                    other.buffer, other.offset, other.offset + other.length);
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

    public static int compareInt(byte[] array, int index, int value) {
        return Integer.compare(toInt(array, index), value);
    }

    public static int compareInt(byte[] array, int index, long value) {
        return Long.compare(toInt(array, index), value);
    }

    public static int compareLong(byte[] array, int index, int value) {
        return Long.compare(toLong(array, index), value);
    }

    public static int compareLong(byte[] array, int index, long value) {
        return Long.compare(toLong(array, index), value);
    }

    public static int toInt(byte[] array, int index) {
        if (HAS_UNSAFE && UNALIGNED) {
            int v = PlatformDependent.getInt(array, index);
            return BIG_ENDIAN_NATIVE_ORDER ? v : Integer.reverseBytes(v);
        }

        return (array[index] & 0xff) << 24
                | //
                (array[index + 1] & 0xff) << 16
                | //
                (array[index + 2] & 0xff) << 8
                | //
                array[index + 3] & 0xff;
    }

    public static java.sql.Timestamp toTimestamp(byte[] bytes, int offset) {
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

    public static int compare(byte[] left, byte[] right) {
        return CompareBytesUtils.compare(left, right);
    }

    @Override
    public int compareTo(Bytes o) {
        if (this == POSITIVE_INFINITY) {
            return this == o ? 0 : 1;
        } else if (o == POSITIVE_INFINITY) {
            return -1;
        }
        return CompareBytesUtils.compare(buffer, offset, offset + length,
                o.buffer, o.offset, o.offset + o.length);
    }

    public static boolean startsWith(byte[] left, int offset, int bufferlen, int max, byte[] right) {
        int endleft = offset + bufferlen;
        int endmax = offset + max;
        for (int i = offset, j = 0; i < endleft && j < right.length && i < endmax; i++, j++) {
            if (left[i] != right[j]) {
                return false;
            }
        }
        // equality
        return true;
    }

    public int getLength() {
        return length;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        if (buffer == null) {
            return "null";
        }
        // ONLY FOR TESTS
        return arraytohexstring(buffer, offset, length);
    }

    public static String arraytohexstring(byte[] buffer, int offset, int length) {
        StringBuilder string = new StringBuilder();
        for (int i = offset; i < offset + length; i++) {
            byte b = buffer[i];
            String hexString = Integer.toHexString(0x00FF & b);
            string.append(hexString.length() == 1 ? "0" + hexString : hexString);
        }
        return string.toString();
    }

    public ByteArrayCursor newCursor() {
        return ByteArrayCursor.wrap(buffer, offset, length);
    }

    /**
     * Returns the next {@code Bytes} instance.
     * <p>
     * Depending on current instance it couldn't be possible to evaluate the
     * next one: if every bit in current byte array is already 1 next would
     * generate an overflow and isn't permitted.
     * </p>
     *
     * @return the next Bytes instance
     * @throws IllegalStateException if cannot evaluate a next value.
     */
    public Bytes next() {

        final byte[] dst = new byte[length];
        System.arraycopy(buffer, offset, dst, 0, length);

        int idx = length - 1;

        /*
         * We alter bytes from last in a backward fashion. We could have done directly a manual copy with
         * increment when needed but System.arraycopy is really faster than manual for loop copy and in
         * standard cases we just need to very fiew bytes (normally just one)
         */
        while (idx > -1 && ++dst[idx] == 0) {
            --idx;
        }

        /* If addition gone up to the byte array end then there isn't any more space */
        if (idx == -1) {
            throw new IllegalStateException(
                    "Cannot generate a next value for a full 1 byte array, no space for another element");
        }

        return new Bytes(dst);

    }

    public boolean startsWith(int length, byte[] prefix) {
        return Bytes.startsWith(this.buffer, this.offset, this.length, length, prefix);
    }

    /**
     * Ensure that this value is not retaining strong references to a shared buffer
     *
     * @return the buffer itself or a copy
     */
    public Bytes nonShared() {
        if (isShared()) {
            byte[] array = new byte[this.length];
            System.arraycopy(buffer, offset, array, 0, length);
            return new Bytes(array, 0, length);
        }
        return this;
    }

    public boolean isShared() {
        return this.offset != 0 || this.length != buffer.length;
    }

}
