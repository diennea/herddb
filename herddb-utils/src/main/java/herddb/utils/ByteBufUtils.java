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

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Utilities for write variable length values on {@link ByteBuf}.
 *
 * @author diego.salvi
 */
public class ByteBufUtils {

    public static void writeArray(ByteBuf buffer, byte[] array) {
        writeVInt(buffer, array.length);
        buffer.writeBytes(array);
    }

    public static void writeFloatArray(ByteBuf buffer, float[] array) {
        writeVInt(buffer, array.length);
        for (float f : array) {
            buffer.writeFloat(f);
        }
    }

    public static void writeFloatArray(ByteBuf buffer, List<Number> array) {
        writeVInt(buffer, array.size());
        for (Number f : array) {
            buffer.writeFloat(f.floatValue());
        }
    }

    public static void writeArray(ByteBuf buffer, Bytes array) {
        writeVInt(buffer, array.getLength());
        buffer.writeBytes(array.getBuffer(), array.getOffset(), array.getLength());
    }

    public static void writeArray(ByteBuf buffer, byte[] array, int offset, int length) {
        writeVInt(buffer, length);
        buffer.writeBytes(array, offset, length);
    }

    public static void writeString(ByteBuf buffer, String string) {
        writeArray(buffer, string.getBytes(StandardCharsets.UTF_8));
    }

    public static void writeRawString(ByteBuf buffer, RawString string) {
        writeArray(buffer, string.getData(), string.getOffset(), string.getLength());
    }

    public static byte[] readArray(ByteBuf buffer) {
        final int len = readVInt(buffer);
        final byte[] array = new byte[len];
        buffer.readBytes(array);
        return array;
    }

    public static float[] readFloatArray(ByteBuf buffer) {
        final int len = readVInt(buffer);
        final float[] array = new float[len];
        for (int i = 0; i < len; i++) {
            array[i] = buffer.readFloat();
        }
        return array;
    }

    public static String readString(ByteBuf buffer) {
        final int len = readVInt(buffer);
        final byte[] array = new byte[len];
        buffer.readBytes(array);
        return new String(array, StandardCharsets.UTF_8);
    }

    public static RawString readRawString(ByteBuf buffer) {
        final int len = readVInt(buffer);
        final byte[] array = new byte[len];
        buffer.readBytes(array);
        return RawString.newPooledRawString(array, 0, len);
    }

    public static RawString readUnpooledRawString(ByteBuf buffer) {
        final int len = readVInt(buffer);
        final byte[] array = new byte[len];
        buffer.readBytes(array);
        return RawString.newUnpooledRawString(array, 0, len);
    }

    public static void skipArray(ByteBuf buffer) {
        final int len = readVInt(buffer);
        buffer.skipBytes(len);
    }

    public static void writeVInt(ByteBuf buffer, int i) {
        if ((i & ~0x7F) != 0) {
            buffer.writeByte((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;

            if ((i & ~0x7F) != 0) {
                buffer.writeByte((byte) ((i & 0x7F) | 0x80));
                i >>>= 7;

                if ((i & ~0x7F) != 0) {
                    buffer.writeByte((byte) ((i & 0x7F) | 0x80));
                    i >>>= 7;

                    if ((i & ~0x7F) != 0) {
                        buffer.writeByte((byte) ((i & 0x7F) | 0x80));
                        i >>>= 7;
                    }
                }
            }
        }

        buffer.writeByte((byte) i);
    }

    public static int readVInt(ByteBuf buffer) {
        byte b = buffer.readByte();
        int i = b & 0x7F;

        if ((b & 0x80) != 0) {
            b = buffer.readByte();
            i |= (b & 0x7F) << 7;

            if ((b & 0x80) != 0) {
                b = buffer.readByte();
                i |= (b & 0x7F) << 14;

                if ((b & 0x80) != 0) {
                    b = buffer.readByte();
                    i |= (b & 0x7F) << 21;

                    if ((b & 0x80) != 0) {
                        b = buffer.readByte();
                        i |= (b & 0x7F) << 28;
                    }
                }
            }
        }
        return i;
    }

    public static void writeZInt(ByteBuf buffer, int i) {
        writeVInt(buffer, zigZagEncode(i));
    }

    public static int readZInt(ByteBuf buffer) {
        return zigZagDecode(readVInt(buffer));
    }

    public static void writeVLong(ByteBuf buffer, long i) {
        if (i < 0) {
            throw new IllegalArgumentException("cannot write negative vLong (got: " + i + ")");
        }
        writeSignedVLong(buffer, i);
    }

    // write a potentially negative vLong
    private static void writeSignedVLong(ByteBuf buffer, long i) {
        while ((i & ~0x7FL) != 0L) {
            buffer.writeByte((byte) ((i & 0x7FL) | 0x80L));
            i >>>= 7;
        }
        buffer.writeByte((byte) i);
    }

    public static long readVLong(ByteBuf buffer) {
        return readVLong(buffer, false);
    }

    private static long readVLong(ByteBuf buffer, boolean allowNegative) {
        byte b = buffer.readByte();
        if (b >= 0) {
            return b;
        }
        long i = b & 0x7FL;
        b = buffer.readByte();
        i |= (b & 0x7FL) << 7;
        if (b >= 0) {
            return i;
        }
        b = buffer.readByte();
        i |= (b & 0x7FL) << 14;
        if (b >= 0) {
            return i;
        }
        b = buffer.readByte();
        i |= (b & 0x7FL) << 21;
        if (b >= 0) {
            return i;
        }
        b = buffer.readByte();
        i |= (b & 0x7FL) << 28;
        if (b >= 0) {
            return i;
        }
        b = buffer.readByte();
        i |= (b & 0x7FL) << 35;
        if (b >= 0) {
            return i;
        }
        b = buffer.readByte();
        i |= (b & 0x7FL) << 42;
        if (b >= 0) {
            return i;
        }
        b = buffer.readByte();
        i |= (b & 0x7FL) << 49;
        if (b >= 0) {
            return i;
        }
        b = buffer.readByte();
        i |= (b & 0x7FL) << 56;
        if (b >= 0) {
            return i;
        }
        if (allowNegative) {
            b = buffer.readByte();
            i |= (b & 0x7FL) << 63;
            if (b == 0 || b == 1) {
                return i;
            }
            throw new IllegalArgumentException("Invalid vLong detected (more than 64 bits)");
        } else {
            throw new IllegalArgumentException("Invalid vLong detected (negative values disallowed)");
        }
    }

    public static void writeZLong(ByteBuf buffer, long i) {
        writeVLong(buffer, zigZagEncode(i));
    }

    public static long readZLong(ByteBuf buffer) {
        return zigZagDecode(readVLong(buffer));
    }

    public static void writeDouble(ByteBuf buffer, double i) {
        buffer.writeLong(Double.doubleToLongBits(i));
    }

    public static double readDouble(ByteBuf buffer) {
        return Double.longBitsToDouble(buffer.readLong());
    }

    /**
     * Same as {@link #zigZagEncode(long)} but on integers.
     */
    private static int zigZagEncode(int i) {
        return (i >> 31) ^ (i << 1);
    }

    /**
     * <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">Zig-zag</a>
     * encode the provided long. Assuming the input is a signed long whose
     * absolute value can be stored on <tt>n</tt> bits, the returned value will
     * be an unsigned long that can be stored on <tt>n+1</tt> bits.
     */
    private static long zigZagEncode(long l) {
        return (l >> 63) ^ (l << 1);
    }

    /**
     * Decode an int previously encoded with {@link #zigZagEncode(int)}.
     */
    private static int zigZagDecode(int i) {
        return ((i >>> 1) ^ -(i & 1));
    }

    /**
     * Decode a long previously encoded with {@link #zigZagEncode(long)}.
     */
    private static long zigZagDecode(long l) {
        return ((l >>> 1) ^ -(l & 1));
    }

}
