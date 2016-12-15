package herddb.utils;

import io.netty.buffer.ByteBuf;

/**
 * Utilities for write variable length values on {@link ByteBuf}.
 * 
 * @author diego.salvi
 */
public class ByteBufUtils {
    
    public static final void writeArray(ByteBuf buffer, byte[] array) {
        writeVInt(buffer, array.length);
        buffer.writeBytes(array);
    }
    
    public static final byte[] readArray(ByteBuf buffer) {
        final int len = readVInt(buffer);
        final byte[] array = new byte[len];
        buffer.readBytes(array);
        return array;
    }
    
    public static final void writeVInt(ByteBuf buffer, int i) {
        while ((i & ~0x7F) != 0) {
            buffer.writeByte((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;
        }
        buffer.writeByte((byte) i);
    }
    
    public static final int readVInt(ByteBuf buffer) {
        byte b = buffer.readByte();
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = buffer.readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }
    
    public static final void writeZInt(ByteBuf buffer, int i) {
        writeVInt(buffer, zigZagEncode(i));
    }
    
    public static final int readZInt(ByteBuf buffer) {
        return zigZagDecode(readVInt(buffer));
    }
    
    public static final void writeVLong(ByteBuf buffer, long i) {
        if (i < 0) {
            throw new IllegalArgumentException("cannot write negative vLong (got: " + i + ")");
        }
        writeSignedVLong(buffer, i);
    }

    // write a potentially negative vLong
    private static final void writeSignedVLong(ByteBuf buffer, long i) {
        while ((i & ~0x7FL) != 0L) {
            buffer.writeByte((byte) ((i & 0x7FL) | 0x80L));
            i >>>= 7;
        }
        buffer.writeByte((byte) i);
    }
    
    public static final long readVLong(ByteBuf buffer) {
        return readVLong(buffer, false);
    }

    private static final long readVLong(ByteBuf buffer, boolean allowNegative) {
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
    
    public static final void writeZLong(ByteBuf buffer, long i) {
        writeVLong(buffer, zigZagEncode(i));
    }
    
    public static final long readZLong(ByteBuf buffer) {
        return zigZagDecode(readVLong(buffer));
    }
    
    /** Same as {@link #zigZagEncode(long)} but on integers. */
    private static final int zigZagEncode(int i) {
      return (i >> 31) ^ (i << 1);
    }

    /**
     * <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">Zig-zag</a>
     * encode the provided long. Assuming the input is a signed long whose
     * absolute value can be stored on <tt>n</tt> bits, the returned value will
     * be an unsigned long that can be stored on <tt>n+1</tt> bits.
     */
    private static final long zigZagEncode(long l) {
      return (l >> 63) ^ (l << 1);
    }

    /** Decode an int previously encoded with {@link #zigZagEncode(int)}. */
    private static final int zigZagDecode(int i) {
      return ((i >>> 1) ^ -(i & 1));
    }

    /** Decode a long previously encoded with {@link #zigZagEncode(long)}. */
    private static final long zigZagDecode(long l) {
      return ((l >>> 1) ^ -(l & 1));
    }
    
}
