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
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

/**
 * This utility class enables accessing a byte[] while leveraging
 * {@link ExtendedDataInputStream} features without performing copies to access
 * data
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ByteArrayCursor implements Closeable {

    private static final Recycler<ByteArrayCursor> RECYCLER = new Recycler<ByteArrayCursor>() {

        @Override
        protected ByteArrayCursor newObject(
                Handle<ByteArrayCursor> handle
        ) {
            return new ByteArrayCursor(handle);
        }

    };

    private final io.netty.util.Recycler.Handle<ByteArrayCursor> handle;

    private byte[] array;
    private int position;
    private int end;

    public static ByteArrayCursor wrap(byte[] array) {
        ByteArrayCursor res = RECYCLER.get();
        res.array = array;
        res.position = 0;
        res.end = array.length;
        return res;
    }

    public static ByteArrayCursor wrap(byte[] array, int offset, int length) {
        ByteArrayCursor res = RECYCLER.get();
        res.array = array;
        res.position = offset;
        res.end = offset + length;
        return res;
    }

    private ByteArrayCursor(Handle<ByteArrayCursor> handle) {
        this.handle = handle;
    }

    public ByteArrayCursor(byte[] array) {
        this.array = array;
        this.handle = null;
    }

    public boolean isEof() {
        return position >= end;
    }

    public int read() {
        if (position >= end) {
            // EOF
            return -1;
        }
        return array[position++];
    }

    public byte readByte() throws IOException {
        checkReadable(1);
        return array[position++];
    }

    private void checkReadable(int len) throws IOException {
        if (position + len > end) {
            throw new EOFException("array len " + end + ", pos " + position + ", try to read " + len + " bytes");
        }
    }

    /**
     * Reads an int stored in variable-length format. Reads between one and five
     * bytes. Smaller values take fewer bytes. Negative numbers are not
     * supported.
     *
     * @return
     * @throws java.io.IOException
     */
    public int readVInt() throws IOException {
        byte b = readByte();
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }

    /**
     * Same as {@link  #readVInt() } but does not throw EOFException. Since
     * throwing exceptions is very expensive for the JVM this operation is
     * preferred if you could hit and EOF
     *
     * @return
     * @throws IOException
     * @see #isEof()
     */
    public int readVIntNoEOFException() throws IOException {
        int ch = read();
        if (ch < 0) {
            // EOF
            position = end;
            return -1;
        }

        byte b = (byte) (ch);
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }

    /**
     * Reads a long stored in variable-length format. Reads between one and nine
     * bytes. Smaller values take fewer bytes. Negative numbers are not
     * supported.
     *
     * @return
     * @throws java.io.IOException
     */
    public long readVLong() throws IOException {
        return readVLong(false);
    }

    private long readVLong(boolean allowNegative) throws IOException {
        /* This is the original code of this method,
     * but a Hotspot bug (see LUCENE-2975) corrupts the for-loop if
     * readByte() is inlined. So the loop was unwinded!
    byte b = readByte();
    long i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = readByte();
      i |= (b & 0x7FL) << shift;
    }
    return i;
         */
        byte b = readByte();
        if (b >= 0) {
            return b;
        }
        long i = b & 0x7FL;
        b = readByte();
        i |= (b & 0x7FL) << 7;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 14;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 21;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 28;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 35;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 42;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 49;
        if (b >= 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 56;
        if (b >= 0) {
            return i;
        }
        if (allowNegative) {
            b = readByte();
            i |= (b & 0x7FL) << 63;
            if (b == 0 || b == 1) {
                return i;
            }
            throw new IOException("Invalid vLong detected (more than 64 bits)");
        } else {
            throw new IOException("Invalid vLong detected (negative values disallowed)");
        }
    }

    public int readZInt() throws IOException {
        int i = readVInt();
        return (i >>> 1) ^ -(i & 1);
    }

    public long readZLong() throws IOException {
        long l = readVLong();
        return (l >>> 1) ^ -(l & 1);
    }

    private static final byte[] EMPTY_ARRAY = new byte[0];
    private static final float[] EMPTY_FLOAT_ARRAY = new float[0];

    public int readArrayLen() throws IOException {
        return readVInt();
    }

    public int getPosition() {
        return position;
    }

    public byte[] getArray() {
        return array;
    }

    public void readArray(int len, byte[] buffer) throws IOException {
        if (len == 0) {
            return;
        }
        try {
            System.arraycopy(array, position, buffer, 0, len);
            position += len;
        } catch (IndexOutOfBoundsException t) {
            throw new IOException(t);
        }
    }

    public int readInt() throws IOException {
        checkReadable(4);
        int ch1 = (array[position++] & 255);
        int ch2 = (array[position++] & 255);
        int ch3 = (array[position++] & 255);
        int ch4 = (array[position++] & 255);
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public float readFloat() throws IOException {
        int asInt = readInt();
        return Float.intBitsToFloat(asInt);
    }

    public void skipFloat() throws IOException {
        skipInt();
    }

    public long readLong() throws IOException {
        checkReadable(8);
        long res = (((long) array[position++] << 56)
                + ((long) (array[position++] & 255) << 48)
                + ((long) (array[position++] & 255) << 40)
                + ((long) (array[position++] & 255) << 32)
                + ((long) (array[position++] & 255) << 24)
                + ((array[position++] & 255) << 16)
                + ((array[position++] & 255) << 8)
                + ((array[position++] & 255) << 0));
        return res;
    }

    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public final boolean readBoolean() throws IOException {
        int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    public byte[] readArray() throws IOException {
        int len = readVInt();
        if (len == 0) {
            return EMPTY_ARRAY;
        } else if (len == -1) {
            /* NULL array */
            return null;
        }
        byte[] res = new byte[len];
        readArray(len, res);
        return res;
    }

    public float[] readFloatArray() throws IOException {
        int len = readVInt();
        if (len == 0) {
            return EMPTY_FLOAT_ARRAY;
        } else if (len == -1) {
            /* NULL array */
            return null;
        }
        float[] result = new float[len];
        for (int i = 0; i < len; i++) {
            result[i] = readFloat();
        }
        return result;
    }

    public Bytes readBytesNoCopy() throws IOException {
        int len = readVInt();
        if (len == 0) {
            return Bytes.EMPTY_ARRAY;
        } else if (len == -1) {
            /* NULL array */
            return null;
        }
        Bytes res = Bytes.from_array(array, position, len);
        position += len;
        return res;
    }

    public Bytes readBytes() throws IOException {
        return Bytes.from_nullable_array(readArray());
    }

    public RawString readRawStringNoCopy() throws IOException {
        int len = readVInt();
        if (len == 0) {
            return RawString.EMPTY;
        } else if (len == -1) {
            /* NULL array */
            return null;
        }

        RawString string = RawString.newPooledRawString(array, position, len);
        position += len;
        return string;
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipArray() throws IOException {
        int len = readVInt();
        if (len <= 0) {
            return;
        }
        skip(len);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipFloatArray() throws IOException {
        int len = readVInt();
        if (len <= 0) {
            return;
        }
        skip(len * 4);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipInt() throws IOException {
        skip(4);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipLong() throws IOException {
        skip(8);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipDouble() throws IOException {
        skip(8);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipBoolean() throws IOException {
        skip(1);
    }

    public void skip(int pos) throws IOException {
        if (pos < 0) {
            throw new IOException("corrupted data");
        }
        checkReadable(pos);
        position += pos;
    }

    @Override
    public void close() {
        array = null;
        position = 0;
        if (handle != null) {
            handle.recycle(this);
        }
    }
}
