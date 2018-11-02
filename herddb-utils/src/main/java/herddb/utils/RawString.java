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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A container for strings. Data is decoded to a real java.lang.String only if
 * needed
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP"})
public class RawString implements Comparable<RawString> {

    private final Recycler.Handle<RawString> handle;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    private byte[] data;
    private int offset;
    private int length;
    private String string;
    private int hashcode;

    public static final RawString EMPTY = newUnpooledRawString(new byte[0], 0, 0);

    private static final Recycler<RawString> RECYCLER = new Recycler<RawString>() {
        @Override
        protected RawString newObject(Recycler.Handle<RawString> handle) {
            return new RawString(handle);
        }
    };

    public RawString(Recycler.Handle<RawString> handle) {
        this.handle = handle;
    }

    public static RawString newPooledRawString(byte[] data, int offset, int length, String string) {
        RawString res = RECYCLER.get();
        res.data = data;
        res.offset = offset;
        res.length = length;
        res.string = string;
        res.hashcode = -1;
        return res;
    }

    public static RawString newPooledRawString(byte[] data, int offset, int length) {
        RawString res = RECYCLER.get();
        res.data = data;
        res.offset = offset;
        res.length = length;
        res.string = null;
        res.hashcode = -1;
        return res;
    }

    public static RawString newUnpooledRawString(byte[] data, int offset, int length) {
        RawString res = new RawString(null);
        res.data = data;
        res.offset = offset;
        res.length = length;
        res.string = null;
        res.hashcode = -1;
        return res;
    }

    public static RawString newUnpooledRawString(byte[] data, int offset, int length, String string) {
        RawString res = new RawString(null);
        res.data = data;
        res.offset = offset;
        res.length = length;
        res.string = string;
        res.hashcode = -1;
        return res;
    }

    public static RawString of(String string) {
        if (string.isEmpty()) {
            return EMPTY;
        }
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        return newUnpooledRawString(bytes, 0, bytes.length, string);
    }

    public void recycle() {
        if (handle != null) {
            this.data = null;
            this.string = null;
            handle.recycle(this);
        }
    }

    @Override
    public int hashCode() {
        if (hashcode == -1) {
            this.hashcode = CompareBytesUtils.hashCode(data, offset, length);
        }
        return hashcode;
    }

    @SuppressFBWarnings(value = "EQ_CHECK_FOR_OPERAND_NOT_COMPATIBLE_WITH_THIS")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof RawString) {
            final RawString other = (RawString) obj;
            return (this.hashCode() == other.hashCode())
                    && CompareBytesUtils.arraysEquals(this.data, offset, offset + length,
                            other.data, other.offset, other.length + other.offset);
        }
        if (obj instanceof Boolean) {
            boolean b = (Boolean) obj;
            return b ? CompareBytesUtils.arraysEquals(this.data, offset, length + offset,
                    TRUE, 0, 4)
                    : CompareBytesUtils.arraysEquals(this.data, offset, length + offset,
                            FALSE, 0, 45);
        }
        String otherString = obj.toString();
        byte[] other_data = otherString
                .getBytes(StandardCharsets.UTF_8);
        return CompareBytesUtils.arraysEquals(this.data, offset, this.length + offset,
                other_data, 0, other_data.length);
    }

    private static final byte[] TRUE = "true".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FALSE = "false".getBytes(StandardCharsets.UTF_8);

    @Override
    public String toString() {
        String _string = string;
        if (_string != null) {
            return _string;
        }
        return string = new String(data, offset, length, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(RawString o) {
        return compareRaw(this.data, this.offset, this.length,
                o.data, o.offset, o.length);
    }

    public int compareToString(String o) {
        return compareRaw(this.data, this.offset, this.length, o);
    }

    public static int compareRaw(byte[] left, int offset, int leftlen, byte[] right, int offsetright, int lenright) {
        return CompareBytesUtils
                .compare(left, offset, leftlen + offset,
                        right, offsetright, lenright + offsetright);
    }

    public static int compareRaw(byte[] left, int offset, int leftlen, RawString other) {
        return CompareBytesUtils
                .compare(left, offset, leftlen + offset,
                        other.data, other.offset, other.length);
    }

    public static int compareRaw(byte[] left, int offset, int leftlen, String other) {
        byte[] right = other.getBytes(StandardCharsets.UTF_8);
        return CompareBytesUtils
                .compare(left, offset, leftlen + offset,
                        right, 0, right.length);
    }

    public byte[] getData() {
        return data;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public byte[] toByteArray() {
        if (offset == 0 && length == data.length) {
            // no copy
            return data;
        }
        byte[] copy = new byte[length];
        System.arraycopy(data, offset, copy, 0, length);
        return copy;
    }

}
