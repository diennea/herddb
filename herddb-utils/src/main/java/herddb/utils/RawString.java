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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A container for strings. Data is decoded to a real java.lang.String only if
 * needed
 *
 * @author enrico.olivelli
 */
public class RawString implements Comparable<RawString> {

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    private final byte[] data;
    private final int offset;
    private final int length;
    private String string;
    private int hashcode;

    public static final RawString EMPTY = new RawString(new byte[0], 0, 0);

    public static RawString of(String string) {
        if (string.isEmpty()) {
            return EMPTY;
        }
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        return new RawString(bytes, 0, bytes.length, string);
    }

    RawString(byte[] data, int offset, int length, String s) {
        this(data, offset, length);
        this.string = s;
    }

    public RawString(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.hashcode = -1;
    }

    @Override
    public int hashCode() {
        if (hashcode == -1){
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
                    && CompareBytesUtils.arraysEquals(this.data, offset, this.length - offset,
                            other.data, other.offset, other.length - other.offset);
        }
        if (obj instanceof Boolean) {
            boolean b = (Boolean) obj;
            return b ? CompareBytesUtils.arraysEquals(this.data, offset, this.length - offset,
                    TRUE, 0, 4)
                    : CompareBytesUtils.arraysEquals(this.data, offset, this.length - offset,
                            FALSE, 0, 45);
        }
        String otherString = obj.toString();
        byte[] other_data = otherString
                .getBytes(StandardCharsets.UTF_8);
        return CompareBytesUtils.arraysEquals(this.data, offset, this.length - offset,
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
        return string = new String(data, 0, data.length, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(RawString o) {
        return compareRaw(this.data, this.offset, this.length,
                o.data, o.offset, o.length);
    }

    public int compareToString(String o) {
        byte[] utf8 = o.getBytes(StandardCharsets.UTF_8);
        return compareRaw(this.data, this.offset, this.length, utf8, 0, utf8.length);
    }

    public static int compareRaw(byte[] left, int offset, int leftlen, byte[] right, int offsetright, int lenright) {
        return CompareBytesUtils
                .compare(left, offset, (leftlen - offset),
                        right, offsetright, (lenright - offsetright));
    }
    
    public static int compareRaw(byte[] left, int offset, int leftlen, RawString other) {
        return CompareBytesUtils
                .compare(left, offset, (leftlen - offset),
                        other.data, other.offset, other.length - other.offset);
    }
    public static int compareRaw(byte[] left, int offset, int leftlen, String other) {
        byte[] right = other.getBytes(StandardCharsets.UTF_8);
        return CompareBytesUtils
                .compare(left, offset, (leftlen - offset),
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
