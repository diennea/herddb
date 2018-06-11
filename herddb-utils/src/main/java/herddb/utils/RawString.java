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
 * A container for strings. Data is decoded to a real java.lang.String only if needed
 *
 * @author enrico.olivelli
 */

public class RawString implements Comparable<RawString> {

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public final byte[] data;
    private String string;
    private final int hashcode;

    public static RawString of(String string) {
        return new RawString(string.getBytes(StandardCharsets.UTF_8), string);
    }

    RawString(byte[] data, String s) {
        this(data);
        this.string = s;
    }

    public RawString(byte[] data) {
        this.data = data;
        this.hashcode = Arrays.hashCode(data);
    }

    @Override
    public int hashCode() {
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
            return (this.hashcode == other.hashcode)
                && Arrays.equals(this.data, other.data);
        }
        if (obj instanceof Boolean) {
            boolean b = (Boolean) obj;
            return b ? Arrays.equals(this.data, TRUE) : Arrays.equals(this.data, FALSE);
        }
        String otherString = obj.toString();
        byte[] other_data = otherString
            .getBytes(StandardCharsets.UTF_8);
        return Arrays.equals(this.data, other_data);
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
        return compareRaw(this.data, o.data);
    }

    public int compareToString(String o) {
        return compareRaw(this.data, o.getBytes(StandardCharsets.UTF_8));
    }

    public static int compareRaw(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
    
    public static int compareRaw(byte[] left, int offset, int leftlen, byte[] right) {
        for (int i = 0, j = 0; i < leftlen && j < right.length; i++, j++) {
            int a = (left[i + offset] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return leftlen - right.length;
    }

}
