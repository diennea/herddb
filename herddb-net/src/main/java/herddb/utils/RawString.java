/*
 * Copyright 2017 enrico.olivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A container for strings. Data is decoded to a real java.lang.String only if needed
 *
 * @author enrico.olivelli
 */
public class RawString implements Comparable<RawString> {

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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof String) {
            byte[] other_data = ((String) obj).getBytes(StandardCharsets.UTF_8);
            return Arrays.equals(this.data, other_data);
        }
        if (obj.getClass() != RawString.class) {
            return false;
        }
        final RawString other = (RawString) obj;
        if (this.hashcode != other.hashcode) {
            return false;
        }
        return Arrays.equals(this.data, other.data);
    }

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
        return compare(this.data, o.data);
    }

    private static int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

}
