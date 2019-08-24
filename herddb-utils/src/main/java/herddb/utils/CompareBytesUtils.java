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

import io.netty.util.internal.PlatformDependent;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Java 8 compatibile version. In Java 8 you cannot use Arrays.compare(byte[],
 * byte[])
 */
public final class CompareBytesUtils {

    private static final Logger LOG = Logger.getLogger(CompareBytesUtils.class.getName());

    static {
        LOG.info("Not Using Arrays#compare(byte[], byte[]). Using legacy pure-Java implementation, use JDK10 in order to get best performances");
    }

    private CompareBytesUtils() {
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

    public static boolean arraysEquals(byte[] left, byte[] right) {
        return Arrays.equals(left, right);
    }

    public static int compare(
            byte[] left, int fromIndex, int toIndex,
            byte[] right, int fromIndex2, int toIndex2
    ) {
        for (int i = fromIndex, j = fromIndex2; i < toIndex && j < toIndex2; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        int len1 = (toIndex - fromIndex);
        int len2 = (toIndex2 - fromIndex2);
        return len1 - len2;
    }

    public static boolean arraysEquals(
            byte[] left, int fromIndex, int toIndex,
            byte[] right, int fromIndex2, int toIndex2
    ) {

        int aLength = toIndex - fromIndex;
        int bLength = toIndex2 - fromIndex2;
        if (aLength != bLength) {
            return false;
        }
        return PlatformDependent.equals(left, fromIndex, right, fromIndex2, aLength);
    }

    public static int hashCode(byte a[], int offset, int length) {
        if (a == null) {
            return 0;
        }

        int result = 1;
        final int toIndex = length + offset;
        for (int i = offset; i < toIndex; i++) {
            result = 31 * result + a[i];
        }
        return result;
    }
}
