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

import herddb.core.HerdDBInternalException;
import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Predicate expressed using SQL syntax
 *
 * @author enrico.olivelli
 */
public interface SQLRecordPredicateFunctions {

    static boolean toBoolean(Object result) {
        if (result == null) {
            return false;
        }
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        return "true".equalsIgnoreCase(result.toString());
    }

    static int compareNullTo(Object b) {
        if (b == null) {
            return 0;
        } else {
            return 1;
        }
    }
    enum CompareResult {
        GREATER,
        MINOR,
        EQUALS,
        NULL;
        public static CompareResult fromInt(int i) {
            if (i == 0) {
                return EQUALS;
            } else if (i > 0) {
                return GREATER;
            } else {
                return MINOR;
            }
        }
        public static CompareResult fromLong(long i) {
            if (i == 0) {
                return EQUALS;
            } else if (i > 0) {
                return GREATER;
            } else {
                return MINOR;
            }
        }
    }
    /**
     * Compare two values, reporting special cases about NULL.
     * NULL is not greater or minor than any other value and NULL is not equal to NULL.
     * @param a
     * @param b
     * @return the outcome of the comparation.
     * @see #compare(java.lang.Object, java.lang.Object)
     */
    static CompareResult compareConsiderNull(Object a, Object b) {
        if (a == null || b == null) {
            return CompareResult.NULL;
        }
        if (a instanceof RawString) {
            if (b instanceof RawString) {
                return CompareResult.fromInt(((RawString) a).compareTo((RawString) b));
            }
            if (b instanceof String) {
                return CompareResult.fromInt(((RawString) a).compareToString((String) b));
            }
        }
        if (a instanceof String && b instanceof RawString) {
            return CompareResult.fromInt(-((RawString) b).compareToString((String) a));
        }
        if (a instanceof Integer) {
            if (b instanceof Integer) {
                return CompareResult.fromInt((Integer) a - (Integer) b);
            }
            if (b instanceof Long) {
                long delta = (Integer) a - (Long) b;
                return CompareResult.fromLong(delta);
            }
        }
        if (a instanceof Long) {
            if (b instanceof Long) {
                long delta = (Long) a - (Long) b;
                return CompareResult.fromLong(delta);
            }
            if (b instanceof java.util.Date) {
                long delta = ((Long) a) - ((java.util.Date) b).getTime();
                return CompareResult.fromLong(delta);
            }
        }
        if (a instanceof Number && b instanceof Number) {
            return CompareResult.fromInt(Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()));
        }
        if (a instanceof java.util.Date) {
            if (b instanceof java.util.Date) {
                long delta = ((java.util.Date) a).getTime() - ((java.util.Date) b).getTime();
                return CompareResult.fromInt(delta == 0 ? 0 : delta > 0 ? 1 : -1);
            }
            if (b instanceof java.lang.Long) {
                long delta = ((java.util.Date) a).getTime() - ((Long) b);
                return CompareResult.fromInt(delta == 0 ? 0 : delta > 0 ? 1 : -1);
            }
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return CompareResult.fromInt(((Comparable) a).compareTo(b));
        }
        if (a instanceof byte[] && b instanceof byte[]) {
            return CompareResult.fromInt(Bytes.compare((byte[]) a, (byte[]) b));
        }
        if (a instanceof float[] && b instanceof float[]) {
            return CompareResult.fromInt(compareFloatArrays((float[]) a, (float[]) b));
        }
        throw new IllegalArgumentException(
                "uncomparable objects " + a.getClass() + " ('" + a + "') vs " + b.getClass() + " ('" + b + "')");
    }

    static int compareFloatArrays(float[] arr1, float[] arr2) {
        int minLength = Math.min(arr1.length, arr2.length);

        for (int i = 0; i < minLength; i++) {
            int comparison = Float.compare(arr1[i], arr2[i]);
            if (comparison != 0) {
                return comparison; // Return the result if elements are not equal
            }
        }

        // If the common elements are equal, check the lengths of the arrays
        return Integer.compare(arr1.length, arr2.length);
    }

    /**
     * Compare two values, NULL are greater than all other values and NULL == NULL.
     * It follows the general contract of {@link Comparator}
     * @param a
     * @param b
     * @return 1 if a is greater than b, 0 if they are equals to each other and -1 if a is minor than b
     * @see #compareConsiderNull(java.lang.Object, java.lang.Object)
     */
    static int compare(Object a, Object b) {
        if (a == null) {
            if (b == null) {
                return 0;
            } else {
                return 1;
            }
        } else if (b == null) {
            return -1;
        }
        if (a instanceof RawString) {
            if (b instanceof RawString) {
                return ((RawString) a).compareTo((RawString) b);
            }
            if (b instanceof String) {
                return ((RawString) a).compareToString((String) b);
            }
        }
        if (a instanceof String && b instanceof RawString) {
            return -((RawString) b).compareToString((String) a);
        }
        if (a instanceof Integer) {
            if (b instanceof Integer) {
                return (Integer) a - (Integer) b;
            }
            if (b instanceof Long) {
                long delta = (Integer) a - (Long) b;
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
        }
        if (a instanceof Long) {
            if (b instanceof Long) {
                long delta = (Long) a - (Long) b;
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
            if (b instanceof java.util.Date) {
                long delta = ((Long) a) - ((java.util.Date) b).getTime();
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
        }
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        if (a instanceof java.util.Date) {
            if (b instanceof java.util.Date) {
                long delta = ((java.util.Date) a).getTime() - ((java.util.Date) b).getTime();
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
            if (b instanceof java.lang.Long) {
                long delta = ((java.util.Date) a).getTime() - ((Long) b);
                return delta == 0 ? 0 : delta > 0 ? 1 : -1;
            }
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b);
        }
        if (a instanceof byte[] && b instanceof byte[]) {
            return Bytes.compare((byte[]) a, (byte[]) b);
        }
        throw new IllegalArgumentException(
                "uncomparable objects " + a.getClass() + " ('" + a + "') vs " + b.getClass() + " ('" + b + "')");
    }

    static Object add(Object a, Object b) throws IllegalArgumentException {
        if (a == null && b == null) {
            return null;
        }
        if (a == null) {
            a = 0;
        }
        if (b == null) {
            b = 0;
        }
        if (a instanceof Long && b instanceof Long) {
            return (Long) a + (Long) b;
        }
        if (a instanceof Integer && b instanceof Integer) {
            return (long) ((Integer) a + (Integer) b);
        }
        if (a instanceof Integer && b instanceof Long) {
            return ((Integer) a + (Long) b);
        }
        if (a instanceof Long && b instanceof Integer) {
            return ((Long) a + (Integer) b);
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() + ((Number) b).doubleValue();
        }
        if (a instanceof java.sql.Timestamp && b instanceof Long) {
            // TIMESTAMPADD
            return new java.sql.Timestamp(((java.sql.Timestamp) a).getTime() + ((Long) b));
        }

        throw new IllegalArgumentException("cannot add " + a + " and " + b);
    }

    static Object modulo(Object a, Object b) throws IllegalArgumentException {
        if (a == null && b == null) {
            return null;
        }
        if (a == null) {
            a = 0;
        }
        if (b == null) {
            b = 0;
        }
        if (a instanceof Long && b instanceof Long) {
            return (Long) a % (Long) b;
        }
        if (a instanceof Integer && b instanceof Integer) {
            return (long) ((Integer) a % (Integer) b);
        }
        if (a instanceof Integer && b instanceof Long) {
            return ((Integer) a % (Long) b);
        }
        if (a instanceof Long && b instanceof Integer) {
            return ((Long) a % (Integer) b);
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() % ((Number) b).doubleValue();
        }
        if (a instanceof java.sql.Timestamp && b instanceof Long) {
            // TIMESTAMPADD
            return new java.sql.Timestamp(((java.sql.Timestamp) a).getTime() % ((Long) b));
        }

        throw new IllegalArgumentException("cannot perform modulo on " + a + " and " + b);
    }

    static Object subtract(Object a, Object b) throws IllegalArgumentException {
        if (a == null && b == null) {
            return null;
        }
        if (a == null) {
            a = 0;
        }
        if (b == null) {
            b = 0;
        }
        if (a instanceof Long && b instanceof Long) {
            return (Long) a - (Long) b;
        }
        if (a instanceof Integer && b instanceof Integer) {
            return (long) ((Integer) a - (Integer) b);
        }
        if (a instanceof Integer && b instanceof Long) {
            return ((Integer) a - (Long) b);
        }
        if (a instanceof Long && b instanceof Integer) {
            return ((Long) a - (Integer) b);
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() - ((Number) b).doubleValue();
        }
        if (a instanceof java.sql.Timestamp && b instanceof java.sql.Timestamp) {
            // TIMESTAMPDIFF
            return ((java.sql.Timestamp) a).getTime() - ((java.sql.Timestamp) b).getTime();
        }
        throw new IllegalArgumentException("cannot subtract " + a + " and " + b);
    }

    static Object multiply(Object a, Object b) throws IllegalArgumentException {
        if (a == null && b == null) {
            return null;
        }
        if (a == null) {
            a = 0;
        }
        if (b == null) {
            b = 0;
        }
        if (a instanceof Long && b instanceof Long) {
            return (Long) a * (Long) b;
        }
        if (a instanceof Integer && b instanceof Integer) {
            return (long) ((Integer) a * (Integer) b);
        }
        if (a instanceof Integer && b instanceof Long) {
            return ((Integer) a * (Long) b);
        }
        if (a instanceof Long && b instanceof Integer) {
            return ((Long) a * (Integer) b);
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() * ((Number) b).doubleValue();
        }
        throw new IllegalArgumentException("cannot multiply " + a + " and " + b);
    }

    static Object divide(Object a, Object b) throws IllegalArgumentException {
        if (a == null && b == null) {
            return null;
        }
        if (a == null) {
            a = 0;
        }
        if (b == null) {
            b = 0;
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() / ((Number) b).doubleValue();
        }
        throw new IllegalArgumentException("cannot divide " + a + " and " + b);
    }

    static boolean objectEquals(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a instanceof RawString) {
            return a.equals(b);
        }
        if (b instanceof RawString) {
            return b.equals(a);
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            return ((java.util.Date) a).getTime() == ((java.util.Date) b).getTime();
        }
        if (a instanceof java.lang.Boolean
                && (Boolean.parseBoolean(b.toString()) == ((Boolean) a))) {
            return true;
        }
        if (b instanceof java.lang.Boolean
                && (Boolean.parseBoolean(a.toString()) == ((Boolean) b))) {
            return true;
        }
        return Objects.equals(a, b);
    }

    static boolean objectNotEquals(Object a, Object b) {
        if (a == null || b == null) {
            // if one of the two operands is NULL that "not equals" should return false
            return false;
        }
        return !objectEquals(a, b);
    }

    static Pattern compileLikePattern(String b, char escapeChar) throws HerdDBInternalException {

        /*
         * We presume that in string there will be 1 or 2 '%' or '_' characters. To avoid multiple array
         * copies in standard cases we preallocate a builder size of string input size plus 6 chars per
         * special character (4 chars for wrapping quoting sequence and 2 for pattern characters: \\E.*\\Q
         * or \\E.?\\Q) plus 4 chars for whole string wrapping quote sequence (\\Qstring\\E).
         */
        final StringBuilder builder = new StringBuilder(b.length() + 18);

        builder.append("\\Q");

        int limit = b.length();
        boolean escaping = false;
        for (int idx = 0; idx < limit; ++idx) {
            char ch = b.charAt(idx);
            if (ch == escapeChar) {
                escaping = true;
            } else {
                if (escaping) {
                    builder.append(ch);
                    escaping = false;
                } else {
            switch (ch) {
                case '%':
                    builder.append("\\E.*\\Q");
                    break;
                case '_':
                    builder.append("\\E.{1}\\Q");
                    break;
                default:
                    builder.append(ch);
                    break;
            }
                }
            }
        }

        builder.append("\\E");

        String like = builder.toString();
        try {
            return Pattern.compile(like, Pattern.DOTALL);
        } catch (IllegalArgumentException err) {
            throw new HerdDBInternalException("Cannot compile LIKE expression '" + b + "': " + err);
        }
    }

    static boolean like(Object a, Object b, char escape) {
        if (a == null || b == null) {
            return false;
        }
        Pattern pattern = compileLikePattern(b.toString(), escape);
        return matches(a, pattern);
    }

    static boolean matches(Object a, Pattern pattern) {
        if (a == null) {
            return false;
        }
        Matcher matcher = pattern.matcher(a.toString());
        return matcher.matches();
    }

}
