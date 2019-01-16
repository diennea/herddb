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

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Predicate expressed using SQL syntax
 *
 * @author enrico.olivelli
 */
public interface SQLRecordPredicateFunctions {

    public static boolean toBoolean(Object result) {
        if (result == null) {
            return false;
        }
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        return "true".equalsIgnoreCase(result.toString());
    }

    public static int compareNullTo(Object b) {
        if (b == null) {
            return 0;
        } else {
            return 1;
        }
    }

    public static int compare(Object a, Object b) {
        if (a == null) {
            if (b == null) {
                return 0;
            } else {
                return 1;
            }
        } else if (b == null) {
            return -1;
        }
        if (a instanceof RawString && b instanceof RawString) {
            return ((RawString) a).compareTo((RawString) b);
        }
        if (a instanceof RawString && b instanceof String) {
            return ((RawString) a).compareToString((String) b);
        }
        if (a instanceof String && b instanceof RawString) {
            return -((RawString) b).compareToString((String) a);
        }
        if (a instanceof Integer && b instanceof Integer) {
            return ((Integer) a) - ((Integer) b);
        }
        if (a instanceof Long && b instanceof Long) {
            double delta = ((Long) a) - ((Long) b);
            return delta == 0 ? 0 : delta > 0 ? 1 : -1;
        }
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        if (a instanceof java.util.Date && b instanceof java.util.Date) {
            long delta = ((java.util.Date) a).getTime() - ((java.util.Date) b).getTime();
            return delta == 0 ? 0 : delta > 0 ? 1 : -1;
        }
        if (a instanceof java.util.Date && b instanceof java.lang.Long) {
            long delta = ((java.util.Date) a).getTime() - ((Long) b);
            return delta == 0 ? 0 : delta > 0 ? 1 : -1;
        }
        if (a instanceof Long && b instanceof java.util.Date) {
            long delta = ((Long) a) - ((java.util.Date) b).getTime();
            return delta == 0 ? 0 : delta > 0 ? 1 : -1;
        }
        if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
            return ((Comparable) a).compareTo(b);
        }
        throw new IllegalArgumentException("uncompable objects " + a.getClass() + " vs " + b.getClass());
    }

    public static Object add(Object a, Object b) throws IllegalArgumentException {
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
            return ((Long) a) + ((Long) b);
        }
        if (a instanceof Integer && b instanceof Integer) {
            return (long) (((Integer) a) + ((Integer) b));
        }
        if (a instanceof Integer && b instanceof Long) {
            return (((Integer) a) + ((Long) b));
        }
        if (a instanceof Long && b instanceof Integer) {
            return (((Long) a) + ((Integer) b));
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() + ((Number) b).doubleValue();
        }
        throw new IllegalArgumentException("cannot add " + a + " and " + b);
    }

    public static Object subtract(Object a, Object b) throws IllegalArgumentException {
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
            return ((Long) a) - ((Long) b);
        }
        if (a instanceof Integer && b instanceof Integer) {
            return (long) (((Integer) a) - ((Integer) b));
        }
        if (a instanceof Integer && b instanceof Long) {
            return (((Integer) a) - ((Long) b));
        }
        if (a instanceof Long && b instanceof Integer) {
            return (((Long) a) - ((Integer) b));
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() - ((Number) b).doubleValue();
        }
        throw new IllegalArgumentException("cannot subtract " + a + " and " + b);
    }

    public static Object multiply(Object a, Object b) throws IllegalArgumentException {
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
            return ((Long) a) * ((Long) b);
        }
        if (a instanceof Integer && b instanceof Integer) {
            return (long) (((Integer) a) * ((Integer) b));
        }
        if (a instanceof Integer && b instanceof Long) {
            return (((Integer) a) * ((Long) b));
        }
        if (a instanceof Long && b instanceof Integer) {
            return (((Long) a) * ((Integer) b));
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() * ((Number) b).doubleValue();
        }
        throw new IllegalArgumentException("cannot multiply " + a + " and " + b);
    }

    public static Object divide(Object a, Object b) throws IllegalArgumentException {
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

    public static boolean objectEquals(Object a, Object b) {
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

    public static Pattern compileLikePattern(String b) {        
        String like = b
                .replace(".", "\\.")
                .replace("\\*", "\\*")
                .replace("%", ".*")
                .replace("_", ".?");

        return Pattern.compile(like, Pattern.DOTALL);
    }
    public static boolean like(Object a, Object b) {
        if (a == null || b == null) {
            return false;
        }
        Pattern pattern = compileLikePattern(b.toString());
        return matches(a, pattern);
    }
    
    public static boolean matches(Object a, Pattern pattern) {
        if (a == null) {
            return false;
        }        
        Matcher matcher = pattern.matcher(a.toString());
        return matcher.matches();
    }

}
