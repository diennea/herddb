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

package herddb.model;

import java.sql.Types;

/**
 * Column types
 *
 * @author enrico.olivelli
 */
public class ColumnTypes {

    public static final int STRING = 0;
    public static final int LONG = 1;
    public static final int INTEGER = 2;
    public static final int BYTEARRAY = 3;
    public static final int TIMESTAMP = 4;
    public static final int NULL = 5;
    public static final int DOUBLE = 6;
    public static final int BOOLEAN = 7;
    public static final int FLOATARRAY = 8;
    public static final int ANYTYPE = 10;

    public static final int NOTNULL_STRING = 11;
    public static final int NOTNULL_INTEGER = 12;
    public static final int NOTNULL_LONG = 13;
    public static final int NOTNULL_BYTEARRAY = 14;
    public static final int NOTNULL_TIMESTAMP = 15;
    public static final int NOTNULL_DOUBLE = 16;
    public static final int NOTNULL_BOOLEAN = 17;

    public static final int NOTNULL_FLOATARRAY = 18;


    public static String typeToString(int type) {
        switch (type) {
            case STRING:
                return "string";
            case LONG:
                return "long";
            case INTEGER:
                return "integer";
            case BYTEARRAY:
                return "bytearray";
            case FLOATARRAY:
                return "floatarray";
            case TIMESTAMP:
                return "timestamp";
            case NULL:
                return "null";
            case DOUBLE:
                return "double";
            case BOOLEAN:
                return "boolean";
            case NOTNULL_STRING:
                return "string not null";
            case NOTNULL_INTEGER:
                return "integer not null";
            case NOTNULL_LONG:
                return "long not null";
            case NOTNULL_DOUBLE:
                return "double not null";
            case NOTNULL_BYTEARRAY:
                return "bytearray not null";
            case NOTNULL_TIMESTAMP:
                return "timestamp not null";
            case NOTNULL_BOOLEAN:
                return "boolean not null";
            default:
                return "type?" + type;
        }
    }

    public static boolean isNotNullDataType(int type) {
        switch (type) {
            case NOTNULL_INTEGER:
            case NOTNULL_LONG:
            case NOTNULL_STRING:
            case NOTNULL_BOOLEAN:
            case NOTNULL_BYTEARRAY:
            case NOTNULL_DOUBLE:
            case NOTNULL_TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    /**
     * Utility method that takes in type and returns the supported not null equivalent. If the current
     * version does not support not null constraints on the type the method throws an exception
     *
     * @param type
     * @return
     * @throws StatementExecutionException
     */
    public static int getNonNullTypeForPrimitiveType(int type) throws StatementExecutionException {
        switch (type) {
            case STRING:
            case NOTNULL_STRING:
                return NOTNULL_STRING;
            case INTEGER:
            case NOTNULL_INTEGER:
                return NOTNULL_INTEGER;
            case LONG:
            case NOTNULL_LONG:
                return NOTNULL_LONG;
            case BYTEARRAY:
            case NOTNULL_BYTEARRAY:
                return NOTNULL_BYTEARRAY;
            case FLOATARRAY:
            case NOTNULL_FLOATARRAY:
                return NOTNULL_FLOATARRAY;
            case TIMESTAMP:
            case NOTNULL_TIMESTAMP:
                return NOTNULL_TIMESTAMP;
            case DOUBLE:
            case NOTNULL_DOUBLE:
                return NOTNULL_DOUBLE;
            case BOOLEAN:
            case NOTNULL_BOOLEAN:
                return NOTNULL_BOOLEAN;
            case NULL:
            case ANYTYPE:
                // not much sense
                return type;
            default:
                throw new StatementExecutionException("Not null constraints not supported for column type " + type);
        }
    }


    /**
     * Convert HerdDB Type to Metadata type.
     * @param type
     * @return the mapped value
     * @see #sqlDataTypeToJdbcType(java.lang.String)
     */
    public static String sqlDataType(int type) {
        switch (type) {
            case STRING:
            case NOTNULL_STRING:
                return "string";
            case LONG:
            case NOTNULL_LONG:
                return "long";
            case INTEGER:
            case NOTNULL_INTEGER:
                return "integer";
            case BYTEARRAY:
            case NOTNULL_BYTEARRAY:
                return "bytearray";
            case FLOATARRAY:
            case NOTNULL_FLOATARRAY:
                return "floatarray";
            case TIMESTAMP:
            case NOTNULL_TIMESTAMP:
                return "timestamp";
            case NULL:
                return "null";
            case DOUBLE:
            case NOTNULL_DOUBLE:
                return "double";
            case BOOLEAN:
            case NOTNULL_BOOLEAN:
                return "boolean";
            default:
                return "type?" + type;
        }
    }

    /**
     * Convert metadata type to java.sql.Types (used on client side, JDBC driver).
     * @param type
     * @return the mapped value
     */
    public static int sqlDataTypeToJdbcType(String type) {
        switch (type) {
            case "string":
                return Types.VARCHAR;
            case "long":
                return Types.BIGINT;
            case "integer":
                return Types.INTEGER;
            case "bytearray":
                return Types.BLOB;
            case "timestamp":
                return Types.TIMESTAMP;
            case "null":
                return Types.NULL;
            case "double":
                return Types.DOUBLE;
            case "boolean":
                return Types.BOOLEAN;
            default:
                return Types.OTHER;
        }
    }

    public static boolean sameRawDataType(int type, int otherType) {
        if (type == otherType) {
            return true;
        }
        return getNonNullTypeForPrimitiveType(type) == getNonNullTypeForPrimitiveType(otherType);
    }

    public static boolean isNotNullToNullConversion(int oldType, int newType) {
        return (ColumnTypes.isNotNullDataType(oldType)
                                && !ColumnTypes.isNotNullDataType(newType)
                                && ColumnTypes.getNonNullTypeForPrimitiveType(newType) == oldType);
    }

    public static boolean isNullToNotNullConversion(int oldType, int newType) {
        return (ColumnTypes.isNotNullDataType(newType)
                                && !ColumnTypes.isNotNullDataType(oldType)
                                && ColumnTypes.getNonNullTypeForPrimitiveType(oldType) == newType);
    }
}
