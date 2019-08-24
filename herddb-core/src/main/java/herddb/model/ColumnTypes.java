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
    public static final int ANYTYPE = 10;

    public static final int NOTNULL_STRING = 11;
    public static final int NOTNULL_INTEGER = 12;
    public static final int NOTNULL_LONG = 13;


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
            default:
                return "type?" + type;
        }
    }

    public static boolean isNotNullDataType(int type) {
        switch (type) {
            case NOTNULL_INTEGER:
            case NOTNULL_LONG:
            case NOTNULL_STRING:
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
                return NOTNULL_STRING;
            case INTEGER:
                return NOTNULL_INTEGER;
            case LONG:
                return NOTNULL_LONG;
            case BYTEARRAY:
            case TIMESTAMP:
            case DOUBLE:
            case BOOLEAN:
            case NULL:
            default:
                throw new StatementExecutionException("Not null constraints not supported for column type " + type);
        }
    }


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
                return "bytearray";
            case TIMESTAMP:
                return "timestamp";
            case NULL:
                return "null";
            case DOUBLE:
                return "double";
            case BOOLEAN:
                return "boolean";
            default:
                return "type?" + type;
        }
    }

}
