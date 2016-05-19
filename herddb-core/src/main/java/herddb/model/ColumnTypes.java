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
            default:
                return "type?" + type;
        }
    }

}
