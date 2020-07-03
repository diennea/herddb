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

import herddb.utils.Bytes;

/**
 * Definition of a column
 *
 * @author enrico.olivelli
 */
public class Column {

    public static String[] buildFieldNamesList(Column[] schema) {
        String[] result = new String[schema.length];
        for (int i = 0; i < schema.length; i++) {
            result[i] = schema[i].name;
        }
        return result;
    }

    public final int serialPosition;

    public final String name;

    /**
     * @see ColumnTypes
     */
    public final int type;
    
    /**
     * Default values, pre-encoded
     */
    public final Bytes defaultValue;

    private Column(String name, int type, int serialPosition, Bytes defaultValue) {
        this.name = name;
        this.type = type;
        this.serialPosition = serialPosition;
        this.defaultValue = defaultValue;
    }

    public static Column column(String name, int type) {
        return new Column(name, type, -1, null);
    }

    public static Column column(String name, int type, int serialPosition) {
        return new Column(name, type, serialPosition, null);
    }
    
    public static Column column(String name, int type, int serialPosition, Bytes defaultValue) {
        return new Column(name, type, serialPosition, defaultValue);
    }
    
    public  String getName(){
        return name;
    }

    @Override
    public String toString() {
        return "{" + "name=" + name + ", type=" + type + '}';
    }

}
