/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.sql.expressions;

import herddb.model.Column;

/**
 * Reference to a table + column
 * @author enrico.olivelli
 */
public final class ColumnRef {

    public final String name;
    public final String tableName; // can be an alias
    public final int type;

    public ColumnRef(String tableName, Column column) {
        this.tableName = tableName;
        this.name = column.name;
        this.type = column.type;
    }

    public ColumnRef(String name, String tableName, int type) {
        this.name = name;
        this.tableName = tableName;
        this.type = type;
    }

    public static Column[] toColumnsArray(ColumnRef[] a) {
        Column[] c = new Column[a.length];
        for (int i = 0; i < c.length; i++) {
            c[i] = a[i].toColumn();
        }
        return c;
    }

    public static ColumnRef[] toColumnsRefsArray(String tableName, Column[] a) {
        ColumnRef[] c = new ColumnRef[a.length];
        for (int i = 0; i < c.length; i++) {
            c[i] = new ColumnRef(tableName, a[i]);
        }
        return c;
    }

    public Column toColumn() {
        return Column.column(name, type);
    }

    @Override
    public String toString() {
        return tableName + "." + name + " (type=" + type + ")";
    }

}
