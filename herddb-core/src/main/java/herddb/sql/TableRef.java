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
package herddb.sql;

import net.sf.jsqlparser.schema.Table;

/**
 * Reference to a Table
 *
 * @author enrico.olivelli
 */
final class TableRef {

    static TableRef buildFrom(Table fromTable, String defaultTableSpace) {
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName();
        String tableAlias = tableName;
        if (fromTable.getAlias() != null && fromTable.getAlias().getName() != null) {
            tableAlias = fromTable.getAlias().getName();
        }
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        return new TableRef(tableSpace, tableName, tableAlias);
    }

    public final String tableSpace;
    public final String tableName;
    public final String tableAlias;

    public TableRef(String tableSpace, String tableName, String tableAlias) {
        this.tableSpace = tableSpace;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
    }

}
