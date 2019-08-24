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

package herddb.sql.functions;

import herddb.core.AbstractTableManager;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import herddb.model.TableDoesNotExistException;
import java.util.List;
import java.util.StringJoiner;


/**
 * Utility class that is responsible for generating Show Create Table command
 *
 * @author amitchavan
 */
public class ShowCreateTableCalculator {

    public static String calculate(boolean showCreateIndex, String tableName, String tableSpace, AbstractTableManager tableManager) {

        Table t = tableManager.getTable();
        if (t == null) {
            throw new TableDoesNotExistException(String.format("Table %s does not exist.", tableName));
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE " + tableSpace + "." + tableName);
        StringJoiner joiner = new StringJoiner(",", "(", ")");
        for (Column c : t.getColumns()) {
            joiner.add(c.name + " " + ColumnTypes.typeToString(c.type) + autoIncrementColumn(t, c));
        }

        if (t.getPrimaryKey().length > 0) {
            joiner.add("PRIMARY KEY(" + String.join(",", t.getPrimaryKey()) + ")");
        }

        if (showCreateIndex) {
            List<Index> indexes = tableManager.getAvailableIndexes();

            if (!indexes.isEmpty()) {
                indexes.forEach(idx -> {
                    joiner.add("INDEX " + idx.name + "(" + String.join(",", idx.columnNames) + ")");
                });
            }
        }

        sb.append(joiner.toString());
        return sb.toString();
    }

    private static String autoIncrementColumn(Table t, Column c) {
        if (t.auto_increment
                && c.name.equals(t.primaryKey[0])
                && (c.type == ColumnTypes.INTEGER
                || c.type == ColumnTypes.NOTNULL_INTEGER
                || c.type == ColumnTypes.LONG
                || c.type == ColumnTypes.NOTNULL_LONG)) {
            return " auto_increment";
        }
        return "";
    }
}
