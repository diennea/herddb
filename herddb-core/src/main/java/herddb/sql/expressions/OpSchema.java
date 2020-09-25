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

import java.util.Arrays;

/**
 * Represents metadata about a step of the Planner.
 */
public final class OpSchema {
    
    public final String tableSpace;
    public final String name;
    public final String alias;
    public final ColumnRef[] columns;
    public final String[] columnNames;
    

    public OpSchema(String tableSpace, String tableName, String alias, String[] columnNames, ColumnRef[] tableSchema) {
        this.tableSpace = tableSpace;
        this.name = tableName;
        this.alias = alias;
        this.columns = tableSchema;
        this.columnNames = columnNames;
    }
    
    public OpSchema(String tableSpace, String name, String alias, ColumnRef[] tableSchema) {
        this.tableSpace = tableSpace;
        this.name = name;
        this.alias = alias;
        this.columns = tableSchema;
        // no aliases
        String[] _columnNames = new String[tableSchema.length];
        for (int i = 0; i < tableSchema.length; i++) {
            _columnNames[i] = tableSchema[i].name;
        }
        this.columnNames = _columnNames;
    }
    
    public boolean isTableOrAlias(String name) {
        if (name == null) {
            return false;
        }
        if (name.equalsIgnoreCase(this.name)) {
                return true;
        }
        if (name.equalsIgnoreCase(this.alias)) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "OpSchema{" + "tableSpace=" + tableSpace + ", name=" + name + ", alias=" + alias
                + ", columns=" + Arrays.toString(columns) + ", columnNames=" + Arrays.toString(columnNames) + '}';
    }
    
}
