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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Table definition
 *
 * @author enrico.olivelli
 */
public class Table {

    public final String name;
    public final String tablespace;
    public final Column[] columns;
    public final Map<String, Column> columnsByName;
    public final String[] primaryKey;
    private final Set<String> primaryKeyColumns;

    private Table(String name, Column[] columns, String[] primaryKey, String tablespace) {
        this.name = name;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.tablespace = tablespace;
        this.columnsByName = new HashMap<>();
        for (Column c : columns) {
            columnsByName.put(c.name.toLowerCase(), c);
        }
        this.primaryKeyColumns = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(primaryKey)));
    }

    public boolean isPrimaryKeyColumn(String column) {
        return primaryKeyColumns.contains(column);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Table deserialize(byte[] data) {
        try {
            ByteArrayInputStream ii = new ByteArrayInputStream(data);
            DataInputStream dii = new DataInputStream(ii);
            String tablespace = dii.readUTF();
            String name = dii.readUTF();
            byte pkcols = dii.readByte();
            String[] primaryKey = new String[pkcols];
            for (int i = 0; i < pkcols; i++) {
                primaryKey[i] = dii.readUTF();
            }
            int ncols = dii.readInt();
            Column[] columns = new Column[ncols];
            for (int i = 0; i < ncols; i++) {
                String cname = dii.readUTF();
                int type = dii.readInt();
                columns[i] = Column.column(cname, type);
            }
            return new Table(name, columns, primaryKey, tablespace);
        } catch (IOException err) {
            throw new IllegalArgumentException(err);
        }
    }

    public byte[] serialize() {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (DataOutputStream doo = new DataOutputStream(oo);) {
            doo.writeUTF(tablespace);
            doo.writeUTF(name);
            doo.writeByte(primaryKey.length);
            for (String primaryKeyColumn : primaryKey) {
                doo.writeUTF(primaryKeyColumn);
            }
            doo.writeInt(columns.length);
            for (Column c : columns) {
                doo.writeUTF(c.name);
                doo.writeInt(c.type);
            }
        } catch (IOException ee) {
            throw new RuntimeException(ee);
        }
        return oo.toByteArray();
    }

    public Column getColumn(String cname) {
        return columnsByName.get(cname.toLowerCase());
    }

    public static class Builder {

        private final List<Column> columns = new ArrayList<>();
        private String name;
        private List<String> primaryKey = new ArrayList<>();
        private String tablespace = TableSpace.DEFAULT;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder tablespace(String tablespace) {
            this.tablespace = tablespace;
            return this;
        }

        public Builder primaryKey(String pk) {
            if (pk == null || pk.isEmpty()) {
                throw new IllegalArgumentException();
            }
            if (!this.primaryKey.contains(pk)) {
                this.primaryKey.add(pk);
            }
            return this;
        }

        public Builder column(String name, int type) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException();
            }
            this.columns.add(Column.column(name, type));
            return this;
        }

        public Table build() {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name is not defined");
            }
            if (primaryKey.isEmpty()) {
                throw new IllegalArgumentException("primary key is not defined");
            }
            for (String pkColumn : primaryKey) {
                Column pk = columns.stream().filter(c -> c.name.equals(pkColumn)).findAny().orElse(null);
                if (pk == null) {
                    throw new IllegalArgumentException("column " + pkColumn + " is not defined in table");
                }
                if (pk.type != ColumnTypes.STRING 
                        && pk.type != ColumnTypes.LONG 
                        && pk.type != ColumnTypes.INTEGER
                        && pk.type != ColumnTypes.TIMESTAMP
                        ) {
                    throw new IllegalArgumentException("primary key " + pkColumn + " must be a string or long or integer or timestamp");
                }
            }

            return new Table(name, columns.toArray(new Column[columns.size()]), primaryKey.toArray(new String[primaryKey.size()]), tablespace);
        }

    }

}
