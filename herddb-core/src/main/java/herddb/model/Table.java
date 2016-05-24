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

import herddb.model.commands.AlterTableStatement;
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
    public final boolean auto_increment;
    private final Set<String> primaryKeyColumns;

    private Table(String name, Column[] columns, String[] primaryKey, String tablespace, boolean auto_increment) {
        this.name = name;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.tablespace = tablespace;
        this.columnsByName = new HashMap<>();
        this.auto_increment = auto_increment;
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
            boolean auto_increment = dii.readByte() > 0;
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
            return new Table(name, columns, primaryKey, tablespace, auto_increment);
        } catch (IOException err) {
            throw new IllegalArgumentException(err);
        }
    }

    public byte[] serialize() {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (DataOutputStream doo = new DataOutputStream(oo);) {
            doo.writeUTF(tablespace);
            doo.writeUTF(name);
            doo.writeByte(auto_increment ? 1 : 0);
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

    public Table applyAlterTable(AlterTableStatement alterTableStatement) {
        Builder builder = builder()
                .name(this.name)
                .tablespace(this.tablespace);
        List<String> dropColumns = alterTableStatement.getDropColumns();
        for (String dropColumn : dropColumns) {
            if (this.getColumn(dropColumn) == null) {
                throw new IllegalArgumentException("column " + dropColumn + " not found int table " + this.name);
            }
            if (isPrimaryKeyColumn(dropColumn)) {
                throw new IllegalArgumentException("column " + dropColumn + " cannot be dropped because is part of the primary key of table " + this.name);
            }
        }
        for (Column c : this.columns) {
            if (dropColumns == null || !dropColumns.contains(c.name.toLowerCase())) {
                builder.column(c.name, c.type);
            }
        }
        if (alterTableStatement.getAddColumns() != null) {
            for (Column c : alterTableStatement.getAddColumns()) {
                if (getColumn(c.name) != null) {
                    throw new IllegalArgumentException("column " + c.name + " not found int table " + this.name);
                }
                builder.column(c.name, c.type);
            }
        }
        for (String pk : this.primaryKey) {
            builder.primaryKey(pk, this.auto_increment);
        }
        return builder.build();

    }

    public static class Builder {

        private final List<Column> columns = new ArrayList<>();
        private String name;
        private List<String> primaryKey = new ArrayList<>();
        private String tablespace = TableSpace.DEFAULT;
        private boolean auto_increment;

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
            return primaryKey(pk, false);
        }

        public Builder primaryKey(String pk, boolean auto_increment) {
            if (pk == null || pk.isEmpty()) {
                throw new IllegalArgumentException();
            }
            if (this.auto_increment && auto_increment) {
                throw new IllegalArgumentException("auto_increment can be used only on one column");
            }
            if (auto_increment) {
                this.auto_increment = true;
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
            if (this.columns.stream().filter(c -> (c.name.equals(name))).findAny().isPresent()) {
                throw new IllegalArgumentException("column " + name + " already exists");
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
                        && pk.type != ColumnTypes.TIMESTAMP) {
                    throw new IllegalArgumentException("primary key " + pkColumn + " must be a string or long or integer or timestamp");
                }
            }

            return new Table(name, columns.toArray(new Column[columns.size()]), primaryKey.toArray(new String[primaryKey.size()]), tablespace, auto_increment);
        }

    }

}
