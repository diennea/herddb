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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public final String primaryKeyColumn;

    private Table(String name, Column[] columns, String primaryKeyColumn, String tablespace) {
        this.name = name;
        this.columns = columns;
        this.primaryKeyColumn = primaryKeyColumn;
        this.tablespace = tablespace;
        this.columnsByName = new HashMap<>();
        for (Column c : columns) {
            columnsByName.put(c.name, c);
        }
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
            String primaryKeyColumn = dii.readUTF();
            int ncols = dii.readInt();
            Column[] columns = new Column[ncols];
            for (int i = 0; i < ncols; i++) {
                String cname = dii.readUTF();
                int type = dii.readInt();
                columns[i] = Column.column(cname, type);
            }
            return new Table(name, columns, primaryKeyColumn, tablespace);
        } catch (IOException err) {
            throw new IllegalArgumentException(err);
        }
    }

    public byte[] serialize() {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (DataOutputStream doo = new DataOutputStream(oo);) {
            doo.writeUTF(tablespace);
            doo.writeUTF(name);
            doo.writeUTF(primaryKeyColumn);
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
        return columnsByName.get(cname);
    }

    public static class Builder {

        private final List<Column> columns = new ArrayList<>();
        private String name;
        private String primaryKeyColumn;
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
            this.primaryKeyColumn = pk;
            return this;
        }

        public Builder column(String name, int type) {
            this.columns.add(Column.column(name, type));
            return this;
        }

        public Table build() {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name is not defined");
            }
            if (primaryKeyColumn == null || primaryKeyColumn.isEmpty()) {
                throw new IllegalArgumentException("primary key is not defined");
            }
            Column pk = columns.stream().filter(c -> c.name.equals(primaryKeyColumn)).findAny().orElse(null);
            if (pk == null) {
                throw new IllegalArgumentException("column " + primaryKeyColumn + " is not defined in table");
            }
            if (pk.type != ColumnTypes.STRING && pk.type != ColumnTypes.LONG) {
                throw new IllegalArgumentException("primary key " + primaryKeyColumn + " must be a string or long");
            }
            return new Table(name, columns.toArray(new Column[columns.size()]), primaryKeyColumn, tablespace);
        }

    }

}
