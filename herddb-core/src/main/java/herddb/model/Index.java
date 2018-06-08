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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import java.util.Arrays;
import java.util.UUID;

/**
 * Index definition
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public class Index implements ColumnsList {

    public static final String TYPE_HASH = "hash";
    public static final String TYPE_BRIN = "brin";

    public final String name;
    public final String uuid;
    public final String table;
    public final String type;
    public final String tablespace;
    public final Column[] columns;
    public final String[] columnNames;
    public final Map<String, Column> columnByName = new HashMap<>();

    @Override
    public String[] getPrimaryKey() {
        return columnNames;
    }

    private Index(String uuid,
            String name, String table, String tablespace, String type, Column[] columns) {
        this.name = name;
        this.uuid = uuid;
        this.table = table;
        this.tablespace = tablespace;
        this.columns = columns;
        this.type = type;
        this.columnNames = new String[columns.length];
        int i = 0;
        for (Column c : columns) {
            this.columnNames[i++] = c.name;
            columnByName.put(c.name, c);
        }
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }

    @Override
    public Column getColumn(String name) {
        return columnByName.get(name);
    }

    public static Builder builder() {
        return new Builder();
    }

    @SuppressFBWarnings("OS_OPEN_STREAM")
    public static Index deserialize(byte[] data) {
        try {
            SimpleByteArrayInputStream ii = new SimpleByteArrayInputStream(data);
            ExtendedDataInputStream dii = new ExtendedDataInputStream(ii);
            long iversion = dii.readVLong(); // version
            long iflags = dii.readVLong(); // flags for future implementations
            if (iversion != 1 || iflags != 0) {
                throw new IOException("corrupted index file");
            }
            String tablespace = dii.readUTF();
            String name = dii.readUTF();
            String uuid = dii.readUTF();
            String table = dii.readUTF();
            dii.readVInt(); // for future implementations
            String type = dii.readUTF();
            int ncols = dii.readVInt();
            Column[] columns = new Column[ncols];
            for (int i = 0; i < ncols; i++) {
                long cversion = dii.readVLong(); // version
                long cflags = dii.readVLong(); // flags for future implementations
                if (cversion != 1 || cflags != 0) {
                    throw new IOException("corrupted index file");
                }
                String cname = dii.readUTF();
                int ctype = dii.readVInt();
                int serialPosition = dii.readVInt();
                dii.readVInt(); // for future implementations
                columns[i] = Column.column(cname, ctype, serialPosition);
            }
            return new Index(uuid, name, table, tablespace, type, columns);
        } catch (IOException err) {
            throw new IllegalArgumentException(err);
        }
    }

    public byte[] serialize() {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(oo);) {
            doo.writeVLong(1); // version
            doo.writeVLong(0); // flags for future implementations
            doo.writeUTF(tablespace);
            doo.writeUTF(name);
            doo.writeUTF(uuid);
            doo.writeUTF(table);
            doo.writeVInt(0); // for future implementation
            doo.writeUTF(type);
            doo.writeVInt(columns.length);
            for (Column c : columns) {
                doo.writeVLong(1); // version
                doo.writeVLong(0); // flags for future implementations
                doo.writeUTF(c.name);
                doo.writeVInt(c.type);
                doo.writeVInt(c.serialPosition);
                doo.writeVInt(0); // flags for future implementations
            }
        } catch (IOException ee) {
            throw new RuntimeException(ee);
        }
        return oo.toByteArray();
    }

    public static class Builder {

        private final List<Column> columns = new ArrayList<>();
        private String name;
        private String uuid;
        private String table;
        private String type = TYPE_HASH;
        private String tablespace = TableSpace.DEFAULT;

        private Builder() {
        }

        public Builder onTable(Table table) {
            this.table = table.name;
            this.tablespace = table.tablespace;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder uuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder tablespace(String tablespace) {
            this.tablespace = tablespace;
            return this;
        }

        public Builder column(String name, int type) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException();
            }
            if (this.columns.stream().filter(c -> (c.name.equals(name))).findAny().isPresent()) {
                throw new IllegalArgumentException("column " + name + " already exists");
            }
            this.columns.add(Column.column(name, type, 0));
            return this;
        }

        public Index build() {
            if (table == null || table.isEmpty()) {
                throw new IllegalArgumentException("table is not defined");
            }
            if (!TYPE_HASH.equals(type) && !TYPE_BRIN.equals(type)) {
                throw new IllegalArgumentException("only index type " + TYPE_HASH + "," + TYPE_BRIN + " are supported");
            }
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("specify at least one column to index");
            }
            if (name == null || name.isEmpty()) {
                name = table + "_" + columns.stream().map(s -> s.name.toLowerCase()).collect(Collectors.joining("_"));
            }
            if (uuid == null || uuid.isEmpty()) {
                uuid = UUID.randomUUID().toString();
            }

            return new Index(uuid, name, table, tablespace, type, columns.toArray(new Column[columns.size()]));
        }

    }

    @Override
    public String toString() {
        return type + "INDEX{" + "name=" + table + '.' + name + " (" + Arrays.toString(columnNames) + ")";
    }

}
