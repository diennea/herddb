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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.commands.AlterTableStatement;
import herddb.sql.expressions.BindableTableScanColumnNameResolver;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;

/**
 * Table definition
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class Table implements ColumnsList, BindableTableScanColumnNameResolver {

    public final String uuid;
    public final String name;
    public final String tablespace;
    public final Column[] columns;
    public final String[] columnNames;
    public final Map<String, Column> columnsByName;
    public final Map<Integer, Column> columnsBySerialPosition;
    public final String[] primaryKey;
    public final int[] primaryKeyProjection;
    public final boolean auto_increment;
    private final Set<String> primaryKeyColumns;
    public final int maxSerialPosition;

    private Table(String uuid, String name, Column[] columns, String[] primaryKey, String tablespace, boolean auto_increment, int maxSerialPosition) {
        this.uuid = uuid;
        this.name = name;
        this.columns = reorderColumnsPrimaryKeyFirst(columns, primaryKey);
        this.maxSerialPosition = maxSerialPosition;
        this.primaryKey = primaryKey;
        this.tablespace = tablespace;
        this.columnsByName = new HashMap<>();
        this.columnsBySerialPosition = new HashMap<>();
        this.auto_increment = auto_increment;
        this.columnNames = new String[columns.length];
        int i = 0;
        this.primaryKeyProjection = new int[columns.length];
        for (Column c : columns) {
            String cname = c.name.toLowerCase();
            columnsByName.put(cname, c);
            if (c.serialPosition < 0) {
                throw new IllegalArgumentException();
            }
            columnsBySerialPosition.put(c.serialPosition, c);
            columnNames[i] = cname;
            primaryKeyProjection[i] = findPositionInArray(cname, primaryKey);
            i++;
        }
        this.primaryKeyColumns = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(primaryKey)));

    }

    private static Column[] reorderColumnsPrimaryKeyFirst(Column[] columns, String[] primaryKey) throws IllegalStateException, IllegalArgumentException {
        Column[] _columns = new Column[columns.length];
        int pos = 0;
        Set<String> pkCols = new HashSet<>();
        for (String pkCol : primaryKey) {
            pkCols.add(pkCol.toLowerCase());
            boolean found = false;
            for (Column column : columns) {
                if (column.name.equalsIgnoreCase(pkCol)) {
                    _columns[pos++] = column;
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("pk col " + pkCol + " not found in columns definition " + Arrays.toString(columns));
            }
        }
        for (Column column : columns) {
            if (!pkCols.contains(column.name.toLowerCase())) {
                _columns[pos++] = column;
            }
        }
        if (pos != columns.length) {
            throw new IllegalStateException();
        }
        return _columns;
    }

    public boolean isPrimaryKeyColumn(String column) {
        return primaryKeyColumns.contains(column);
    }

    public boolean isPrimaryKeyColumn(int index) {
        return primaryKeyProjection[index] >= 0;
    }

    public static Builder builder() {
        return new Builder();
    }

    @SuppressFBWarnings("OS_OPEN_STREAM")
    public static Table deserialize(byte[] data) {
        try {
            SimpleByteArrayInputStream ii = new SimpleByteArrayInputStream(data);
            ExtendedDataInputStream dii = new ExtendedDataInputStream(ii);
            long tversion = dii.readVLong(); // version
            long tflags = dii.readVLong(); // flags for future implementations
            if (tversion != 1 || tflags != 0) {
                throw new IOException("corrupted table file");
            }
            String tablespace = dii.readUTF();
            String name = dii.readUTF();
            String uuid = dii.readUTF();
            boolean auto_increment = dii.readByte() > 0;
            int maxSerialPosition = dii.readVInt();
            byte pkcols = dii.readByte();
            String[] primaryKey = new String[pkcols];
            for (int i = 0; i < pkcols; i++) {
                primaryKey[i] = dii.readUTF();
            }
            dii.readVInt(); // for future implementations
            int ncols = dii.readVInt();
            Column[] columns = new Column[ncols];
            for (int i = 0; i < ncols; i++) {
                long cversion = dii.readVLong(); // version
                long cflags = dii.readVLong(); // flags for future implementations
                if (cversion != 1 || cflags != 0) {
                    throw new IOException("corrupted table file");
                }
                String cname = dii.readUTF();
                int type = dii.readVInt();
                int serialPosition = dii.readVInt();
                columns[i] = Column.column(cname, type, serialPosition);
            }
            return new Table(uuid, name, columns, primaryKey, tablespace, auto_increment, maxSerialPosition);
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
            doo.writeByte(auto_increment ? 1 : 0);
            doo.writeVInt(maxSerialPosition);
            doo.writeByte(primaryKey.length);
            for (String primaryKeyColumn : primaryKey) {
                doo.writeUTF(primaryKeyColumn);
            }
            doo.writeVInt(0); // flags for future implementations
            doo.writeVInt(columns.length);
            for (Column c : columns) {
                doo.writeVLong(1); // version
                doo.writeVLong(0); // flags for future implementations
                doo.writeUTF(c.name);
                doo.writeVInt(c.type);
                doo.writeVInt(c.serialPosition);
            }
        } catch (IOException ee) {
            throw new RuntimeException(ee);
        }
        return oo.toByteArray();
    }

    @Override
    public Column getColumn(String cname) {
        return columnsByName.get(cname);
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }

    @Override
    public Column resolveColumName(int columnReference) {
        return columns[columnReference];
    }

    @Override
    public String[] getPrimaryKey() {
        return primaryKey;
    }

    public Table applyAlterTable(AlterTableStatement alterTableStatement) {
        int new_maxSerialPosition = this.maxSerialPosition;
        String newTableName = alterTableStatement.getNewTableName() != null ? alterTableStatement.getNewTableName().toLowerCase()
                : this.name;

        Builder builder = builder()
                .name(newTableName)
                .uuid(this.uuid)
                .tablespace(this.tablespace);

        List<String> dropColumns = alterTableStatement.getDropColumns().stream().map(String::toLowerCase)
                .collect(Collectors.toList());
        for (String dropColumn : dropColumns) {
            if (this.getColumn(dropColumn) == null) {
                throw new IllegalArgumentException("column " + dropColumn + " not found int table " + this.name);
            }
            if (isPrimaryKeyColumn(dropColumn)) {
                throw new IllegalArgumentException("column " + dropColumn + " cannot be dropped because is part of the primary key of table " + this.name);
            }
        }
        Set<String> changedColumns = new HashSet<>();
        Map<Integer, Column> realStructure
                = Stream
                        .of(columns)
                        .collect(
                                Collectors.toMap(
                                        t -> t.serialPosition,
                                        Function.identity()
                                ));
        if (alterTableStatement.getModifyColumns() != null) {
            for (Column newColumn : alterTableStatement.getModifyColumns()) {
                Column oldColumn = realStructure.get(newColumn.serialPosition);
                if (oldColumn == null) {
                    throw new IllegalArgumentException("column " + newColumn.name + " not found int table " + this.name
                            + ", looking for serialPosition = " + newColumn.serialPosition);

                }
                changedColumns.add(oldColumn.name);
            }
        }

        for (Column c : this.columns) {
            String lowercase = c.name.toLowerCase();
            if (!dropColumns.contains(lowercase)
                    && !changedColumns.contains(lowercase)) {
                builder.column(c.name, c.type, c.serialPosition);
            }
            new_maxSerialPosition = Math.max(new_maxSerialPosition, c.serialPosition);
        }

        if (alterTableStatement.getAddColumns() != null) {
            for (Column c : alterTableStatement.getAddColumns()) {
                if (getColumn(c.name) != null) {
                    throw new IllegalArgumentException("column " + c.name + " already found int table " + this.name);
                }
                builder.column(c.name, c.type, ++new_maxSerialPosition);
            }
        }
        String[] newPrimaryKey = new String[primaryKey.length];
        System.arraycopy(primaryKey, 0, newPrimaryKey, 0, primaryKey.length);
        if (alterTableStatement.getModifyColumns() != null) {
            for (Column c : alterTableStatement.getModifyColumns()) {

                builder.column(c.name, c.type, c.serialPosition);
                new_maxSerialPosition = Math.max(new_maxSerialPosition, c.serialPosition);

                // RENAME PK
                Column oldcolumn = realStructure.get(c.serialPosition);
                if (isPrimaryKeyColumn(oldcolumn.name)) {
                    for (int i = 0; i < newPrimaryKey.length; i++) {
                        if (newPrimaryKey[i].equals(oldcolumn.name)) {
                            newPrimaryKey[i] = c.name;
                        }
                    }
                }
            }
        }
        boolean new_auto_increment = alterTableStatement.getChangeAutoIncrement() != null
                ? alterTableStatement.getChangeAutoIncrement()
                : this.auto_increment;
        for (String pk : newPrimaryKey) {
            builder.primaryKey(pk, new_auto_increment);
        }
        builder.maxSerialPosition(new_maxSerialPosition);
        return builder.build();

    }

    public Column getColumnBySerialPosition(int serialPosition) {
        return columnsBySerialPosition.get(serialPosition);
    }

    public int[] getPrimaryKeyProjection() {
        return primaryKeyProjection;
    }

    private static int findPositionInArray(String cname, String[] primaryKey) {
        for (int i = 0; i < primaryKey.length; i++) {
            if (primaryKey[i].equals(cname)) {
                return i;
            }
        }
        return -1;
    }

    public Column getColumn(int index) {
        return columns[index];
    }

    public static class Builder {

        private final List<Column> columns = new ArrayList<>();
        private String name;
        private String uuid;
        private final List<String> primaryKey = new ArrayList<>();
        private String tablespace = TableSpace.DEFAULT;
        private boolean auto_increment;
        private int maxSerialPosition = 0;

        private Builder() {
        }

        public Builder uuid(String uuid) {
            this.uuid = uuid.toLowerCase();
            return this;
        }

        public Builder name(String name) {
            this.name = name.toLowerCase();
            return this;
        }

        public Builder maxSerialPosition(int maxSerialPosition) {
            this.maxSerialPosition = maxSerialPosition;
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
            pk = pk.toLowerCase();
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
            String _name = name.toLowerCase();
            if (this.columns.stream().filter(c -> (c.name.equals(_name))).findAny().isPresent()) {
                throw new IllegalArgumentException("column " + name + " already exists");
            }
            this.columns.add(Column.column(_name, type, maxSerialPosition++));
            return this;
        }

        public Builder column(String name, int type, int serialPosition) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException();
            }
            String _name = name.toLowerCase();
            if (this.columns.stream().filter(c -> (c.name.equals(_name))).findAny().isPresent()) {
                throw new IllegalArgumentException("column " + name + " already exists");
            }
            this.columns.add(Column.column(_name, type, serialPosition));
            return this;
        }

        public Table build() {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name is not defined");
            }
            if (uuid == null || uuid.isEmpty()) {
                uuid = UUID.randomUUID().toString();
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

            columns.sort((Column o1, Column o2) -> o1.serialPosition - o2.serialPosition);

            return new Table(uuid, name,
                    columns.toArray(new Column[columns.size()]), primaryKey.toArray(new String[primaryKey.size()]),
                    tablespace, auto_increment, maxSerialPosition);
        }

        public Builder cloning(Table tableSchema) {
            this.columns.addAll(Arrays.asList(tableSchema.columns));
            this.name = tableSchema.name;
            this.uuid = tableSchema.uuid;
            this.primaryKey.addAll(Arrays.asList(tableSchema.primaryKey));
            this.tablespace = tableSchema.tablespace;
            this.auto_increment = tableSchema.auto_increment;
            this.maxSerialPosition = tableSchema.maxSerialPosition;
            return this;
        }

    }

}
