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
package herddb.core.system;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;

/**
 * Table Manager for the SYSTABLES virtual table
 *
 * @author enrico.olivelli
 */
public class SyscolumnsTableManager extends AbstractSystemTableManager {

    private final static Table TABLE = Table
            .builder()
            .name("syscolumns")
            .column("table_name", ColumnTypes.STRING)
            .column("column_name", ColumnTypes.STRING)
            .column("ordinal_position", ColumnTypes.INTEGER)
            .column("is_nullable", ColumnTypes.INTEGER)
            .column("data_type", ColumnTypes.STRING)
            .column("auto_increment", ColumnTypes.INTEGER)
            .primaryKey("column_name", false)
            .primaryKey("table_name", false)
            .build();

    public SyscolumnsTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList() {
        List<Table> tables = tableSpaceManager.getAllCommittedTables();
        List<Record> result = new ArrayList<>();
        for (Table t : tables) {
            int pos = 1;
            for (Column c : t.columns) {
                boolean pk = t.isPrimaryKeyColumn(c.name);
                boolean nonNullCType = pk ? true : ColumnTypes.isNotNullDataType(c.type);
                String data_type = ColumnTypes.typeToString(c.type);

                result.add(RecordSerializer.makeRecord(
                        table,
                        "table_name", t.name,
                        "column_name", c.name,
                        "ordinal_position", pos++,
                        "is_nullable", (pk || nonNullCType) ? DatabaseMetaData.columnNoNulls : DatabaseMetaData.columnNullable,
                        "data_type", data_type,
                        "auto_increment", (pk && t.auto_increment)?1:0
                ));
            }
        }
        return result;
    }

}
