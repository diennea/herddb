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

import herddb.codec.RecordSerializer;
import herddb.core.AbstractIndexManager;
import herddb.core.TableSpaceManager;
import herddb.index.blink.BLinkKeyToPageIndex;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table Manager for the SYSINDEXCOLUMNS virtual table
 *
 * @author enrico.olivelli
 */
public class SysindexcolumnsTableManager extends AbstractSystemTableManager {

    private static final Table TABLE = Table
            .builder()
            .name("sysindexcolumns")
            .column("tablespace", ColumnTypes.STRING)
            .column("table_name", ColumnTypes.STRING)
            .column("index_name", ColumnTypes.STRING)
            .column("index_uuid", ColumnTypes.STRING)
            .column("index_type", ColumnTypes.STRING)
            .column("column_name", ColumnTypes.STRING)
            .column("ordinal_position", ColumnTypes.INTEGER)
            .column("clustered", ColumnTypes.INTEGER)
            .column("unique", ColumnTypes.INTEGER)
            .primaryKey("table_name", false)
            .primaryKey("index_name", false)
            .primaryKey("column_name", false)
            .build();

    public SysindexcolumnsTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList() {
        List<Table> tables = tableSpaceManager.getAllCommittedTables();
        List<Record> result = new ArrayList<>();
        tables.forEach(r -> {
            // PK
            int posPk = 0;
            for (String pk : r.getPrimaryKey()) {
                Column column = r.getColumn(pk);
                String indexName = BLinkKeyToPageIndex.deriveIndexName(r.name);
                result.add(RecordSerializer.makeRecord(table,
                        "index_type", "pk",
                        "column_name", column.name,
                        "ordinal_position", posPk++,
                        "clustered", 1,
                        "unique", 1,
                        "tablespace", r.tablespace,
                        "table_name", r.name,
                        "index_name", indexName,
                        "index_uuid", indexName // uuid = index_name in BLink !
                ));
            }

            Map<String, AbstractIndexManager> indexesOnTable = tableSpaceManager.getIndexesOnTable(r.name);
            if (indexesOnTable != null) {
                indexesOnTable.values().forEach(indexManager -> {
                    Index index = indexManager.getIndex();
                    int pos = 0;
                    for (Column cc : index.getColumns()) {
                        result.add(RecordSerializer.makeRecord(table,
                                "tablespace", r.tablespace,
                                "table_name", r.name,
                                "index_name", index.name,
                                "index_uuid", index.uuid,
                                "index_type", index.type,
                                "column_name", cc.name,
                                "ordinal_position", pos++,
                                "clustered", 0,
                                "unique", index.unique ? 1 : 0
                        ));
                    }
                });
            }
        });
        return result;
    }

}
