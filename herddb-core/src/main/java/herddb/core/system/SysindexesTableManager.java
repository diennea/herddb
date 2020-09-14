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
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table Manager for the SYSINDEXES virtual table
 *
 * @author enrico.olivelli
 */
public class SysindexesTableManager extends AbstractSystemTableManager {

    private static final Table TABLE = Table
            .builder()
            .name("sysindexes")
            .column("tablespace", ColumnTypes.STRING)
            .column("table_name", ColumnTypes.STRING)
            .column("index_name", ColumnTypes.STRING)
            .column("index_uuid", ColumnTypes.STRING)
            .column("index_type", ColumnTypes.STRING)
            .column("unique", ColumnTypes.INTEGER)
            .primaryKey("table_name", false)
            .primaryKey("index_name", false)
            .build();

    public SysindexesTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList(Transaction transaction) {
        List<Table> tables = tableSpaceManager.getAllVisibleTables(transaction);
        return tables
                .stream()
                .flatMap((Table r) -> {
                    Map<String, AbstractIndexManager> indexesOnTable = tableSpaceManager.getIndexesOnTable(r.name);
                    if (indexesOnTable == null) {
                        // empty stream
                        return null;
                    }
                    return indexesOnTable.values().stream().map(i -> i.getIndex());
                })
                .map(r -> RecordSerializer.makeRecord(table,
                        "tablespace", r.tablespace,
                        "table_name", r.table,
                        "index_name", r.name,
                        "index_uuid", r.uuid,
                        "index_type", r.type,
                        "unique", r.unique ? 1 : 0
                ))
                .collect(Collectors.toList());
    }

}
