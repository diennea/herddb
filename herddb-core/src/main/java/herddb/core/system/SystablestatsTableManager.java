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

import java.util.ArrayList;
import java.util.List;

import herddb.codec.RecordSerializer;
import herddb.core.AbstractTableManager;
import herddb.core.TableSpaceManager;
import herddb.core.stats.TableManagerStats;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;

/**
 * Table Manager for the SYSTABLES virtual table
 *
 * @author enrico.olivelli
 */
public class SystablestatsTableManager extends AbstractSystemTableManager {

    private final static Table TABLE = Table
        .builder()
        .name("systablestats")
        .column("tablespace", ColumnTypes.STRING)
        .column("table_name", ColumnTypes.STRING)
        .column("systemtable", ColumnTypes.STRING)
        .column("tablesize", ColumnTypes.LONG)
        .column("loadedpages", ColumnTypes.INTEGER)
        .column("dirtypages", ColumnTypes.INTEGER)
        .column("dirtyrecords", ColumnTypes.LONG)
        .column("maxlogicalpagesize", ColumnTypes.LONG)
        .column("keysmemory", ColumnTypes.LONG)
        .column("buffersmemory", ColumnTypes.LONG)
        .column("dirtymemory", ColumnTypes.LONG)
        .primaryKey("tablespace", false)
        .primaryKey("table_name", false)
        .build();

    public SystablestatsTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList() {
        List<Table> tables = tableSpaceManager.getAllCommittedTables();
        List<Record> result = new ArrayList<>();
        for (Table r : tables) {
            AbstractTableManager tableManager = tableSpaceManager.getTableManager(r.name);
            if (tableManager != null && !tableManager.isSystemTable()) {
                TableManagerStats stats = tableManager.getStats();
                result.add(RecordSerializer.makeRecord(
                    table,
                    "tablespace", r.tablespace,
                    "table_name", r.name,
                    "systemtable", r.name.startsWith("sys") ? "true" : "false",
                    "tablesize", stats.getTablesize(),
                    "loadedpages", stats.getLoadedpages(),
                    "loadedpagescount", stats.getLoadedPagesCount(),
                    "unloadedloadedpagescount", stats.getUnloadedPagesCount(),
                    "dirtypages", stats.getDirtypages(),
                    "dirtyrecords", stats.getDirtyrecords(),
                    "maxlogicalpagesize", stats.getMaxLogicalPageSize(),
                    "keysmemory", stats.getKeysUsedMemory(),
                    "buffersmemory", stats.getBuffersUsedMemory()
                ));
            }
        }
        return result;
    }

}
