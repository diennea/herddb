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
import herddb.core.RunningStatementInfo;
import herddb.core.TableSpaceManager;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Table Manager for the SYSSTATEMENTS virtual table
 *
 * @author enrico.olivelli
 */
public class SysstatementsTableManager extends AbstractSystemTableManager {

    private final static Table TABLE = Table
            .builder()
            .name("sysstatements")
            .column("id", ColumnTypes.NOTNULL_LONG)
            .column("tablespace", ColumnTypes.STRING)
            .column("query", ColumnTypes.STRING)
            .column("startts", ColumnTypes.TIMESTAMP)
            .column("runningtime", ColumnTypes.LONG)
            .column("batches", ColumnTypes.INTEGER)
            .column("info", ColumnTypes.STRING)
            .primaryKey("id", false)
            .build();

    public SysstatementsTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList() {
        ConcurrentHashMap<Long, RunningStatementInfo> runningStatements = tableSpaceManager.getDbmanager().getRunningStatements().getRunningStatements();
        List<Record> result = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (RunningStatementInfo info : runningStatements.values()) {

            result.add(RecordSerializer.makeRecord(
                    table,
                    "id", info.getId(),
                    "tablespace", info.getTablespace(),
                    "query", info.getQuery(),
                    "startts", new java.sql.Timestamp(info.getStartTimestamp()),
                    "runningtime", (now - info.getStartTimestamp()),
                    "batches", info.getNumBatches(),
                    "info", info.getInfo())
            );
        }
        return result;
    }

}
