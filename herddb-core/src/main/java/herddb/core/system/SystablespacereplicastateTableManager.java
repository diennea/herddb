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
import herddb.core.TableSpaceManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TableSpaceReplicaState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Table Manager for the SYSTABLESPACES virtual table
 *
 * @author enrico.olivelli
 */
public class SystablespacereplicastateTableManager extends AbstractSystemTableManager {

    private final static Table TABLE = Table
        .builder()
        .name("systablespacereplicastate")
        .column("tablespace_name", ColumnTypes.STRING)
        .column("uuid", ColumnTypes.STRING)
        .column("nodeid", ColumnTypes.STRING)
        .column("mode", ColumnTypes.STRING)
        .column("timestamp", ColumnTypes.TIMESTAMP)
        .column("maxleaderinactivitytime", ColumnTypes.LONG)
        .column("inactivitytime", ColumnTypes.LONG)
        .primaryKey("uuid", false)
        .primaryKey("nodeid", false)
        .build();

    public SystablespacereplicastateTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList() throws StatementExecutionException {
        try {
            Collection<String> names = tableSpaceManager.getMetadataStorageManager().listTableSpaces();
            long now = System.currentTimeMillis();
            List<Record> result = new ArrayList<>();
            for (String name : names) {
                TableSpace t = tableSpaceManager.getMetadataStorageManager().describeTableSpace(name);
                if (t != null) {
                    List<TableSpaceReplicaState> tableSpaceReplicaStates = tableSpaceManager.getMetadataStorageManager().getTableSpaceReplicaState(t.uuid);
                    for (TableSpaceReplicaState state : tableSpaceReplicaStates) {
                        result.add(RecordSerializer.makeRecord(
                            table,
                            "tablespace_name", t.name,
                            "uuid", t.uuid,
                            "nodeid", state.nodeId,
                            "timestamp", new java.sql.Timestamp(state.timestamp),
                            "maxleaderinactivitytime", t.maxLeaderInactivityTime,
                            "inactivitytime", now - state.timestamp,
                            "mode", TableSpaceReplicaState.modeToSQLString(state.mode))
                        );
                    }
                }
            }
            return result;
        } catch (MetadataStorageManagerException error) {
            throw new StatementExecutionException(error);
        }
    }

}
