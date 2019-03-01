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
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import java.util.ArrayList;
import java.util.List;

/**
 * Table Manager for the SYSTABLESPACES virtual table
 *
 * @author enrico.olivelli
 */
public class SyslogstatusManager extends AbstractSystemTableManager {

    private final static Table TABLE = Table
            .builder()
            .name("syslogstatus")
            .column("tablespace_uuid", ColumnTypes.NOTNULL_STRING)
            .column("nodeid", ColumnTypes.NOTNULL_STRING)
            .column("tablespace_name", ColumnTypes.STRING)
            .column("ledger", ColumnTypes.LONG)
            .column("offset", ColumnTypes.LONG)
            .column("status", ColumnTypes.STRING)
            .primaryKey("tablespace_uuid", false)
            .primaryKey("nodeid", false)
            .build();

    public SyslogstatusManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList() throws StatementExecutionException {
        boolean isVirtual = tableSpaceManager.isVirtual();
        boolean isLeader = tableSpaceManager.isLeader();
        LogSequenceNumber logSequenceNumber = isVirtual ? null : tableSpaceManager.getLog().getLastSequenceNumber();
        List<Record> result = new ArrayList<>();
        result.add(RecordSerializer.makeRecord(
                table,
                "tablespace_uuid", tableSpaceManager.getTableSpaceUUID(),
                "nodeid", tableSpaceManager.getDbmanager().getNodeId(),
                "tablespace_name", tableSpaceManager.getTableSpaceName(),
                "ledger", isVirtual ? 0L : logSequenceNumber.ledgerId,
                "offset", isVirtual ? 0L : logSequenceNumber.offset,
                "status", isVirtual ? "virtual" : isLeader ? "leader" : "follower"
        ));
        return result;

    }

}
