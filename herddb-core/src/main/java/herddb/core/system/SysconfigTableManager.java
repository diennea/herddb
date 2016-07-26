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
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;
import herddb.server.ServerConfiguration;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Table Manager for the SYSCONFIG virtual table
 *
 * @author enrico.olivelli
 */
public class SysconfigTableManager extends AbstractSystemTableManager {

    private final static Table TABLE = Table
            .builder()
            .name("sysconfig")
            .column("name", ColumnTypes.STRING)
            .column("value", ColumnTypes.STRING)
            .primaryKey("name", false)
            .build();

    public SysconfigTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList() {
        ServerConfiguration configuration = tableSpaceManager.getDbmanager().getConfiguration();
        return configuration.keys()
                .stream()
                .map(r -> RecordSerializer.makeRecord(table, "name", r, "value", configuration.getString(r, "")))
                .collect(Collectors.toList());
    }

}
