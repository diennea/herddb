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
import herddb.model.NodeMetadata;
import herddb.model.Record;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Table Manager for the SYSNODES virtual table
 *
 * @author enrico.olivelli
 */
public class SysnodesTableManager extends AbstractSystemTableManager {

    private final static Table TABLE = Table
            .builder()
            .name("sysnodes")
            .column("nodeid", ColumnTypes.NOTNULL_STRING)
            .column("address", ColumnTypes.STRING)
            .column("ssl", ColumnTypes.INTEGER)
            .primaryKey("nodeid", false)
            .build();

    public SysnodesTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList() throws StatementExecutionException {
        try {
            Collection<NodeMetadata> nodes = tableSpaceManager.getMetadataStorageManager().listNodes();
            List<Record> result = new ArrayList<>();
            for (NodeMetadata t : nodes) {
                result.add(RecordSerializer.makeRecord(
                        table,
                        "nodeid", t.nodeId,
                        "address", t.host + ":" + t.port,
                        "ssl", t.ssl ? 1 : 0
                ));
            }
            return result;
        } catch (MetadataStorageManagerException error) {
            throw new StatementExecutionException(error);
        }
    }

}
