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

package herddb.backup;

import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Table;
import java.util.List;

/**
 * Data about the dump of a table. We have to persist the schema and the position of the log at the time of the dump in
 * order to recover correctly the data
 *
 * @author enrico.olivelli
 */
public class DumpedTableMetadata {

    public final Table table;
    public final LogSequenceNumber logSequenceNumber;
    public List<Index> indexes;

    public DumpedTableMetadata(Table table, LogSequenceNumber logSequenceNumber, List<Index> indexes) {
        this.table = table;
        this.logSequenceNumber = logSequenceNumber;
        this.indexes = indexes;
    }

    @Override
    public String toString() {
        return "DumpedTableMetadata{" + "table=" + table + ", logSequenceNumber=" + logSequenceNumber + ", indexes=" + indexes + '}';
    }

}
