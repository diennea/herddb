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
package herddb.client;

import herddb.backup.DumpedTableMetadata;
import herddb.network.KeyValue;
import herddb.storage.DataStorageManagerException;
import java.util.List;

/**
 * Serves a full dump of a TableSpace for a full restore
 *
 * @author enrico.olivelli
 */
public class TableSpaceRestoreSource {

    public DumpedTableMetadata nextTable() throws DataStorageManagerException {
        return null;
    }

    public List<KeyValue> nextTableDataChunk() throws DataStorageManagerException {
        return null;
    }

    public List<KeyValue> nextTransactionLogChunk() throws DataStorageManagerException {
        return null;
    }

    public List<byte[]> nextTransactionsBlock() throws DataStorageManagerException {
        return null;
    }

    public String nextEntryType() throws DataStorageManagerException {
        return null;
    }

}
