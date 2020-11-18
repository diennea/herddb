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
package herddb.core;

import herddb.backup.DumpedTableMetadata;
import herddb.client.TableSpaceDumpReceiver;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManagerException;
import herddb.utils.SystemInstrumentation;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives data for a table on the full download from a 'replica' node
 *
 * @author enrico.olivelli
 */
public class ReplicaFullTableDataDumpReceiver extends TableSpaceDumpReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaFullTableDataDumpReceiver.class.getName());

    private TableManager currentTable;
    private final CompletableFuture<Object> latch;
    private Throwable error;
    LogSequenceNumber logSequenceNumber;
    private final TableSpaceManager tableSpaceManager;
    private final String tableSpaceName;

    public ReplicaFullTableDataDumpReceiver(TableSpaceManager tableSpaceManager) {
        this.latch = new CompletableFuture<>();
        this.tableSpaceManager = tableSpaceManager;
        this.tableSpaceName = tableSpaceManager.getTableSpaceName();
    }

    @Override
    public void start(LogSequenceNumber logSequenceNumber) throws DataStorageManagerException {
        this.logSequenceNumber = logSequenceNumber;
    }

    public LogSequenceNumber getLogSequenceNumber() {
        return logSequenceNumber;
    }

    public CompletableFuture<Object> getLatch() {
        return latch;
    }

    public Throwable getError() {
        return error;
    }

    @Override
    public void onError(Throwable error) throws DataStorageManagerException {
        LOGGER.error("dumpReceiver " + tableSpaceName + ", onError ", error);
        this.error = error;
        latch.completeExceptionally(error);
    }

    @Override
    public void finish(LogSequenceNumber pos) throws DataStorageManagerException {
        LOGGER.info("dumpReceiver " + tableSpaceName + ", finish, at " + pos);
        latch.complete("");
    }

    @Override
    public void endTable() throws DataStorageManagerException {
        if (currentTable == null) {
            LOGGER.error("dumpReceiver " + tableSpaceName + ", endTable swallow data after leader side error");
            return;
        }
        LOGGER.info("dumpReceiver " + tableSpaceName + ", endTable " + currentTable.getTable().name);
        currentTable = null;
    }

    @Override
    public void receiveTableDataChunk(List<Record> record) throws DataStorageManagerException {
        if (currentTable == null) {
            LOGGER.error("dumpReceiver " + tableSpaceName + ", receiveTableDataChunk swallow data after leader side error");
            return;
        }
        currentTable.writeFromDump(record);
        // after writing to local storage
        SystemInstrumentation.instrumentationPoint("receiveTableDataChunk", tableSpaceManager, currentTable, record);
    }

    @Override
    public void beginTable(DumpedTableMetadata dumpedTable, Map<String, Object> stats) throws DataStorageManagerException {
        Table table = dumpedTable.table;
        LOGGER.info("dumpReceiver " + tableSpaceName + ", beginTable " + table.name + ", stats:" + stats + ", dumped at " + dumpedTable.logSequenceNumber + " (general dump at " + logSequenceNumber + ")");
        currentTable = tableSpaceManager.bootTable(table, 0, dumpedTable.logSequenceNumber, false);
        for (Index index : dumpedTable.indexes) {
            tableSpaceManager.bootIndex(index, currentTable, false, 0, false, true);
        }
    }

}
