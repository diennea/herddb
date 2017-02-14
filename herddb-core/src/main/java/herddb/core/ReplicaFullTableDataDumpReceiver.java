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
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManagerException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives data for a table on the full download from a 'replica' node
 *
 * @author enrico.olivelli
 */
class ReplicaFullTableDataDumpReceiver extends TableSpaceDumpReceiver {

    private static final Logger LOGGER = Logger.getLogger(ReplicaFullTableDataDumpReceiver.class.getName());

    private TableManager currentTable;
    private final CountDownLatch latch;
    private Throwable error;
    LogSequenceNumber logSequenceNumber;
    private final TableSpaceManager tableSpaceManager;
    private final String tableSpaceName;

    public ReplicaFullTableDataDumpReceiver(TableSpaceManager tableSpaceManager) {
        this.latch = new CountDownLatch(1);
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

    public boolean join(int timeout) throws InterruptedException {
        return latch.await(timeout, TimeUnit.MILLISECONDS);
    }

    public Throwable getError() {
        return error;
    }

    @Override
    public void onError(Throwable error) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "dumpReceiver " + tableSpaceName + ", onError ", error);
        this.error = error;
        latch.countDown();
    }

    @Override
    public void finish(LogSequenceNumber pos) throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "dumpReceiver " + tableSpaceName + ", finish, at " + pos);
        latch.countDown();
    }

    @Override
    public void endTable() throws DataStorageManagerException {
        LOGGER.log(Level.SEVERE, "dumpReceiver " + tableSpaceName + ", endTable " + currentTable.getTable().name);
        currentTable = null;
    }

    @Override
    public void receiveTableDataChunk(List<Record> record) throws DataStorageManagerException {
        currentTable.writeFromDump(record);
    }

    @Override
    public void beginTable(DumpedTableMetadata dumpedTable, Map<String, Object> stats) throws DataStorageManagerException {
        Table table = dumpedTable.table;
        LOGGER.log(Level.SEVERE, "dumpReceiver " + tableSpaceName + ", beginTable " + table.name + ", stats:" + stats);
        currentTable = tableSpaceManager.bootTable(table, 0, null);
    }

}
