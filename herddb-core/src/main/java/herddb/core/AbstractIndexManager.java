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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import herddb.index.IndexOperation;
import herddb.index.KeyToPageIndex;
import herddb.log.CommitLog;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.model.Transaction;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;

/**
 * Index runtime
 *
 * @author enrico.olivelli
 */
public abstract class AbstractIndexManager implements AutoCloseable {

    protected final Index index;
    protected final AbstractTableManager tableManager;
    protected final DataStorageManager dataStorageManager;
    protected final String tableSpaceUUID;
    protected final CommitLog log;
    /**
     * This value is not empty until the transaction who creates the table does not commit
     */
    protected long createdInTransaction;

    public AbstractIndexManager(Index index, AbstractTableManager tableManager, DataStorageManager dataStorageManager, String tableSpaceUUID, CommitLog log, long createdInTransaction) {
        this.index = index;
        this.createdInTransaction = createdInTransaction;
        this.tableManager = tableManager;
        this.dataStorageManager = dataStorageManager;
        this.tableSpaceUUID = tableSpaceUUID;
        this.log = log;
    }

    public final Index getIndex() {
        return index;
    }

    public final String getIndexName() {
        return index.name;
    }

    public final String[] getColumnNames() {
        return index.columnNames;
    }

    /**
     * Boots the index, this method usually reload state from the DataStorageManager
     *
     * @param sequenceNumber sequence number from which boot the index
     *
     * @throws DataStorageManagerException
     * @see DataStorageManager#fullIndexScan(java.lang.String, java.lang.String, herddb.storage.FullIndexScanConsumer)
     */
    public abstract void start(LogSequenceNumber sequenceNumber) throws DataStorageManagerException;

    /**
     * Release resources. Do not drop data
     */
    @Override
    public void close() {

    }

    /**
     * Rebuild entirely the index, usually performing a full table scan
     *
     * @throws DataStorageManagerException
     */
    public abstract void rebuild() throws DataStorageManagerException;

    /**
     * Ensures that all data is persisted from disk
     *
     * @param sequenceNumber
     * @return
     * @throws DataStorageManagerException
     */
    public abstract List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException;

    /**
     * Basic function of the index. The index returns the list of PKs of the table which match the predicate
     * (IndexOperation) Beare that this function could return a super set of the list of the PKs which actually match
     * the predicate. TableManager will check every record againts the (WHERE) Predicate in order to ensure the final
     * result
     *
     * @param operation
     * @param context
     * @param tableContext
     * @return a stream on the PK values of the tables which match the index
     * @throws StatementExecutionException
     */
    protected abstract Stream<Bytes> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext) throws StatementExecutionException;

    /**
     * This function is called from the TableManager to perform scans. It usually have to deal with a JOIN on the
     * KeyToPageIndex of the TableManager
     *
     * @param operation
     * @param context
     * @param tableContext
     * @param keyToPageIndex
     * @return
     * @throws DataStorageManagerException
     * @throws StatementExecutionException
     */
    public Stream<Map.Entry<Bytes, Long>> recordSetScanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext, KeyToPageIndex keyToPageIndex) throws DataStorageManagerException, StatementExecutionException {
        return scanner(operation, context, tableContext).map((Bytes b) -> {
            Long idPage = keyToPageIndex.get(b);
            if (idPage == null) {
                return null;
            }
            return (Map.Entry<Bytes, Long>) new SimpleImmutableEntry<>(b, idPage);
        }).filter(p -> p != null);
    }

    public abstract void recordUpdated(Bytes key, DataAccessor previousValues, DataAccessor newValues) throws DataStorageManagerException;

    public abstract void recordInserted(Bytes key, DataAccessor values) throws DataStorageManagerException;

    public abstract void recordDeleted(Bytes key, DataAccessor values) throws DataStorageManagerException;

    /**
     * Drop the index from persist storage
     *
     * @throws DataStorageManagerException
     */
    public void dropIndexData() throws DataStorageManagerException {
        dataStorageManager.dropIndex(tableSpaceUUID, index.name);
    }

    final void onTransactionCommit(Transaction transaction, boolean recovery) throws DataStorageManagerException {
        if (createdInTransaction > 0) {
            if (transaction.transactionId != createdInTransaction) {
                throw new DataStorageManagerException("this indexManager is available only on transaction " + createdInTransaction);
            }
            createdInTransaction = 0;
        }
    }

    public final boolean isAvailable() {
        return createdInTransaction == 0;
    }

    /**
     * Erase the index. Out-side the scope of a transaction
     */
    public abstract void truncate() throws DataStorageManagerException;
}
