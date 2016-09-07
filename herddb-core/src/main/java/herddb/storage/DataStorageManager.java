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
package herddb.storage;

import herddb.core.PostCheckpointAction;
import herddb.core.RecordSetFactory;
import herddb.index.KeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Physical storage of data
 *
 * @author enrico.olivelli
 */
public abstract class DataStorageManager {

    /**
     * Load a data page in memory
     *
     * @param tableName
     * @param pageId
     * @return
     */
    public abstract List<Record> readPage(String tableSpace, String tableName, Long pageId) throws DataStorageManagerException;

    public abstract byte[] readIndexPage(String tableSpace, String indexName, Long pageId) throws DataStorageManagerException;

    /**
     * Load the full data of a table
     *
     * @param tableSpace
     * @param tableName
     * @param consumer
     * @throws herddb.storage.DataStorageManagerException
     */
    public abstract void fullTableScan(String tableSpace, String tableName, FullTableScanConsumer consumer) throws DataStorageManagerException;

    public abstract void fullIndexScan(String tableSpace, String tableName, FullIndexScanConsumer consumer) throws DataStorageManagerException;

    /**
     * Write a page on disk
     *
     * @param tableSpace
     * @param tableName
     * @param pageId
     * @param sequenceNumber
     * @param newPage
     * @return
     * @throws herddb.storage.DataStorageManagerException
     */
    public abstract void writePage(String tableSpace, String tableName, long pageId, List<Record> newPage) throws DataStorageManagerException;

    public abstract void writeIndexPage(String tableSpace, String indexName, long pageId, byte[] page) throws DataStorageManagerException;

    /**
     * Write current table status. This operations mark the actual set of pages
     * at a given log sequence number and "closes" a snapshot
     *
     * @param tableSpace
     * @param tableName
     * @param tableStatus
     * @return
     * @throws DataStorageManagerException
     */
    public abstract List<PostCheckpointAction> tableCheckpoint(String tableSpace, String tableName, TableStatus tableStatus) throws DataStorageManagerException;

    public abstract List<PostCheckpointAction> indexCheckpoint(String tableSpace, String tableName, IndexStatus indexStatus) throws DataStorageManagerException;

    /**
     * Return the actual number of pages presents on disk
     *
     * @param tableName
     * @return
     * @throws DataStorageManagerException
     */
    public abstract int getActualNumberOfPages(String tableSpace, String tableName) throws DataStorageManagerException;

    /**
     * Boots the Storage Manager
     *
     * @throws DataStorageManagerException
     */
    public abstract void start() throws DataStorageManagerException;

    /**
     * Shutsdown cleanly the Storage Manager
     *
     * @throws DataStorageManagerException
     */
    public abstract void close() throws DataStorageManagerException;

    /**
     * Load tables metadata
     *
     * @param sequenceNumber
     * @param tableSpace
     * @return
     * @throws DataStorageManagerException
     */
    public abstract List<Table> loadTables(LogSequenceNumber sequenceNumber, String tableSpace) throws DataStorageManagerException;

    /**
     * Load indexes metadata
     *
     * @param sequenceNumber
     * @param tableSpace
     * @return
     * @throws DataStorageManagerException
     */
    public abstract List<Index> loadIndexes(LogSequenceNumber sequenceNumber, String tableSpace) throws DataStorageManagerException;

    public abstract void loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace, Consumer<Transaction> consumer) throws DataStorageManagerException;

    /**
     * Writes tables metadata
     *
     * @param sequenceNumber
     * @param tableSpace
     * @param tables
     * @param indexlist
     * @throws DataStorageManagerException
     */
    public abstract void writeTables(String tableSpace, LogSequenceNumber sequenceNumber, List<Table> tables, List<Index> indexlist) throws DataStorageManagerException;

    public abstract void writeCheckpointSequenceNumber(String tableSpace, LogSequenceNumber sequenceNumber) throws DataStorageManagerException;

    public abstract void writeTransactionsAtCheckpoint(String tableSpace, LogSequenceNumber sequenceNumber, Collection<Transaction> transactions) throws DataStorageManagerException;

    public abstract LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace) throws DataStorageManagerException;

    public abstract void dropTable(String tablespace, String name) throws DataStorageManagerException;

    public abstract KeyToPageIndex createKeyToPageMap(String tablespace, String name) throws DataStorageManagerException;

    public abstract void releaseKeyToPageMap(String tablespace, String name, KeyToPageIndex index);

    public abstract RecordSetFactory createRecordSetFactory();

    public abstract void cleanupAfterBoot(String tablespace, String name, Set<Long> activePagesAtBoot) throws DataStorageManagerException;

    public abstract void dropIndex(String tableSpaceUUID, String name) throws DataStorageManagerException;

}
