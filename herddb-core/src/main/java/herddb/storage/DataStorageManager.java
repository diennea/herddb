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

import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.core.RecordSetFactory;
import herddb.index.KeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.utils.ByteArrayCursor;
import herddb.utils.ExtendedDataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Physical storage of data
 *
 * @author enrico.olivelli
 */
public abstract class DataStorageManager implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(DataStorageManager.class.getName());

    /**
     * Load a data page in memory
     *
     * @param tableSpace
     * @param uuid
     * @param pageId
     * @return
     * @throws herddb.storage.DataStorageManagerException
     */
    public abstract List<Record> readPage(String tableSpace, String uuid, Long pageId)
            throws DataStorageManagerException;

    public abstract void initIndex(String tableSpace, String uuid) throws DataStorageManagerException;

    public abstract void initTable(String tableSpace, String uuid) throws DataStorageManagerException;

    @FunctionalInterface
    public interface DataReader<X> {

        X read(ByteArrayCursor in) throws IOException;
    }

    public abstract <X> X readIndexPage(String tableSpace, String uuid, Long pageId, DataReader<X> reader)
            throws DataStorageManagerException;

    /**
     * Load the full data of a table
     *
     * @param tableSpace
     * @param uuid
     * @param consumer
     * @throws herddb.storage.DataStorageManagerException
     */
    public abstract void fullTableScan(String tableSpace, String uuid, FullTableScanConsumer consumer)
            throws DataStorageManagerException;

    /**
     * Load the full data of a table
     *
     * @param tableSpace
     * @param uuid
     * @param sequenceNumber
     * @param consumer
     * @throws herddb.storage.DataStorageManagerException
     */
    public abstract void fullTableScan(String tableSpace, String uuid, LogSequenceNumber sequenceNumber, FullTableScanConsumer consumer)
            throws DataStorageManagerException;

    /**
     * Write a page on disk
     *
     * @param tableSpace
     * @param uuid
     * @param pageId
     * @param newPage
     * @throws herddb.storage.DataStorageManagerException
     */
    public abstract void writePage(String tableSpace, String uuid, long pageId, Collection<Record> newPage)
            throws DataStorageManagerException;

    @FunctionalInterface
    public interface DataWriter {

        void write(ExtendedDataOutputStream out) throws IOException;
    }

    public abstract void writeIndexPage(String tableSpace, String uuid, long pageId, DataWriter writer);

    /**
     * Write current table status. This operations mark the actual set of pages at a given log sequence number and
     * "closes" a snapshot
     *
     * @param tableSpace
     * @param uuid
     * @param tableStatus
     * @param pin
     * @return
     * @throws DataStorageManagerException
     */
    public abstract List<PostCheckpointAction> tableCheckpoint(
            String tableSpace, String uuid,
            TableStatus tableStatus, boolean pin
    ) throws DataStorageManagerException;

    public abstract List<PostCheckpointAction> indexCheckpoint(
            String tableSpace, String uuid,
            IndexStatus indexStatus, boolean pin
    ) throws DataStorageManagerException;

    /**
     * Return the actual number of pages presents on disk
     *
     * @param uuid
     * @return
     * @throws DataStorageManagerException
     */
    public abstract int getActualNumberOfPages(String tableSpace, String uuid) throws DataStorageManagerException;

    public abstract TableStatus getLatestTableStatus(String tableSpace, String uuid)
            throws DataStorageManagerException;

    /**
     * Returns the {@link TableStatus} relative to given sequence number.
     *
     * @throws DataStorageManagerException if no status exists for given data or it cannot be read
     */
    public abstract TableStatus getTableStatus(String tableSpace, String uuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException;

    /**
     * Returns the {@link IndexStatus} relative to given sequence number.
     *
     * @throws DataStorageManagerException if no status exists for given data or it cannot be read
     */
    public abstract IndexStatus getIndexStatus(String tableSpace, String uuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException;

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
    @Override
    public abstract void close() throws DataStorageManagerException;

    public abstract void eraseTablespaceData(String tableSpace) throws DataStorageManagerException;

    /**
     * Load tables metadata
     *
     * @param sequenceNumber
     * @param tableSpace
     * @return
     * @throws DataStorageManagerException
     */
    public abstract List<Table> loadTables(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException;

    /**
     * Load indexes metadata
     *
     * @param sequenceNumber
     * @param tableSpace
     * @return
     * @throws DataStorageManagerException
     */
    public abstract List<Index> loadIndexes(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException;

    public abstract void loadTransactions(
            LogSequenceNumber sequenceNumber, String tableSpace,
            Consumer<Transaction> consumer
    ) throws DataStorageManagerException;

    /**
     * Writes tables metadata
     *
     * @param sequenceNumber
     * @param tableSpace
     * @param tables
     * @param indexlist
     * @throws DataStorageManagerException
     */
    public abstract Collection<PostCheckpointAction> writeTables(
            String tableSpace, LogSequenceNumber sequenceNumber, List<Table> tables,
            List<Index> indexlist, boolean prepareActions
    ) throws DataStorageManagerException;

    public abstract Collection<PostCheckpointAction> writeCheckpointSequenceNumber(String tableSpace, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException;

    public abstract Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(
            String tableSpace, LogSequenceNumber sequenceNumber,
            Collection<Transaction> transactions
    ) throws DataStorageManagerException;

    public abstract LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace)
            throws DataStorageManagerException;

    public abstract void dropTable(String tablespace, String name) throws DataStorageManagerException;

    public abstract KeyToPageIndex createKeyToPageMap(String tablespace, String name, MemoryManager memoryManager)
            throws DataStorageManagerException;

    public abstract void releaseKeyToPageMap(String tablespace, String name, KeyToPageIndex index);

    public abstract RecordSetFactory createRecordSetFactory();

    public abstract void cleanupAfterTableBoot(String tablespace, String name, Set<Long> activePagesAtBoot)
            throws DataStorageManagerException;

    public abstract void truncateIndex(String tableSpaceUUID, String name) throws DataStorageManagerException;

    public abstract void dropIndex(String tableSpaceUUID, String name) throws DataStorageManagerException;

    /* Map[tablespace_uuid,Map[pageid,pincounts] */
    private final Map<String, Map<Long, Integer>> tablePagesPins = new ConcurrentHashMap<>();

    /* Map[tablespace_uuid,Set[sequenceNumber]] */
    private final Map<String, Set<LogSequenceNumber>> tableCheckpointPins = new ConcurrentHashMap<>();

    private final Map<String, Map<Long, Integer>> indexPagesPins = new ConcurrentHashMap<>();
    private final Map<String, Set<LogSequenceNumber>> indexCheckpointPins = new ConcurrentHashMap<>();

    protected Map<Long, Integer> pinTableAndGetPages(String tableSpace, String uuid, TableStatus status, boolean pin) {
        return pinAndGetPages(tableSpace, uuid, status.activePages.keySet(), tablePagesPins, pin);
    }

    protected Map<Long, Integer> pinIndexAndGetPages(String tableSpace, String uuid, IndexStatus status, boolean pin) {
        return pinAndGetPages(tableSpace, uuid, status.activePages, indexPagesPins, pin);
    }

    protected Set<LogSequenceNumber> pinTableAndGetCheckpoints(
            String tableSpace, String uuid, TableStatus status,
            boolean pin
    ) {
        return pinAndGetCheckpoints(tableSpace, uuid, status.sequenceNumber, tableCheckpointPins, pin);
    }

    protected Set<LogSequenceNumber> pinIndexAndGetCheckpoints(
            String tableSpace, String uuid, IndexStatus status,
            boolean pin
    ) {
        return pinAndGetCheckpoints(tableSpace, uuid, status.sequenceNumber, indexCheckpointPins, pin);
    }

    public void unPinTableCheckpoint(String tableSpace, String uuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {

        final TableStatus status = getTableStatus(tableSpace, uuid, sequenceNumber);

        if (status == null) {
            throw new DataStorageManagerException("Cannot unpin a not pinned checkpoint "
                    + tableSpace + "." + uuid + "." + sequenceNumber.ledgerId + "." + sequenceNumber.offset);
        }

        unPinPages(tableSpace, uuid, status.activePages.keySet(), tablePagesPins);
        unPinCheckPoint(tableSpace, uuid, status.sequenceNumber, tableCheckpointPins);
    }

    public void unPinIndexCheckpoint(String tableSpace, String uuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {

        final IndexStatus status = getIndexStatus(tableSpace, uuid, sequenceNumber);

        if (status == null) {
            throw new DataStorageManagerException("Cannot unpin a not pinned checkpoint "
                    + tableSpace + "." + uuid + "." + sequenceNumber.ledgerId + "." + sequenceNumber.offset);
        }

        unPinPages(tableSpace, uuid, status.activePages, indexPagesPins);
        unPinCheckPoint(tableSpace, uuid, status.sequenceNumber, indexCheckpointPins);
    }

    private Map<Long, Integer> pinAndGetPages(
            String tableSpace, String name, Collection<Long> activePages,
            Map<String, Map<Long, Integer>> pagesPins, boolean pin
    ) {
        LOG.log(Level.FINER, "pinAndGetPages {0}.{1} activePages: {2}, pagesPins:{3} (pin: {4})", new Object[]{tableSpace, name, activePages, pagesPins, pin});
        final Map<Long, Integer> pins;
        final String pinkey = tableSpace + "_" + name;
        if (pin) {

            /*
             * Synchronize the whole pinning map to simplify insertion/deletion. It is a fast and rarely used
             * procedure. Being a concurrent map read operation will not be synchronized as unneeded
             */
            synchronized (pagesPins) {
                /* Must collect every page */
                pins = pagesPins.computeIfAbsent(pinkey, k -> new ConcurrentHashMap<>());
                for (Long pageId : activePages) {
                    pins.compute(pageId, (k, v) -> v == null ? 1 : v + 1);
                }
            }

        } else {
            /* No pin to add if the don't exists */
            pins = pagesPins.getOrDefault(pinkey, Collections.emptyMap());
        }

        return pins;
    }

    private Set<LogSequenceNumber> pinAndGetCheckpoints(
            String tableSpace, String uuid,
            LogSequenceNumber sequenceNumber, Map<String, Set<LogSequenceNumber>> checkpointsPins, boolean pin
    ) {

        final Set<LogSequenceNumber> checkpoints;
        final String pinkey = tableSpace + "_" + uuid;
        if (pin) {

            /*
             * Synchronize the whole pinning map to simplify insertion/deletion. It is a fast and rarely used
             * procedure. Being a concurrent map read operation will not be synchronized as unneeded
             */
            synchronized (checkpointsPins) {
                /* Must collect the checkpoint entry */
                checkpoints = checkpointsPins.computeIfAbsent(pinkey, k -> ConcurrentHashMap.newKeySet());
                checkpoints.add(sequenceNumber);
            }

        } else {
            /* No pin to add if the don't exists */
            checkpoints = checkpointsPins.getOrDefault(pinkey, Collections.emptySet());
        }

        return checkpoints;
    }

    private void unPinPages(String tableSpace, String name, Collection<Long> activePages, Map<String, Map<Long, Integer>> pagesPins)
            throws DataStorageManagerException {

        final String pinkey = tableSpace + "_" + name;

        /*
         * Synchronize the whole pinning map to simplify insertion/deletion. It is a fast and rarely used
         * procedure. Being a concurrent map read operation will not be synchronized as unneeded
         */
        synchronized (pagesPins) {
            /* Must unpin every page */
            Map<Long, Integer> pins = pagesPins.get(pinkey);
            if (pins != null) {
                for (Long pageId : activePages) {
                    pins.compute(pageId, (k, v) -> {
                        if (v == null || v < 2) {
                            /* Remove pin */
                            return null;
                        } else {
                            return v - 1;
                        }
                    });
                }
            }
        }
    }

    private void unPinCheckPoint(String tableSpace, String name, LogSequenceNumber sequenceNumber, Map<String, Set<LogSequenceNumber>> checkpointsPins)
            throws DataStorageManagerException {

        final String pinkey = tableSpace + "_" + name;

        /*
         * Synchronize the whole pinning map to simplify insertion/deletion. It is a fast and rarely used
         * procedure. Being a concurrent map read operation will not be synchronized as unneeded
         */
        synchronized (checkpointsPins) {
            /* Must collect the checkpoint entry */
            Set<LogSequenceNumber> checkpoints = checkpointsPins.get(pinkey);

            if (checkpoints != null) {
                checkpoints.remove(sequenceNumber);
            }
        }
    }
}
