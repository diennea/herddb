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

import static herddb.sql.JSQLParserPlanner.delimit;
import static java.util.concurrent.TimeUnit.SECONDS;
import herddb.codec.RecordSerializer;
import herddb.core.PageSet.DataPageMetaData;
import herddb.core.stats.TableManagerStats;
import herddb.index.IndexOperation;
import herddb.index.KeyToPageIndex;
import herddb.index.PrimaryIndexSeek;
import herddb.log.CommitLog;
import herddb.log.CommitLogResult;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.DDLException;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.ForeignKeyDef;
import herddb.model.ForeignKeyViolationException;
import herddb.model.GetResult;
import herddb.model.Index;
import herddb.model.Predicate;
import herddb.model.Projection;
import herddb.model.Record;
import herddb.model.RecordFunction;
import herddb.model.RecordTooBigException;
import herddb.model.ScanLimits;
import herddb.model.ScanLimitsImpl;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.TupleComparator;
import herddb.model.UniqueIndexContraintViolationException;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.TableConsistencyCheckStatement;
import herddb.model.commands.TruncateTableStatement;
import herddb.model.commands.UpdateStatement;
import herddb.server.ServerConfiguration;
import herddb.storage.DataPageDoesNotExistException;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.BatchOrderedExecutor;
import herddb.utils.BooleanHolder;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.EnsureLongIncrementAccumulator;
import herddb.utils.Futures;
import herddb.utils.Holder;
import herddb.utils.ILocalLockManager;
import herddb.utils.LocalLockManager;
import herddb.utils.LockHandle;
import herddb.utils.NullLockManager;
import herddb.utils.SystemProperties;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Handles Data of a Table
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public final class TableManager implements AbstractTableManager, Page.Owner {

    private static final Logger LOGGER = Logger.getLogger(TableManager.class.getName());

    private static final long CHECKPOINT_LOCK_WRITE_TIMEOUT = SystemProperties.
            getIntSystemProperty("herddb.tablemanager.checkpoint.lock.write.timeout", 60);

    private static final long CHECKPOINT_LOCK_READ_TIMEOUT = SystemProperties.
            getIntSystemProperty("herddb.tablemanager.checkpoint.lock.read.timeout", 10);

    private static final int SORTED_PAGE_ACCESS_WINDOW_SIZE = SystemProperties.
            getIntSystemProperty("herddb.tablemanager.sortedPageAccessWindowSize", 2000);

    private static final boolean ENABLE_LOCAL_SCAN_PAGE_CACHE = SystemProperties.
            getBooleanSystemProperty("herddb.tablemanager.enableLocalScanPageCache", true);

    private static final int HUGE_TABLE_SIZE_FORCE_MATERIALIZED_RESULTSET = SystemProperties.
            getIntSystemProperty("herddb.tablemanager.hugeTableSizeForceMaterializedResultSet", 100_000);

    private static final boolean ENABLE_STREAMING_DATA_SCANNER = SystemProperties.
            getBooleanSystemProperty("herddb.tablemanager.enableStreamingDataScanner", true);

    /**
     * Ignores insert/update/delete failures due to missing transactions during recovery. The operation in
     * recovery will be ignored.
     * <p>
     * Mutable and visible for tests.
     */
    static boolean ignoreMissingTransactionsOnRecovery =
            SystemProperties.getBooleanSystemProperty("herddb.tablemanager.ignoreMissingTransactionsOnRecovery", false);

    private final ConcurrentMap<Long, DataPage> newPages;

    private final ConcurrentMap<Long, DataPage> pages;

    /**
     * A structure which maps each key to the ID of the page (map<byte[], long>)
     * (this can be quite large)
     */
    private final KeyToPageIndex keyToPage;

    private final PageSet pageSet = new PageSet();

    private long nextPageId = 1;
    private final Lock nextPageLock = new ReentrantLock();

    private final AtomicLong currentDirtyRecordsPage = new AtomicLong();

    /**
     * Counts how many pages had been unloaded
     */
    private final LongAdder loadedPagesCount = new LongAdder();

    /**
     * Counts how many pages had been loaded
     */
    private final LongAdder unloadedPagesCount = new LongAdder();
    /**
     * Local locks
     */
    private final ILocalLockManager locksManager;

    /**
     * Set to {@code true} when this {@link TableManager} is fully started
     */
    private volatile boolean started = false;

    private volatile boolean checkPointRunning = false;

    /**
     * Allow checkpoint
     */
    private final StampedLock checkpointLock = new StampedLock();

    /**
     * auto_increment support
     */
    private final AtomicLong nextPrimaryKeyValue = new AtomicLong(1);

    private final TableContext tableContext;

    /**
     * Phisical ID of the TableSpace
     */
    private final String tableSpaceUUID;

    /**
     * Definition of the table
     */
    private Table table;
    private final CommitLog log;
    private final DataStorageManager dataStorageManager;
    private final TableSpaceManager tableSpaceManager;
    private Table[] childrenTables;

    private final PageReplacementPolicy pageReplacementPolicy;

    /**
     * Max logical size of a page (raw key size + raw value size)
     */
    private final long maxLogicalPageSize;

    private final Semaphore maxCurrentPagesLoads = new Semaphore(4, true);

    /**
     * This value is not empty until the transaction who creates the table does
     * not commit
     */
    private long createdInTransaction;

    /**
     * Default dirty threshold for page rebuild during checkpoints
     */
    private final double dirtyThreshold;

    /**
     * Default fill threshold for page rebuild during checkpoints
     */
    private final double fillThreshold;

    /**
     * Checkpoint target max milliseconds
     */
    private final long checkpointTargetTime;

    /**
     * Cleanup (dirty pages) target max milliseconds
     */
    private final long cleanupTargetTime;

    /**
     * Compaction (small pages) target max milliseconds
     */
    private final long compactionTargetTime;

    private final TableManagerStats stats;

    private final Counter checkpointProcessedDirtyRecords;

    private final boolean keyToPageSortedAscending;

    private volatile boolean closed;

    private final ConcurrentHashMap<String, String> childForeignKeyQueries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> parentForeignKeyQueries = new ConcurrentHashMap<>();

    void prepareForRestore(LogSequenceNumber dumpLogSequenceNumber) {
        LOGGER.log(Level.INFO, "Table " + table.name + ", receiving dump,"
                + "done at external logPosition " + dumpLogSequenceNumber);
        this.dumpLogSequenceNumber = dumpLogSequenceNumber;
    }

    void restoreFinished() {
        dumpLogSequenceNumber = null;
        LOGGER.log(Level.INFO, "Table " + table.name + ", received dump");
    }

    private final class TableManagerStatsImpl implements TableManagerStats {

        @Override
        public int getLoadedpages() {
            return pages.size();
        }

        @Override
        public long getLoadedPagesCount() {
            return loadedPagesCount.sum();
        }

        @Override
        public long getUnloadedPagesCount() {
            return unloadedPagesCount.sum();
        }

        @Override
        public long getTablesize() {
            if (closed)  {
                // the keyToPage has been disposed
                return 0;
            }
            // please note that this method is called very often
            // by Calcite to have a statistic about the size of the table
            return keyToPage.size();
        }

        @Override
        public int getDirtypages() {
            return pageSet.getDirtyPagesCount();
        }

        @Override
        public int getDirtyrecords() {
            int size = 0;
            for (DataPage newPage : newPages.values()) {
                size += newPage.size();
            }
            return size;
        }

        @Override
        public long getDirtyUsedMemory() {
            long used = 0;
            for (DataPage newPage : newPages.values()) {
                used += newPage.getUsedMemory();
            }
            return used;
        }

        @Override
        public long getMaxLogicalPageSize() {
            return maxLogicalPageSize;
        }

        @Override
        public long getBuffersUsedMemory() {
            long value = 0;
            for (DataPage page : pages.values()) {
                value += page.getUsedMemory();
            }
            return value;
        }

        @Override
        public long getKeysUsedMemory() {
            return keyToPage.getUsedMemory();
        }

    }

    TableManager(
            Table table, CommitLog log, MemoryManager memoryManager,
            DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID,
            long createdInTransaction
    ) throws DataStorageManagerException {
        this.stats = new TableManagerStatsImpl();

        this.log = log;

        this.table = table;
        this.tableSpaceManager = tableSpaceManager;
        this.childrenTables = tableSpaceManager.collectChildrenTables(this.table);

        this.dataStorageManager = dataStorageManager;
        this.createdInTransaction = createdInTransaction;
        this.tableSpaceUUID = tableSpaceUUID;
        this.tableContext = buildTableContext();
        this.maxLogicalPageSize = memoryManager.getMaxLogicalPageSize();
        this.keyToPage = dataStorageManager.createKeyToPageMap(tableSpaceUUID, table.uuid, memoryManager);

        this.pageReplacementPolicy = memoryManager.getDataPageReplacementPolicy();
        this.pages = new ConcurrentHashMap<>();
        this.newPages = new ConcurrentHashMap<>();

        this.dirtyThreshold = tableSpaceManager.getDbmanager().getServerConfiguration().getDouble(
                ServerConfiguration.PROPERTY_DIRTY_PAGE_THRESHOLD,
                ServerConfiguration.PROPERTY_DIRTY_PAGE_THRESHOLD_DEFAULT);

        this.fillThreshold = tableSpaceManager.getDbmanager().getServerConfiguration().getDouble(
                ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD,
                ServerConfiguration.PROPERTY_FILL_PAGE_THRESHOLD_DEFAULT);

        long checkpointTargetTime = tableSpaceManager.getDbmanager().getServerConfiguration().getLong(
                ServerConfiguration.PROPERTY_CHECKPOINT_DURATION,
                ServerConfiguration.PROPERTY_CHECKPOINT_DURATION_DEFAULT);

        this.checkpointTargetTime = checkpointTargetTime < 0 ? Long.MAX_VALUE : checkpointTargetTime;

        long cleanupTargetTime = tableSpaceManager.getDbmanager().getServerConfiguration().getLong(
                ServerConfiguration.PROPERTY_CLEANUP_DURATION,
                ServerConfiguration.PROPERTY_CLEANUP_DURATION_DEFAULT);

        this.cleanupTargetTime = cleanupTargetTime < 0 ? Long.MAX_VALUE : cleanupTargetTime;

        long compactionTargetTime = tableSpaceManager.getDbmanager().getServerConfiguration().getLong(
                ServerConfiguration.PROPERTY_COMPACTION_DURATION,
                ServerConfiguration.PROPERTY_COMPACTION_DURATION_DEFAULT);

        this.compactionTargetTime = compactionTargetTime < 0 ? Long.MAX_VALUE : compactionTargetTime;


        StatsLogger tableMetrics = tableSpaceManager.tablespaceStasLogger.scope("table_" + table.name);
        this.checkpointProcessedDirtyRecords = tableMetrics.getCounter("checkpoint_processed_dirty_records");
        int[] pkTypes = new int[table.primaryKey.length];
        for (int i = 0; i < table.primaryKey.length; i++) {
            Column col = table.getColumn(table.primaryKey[i]);
            pkTypes[i] = col.type;
        }
        this.keyToPageSortedAscending = keyToPage.isSortedAscending(pkTypes);

        boolean nolocks = tableSpaceManager.getDbmanager().getServerConfiguration().getBoolean(
                ServerConfiguration.PROPERTY_TABLEMANAGER_DISABLE_ROWLEVELLOCKS,
                ServerConfiguration.PROPERTY_TABLEMANAGER_DISABLE_ROWLEVELLOCKS_DEFAULT
        );
        if (nolocks) {
            locksManager = new NullLockManager();
        } else {
            int writeLockTimeout = tableSpaceManager.getDbmanager().getServerConfiguration().getInt(
                ServerConfiguration.PROPERTY_WRITELOCK_TIMEOUT,
                ServerConfiguration.PROPERTY_WRITELOCK_TIMEOUT_DEFAULT
            );
            int readLockTimeout = tableSpaceManager.getDbmanager().getServerConfiguration().getInt(
                ServerConfiguration.PROPERTY_READLOCK_TIMEOUT,
                ServerConfiguration.PROPERTY_READLOCK_TIMEOUT_DEFAULT
            );
            LocalLockManager newLocksManager = new LocalLockManager();
            newLocksManager.setWriteLockTimeout(writeLockTimeout);
            newLocksManager.setReadLockTimeout(readLockTimeout);
            locksManager = newLocksManager;
    }
    }

    public TableContext buildTableContext() {
        TableContext tableContext;
        if (!table.auto_increment) {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    throw new UnsupportedOperationException("no auto_increment function on this table");
                }

                @Override
                public Table getTable() {
                    return table;
                }
            };
        } else if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.INTEGER || table.getColumn(table.primaryKey[0]).type == ColumnTypes.NOTNULL_INTEGER) {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    return Bytes.intToByteArray((int) nextPrimaryKeyValue.getAndIncrement());
                }

                @Override
                public Table getTable() {
                    return table;
                }
            };
        } else if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.LONG || table.getColumn(table.primaryKey[0]).type == ColumnTypes.NOTNULL_LONG) {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    return Bytes.longToByteArray(nextPrimaryKeyValue.getAndIncrement());
                }

                @Override
                public Table getTable() {
                    return table;
                }
            };
        } else {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    throw new UnsupportedOperationException("no auto_increment function on this table");
                }

                @Override
                public Table getTable() {
                    return table;
                }
            };
        }
        return tableContext;
    }

    @Override
    public Table getTable() {
        return table;
    }

    private LogSequenceNumber bootSequenceNumber;
    private LogSequenceNumber dumpLogSequenceNumber;

    @Override
    public LogSequenceNumber getBootSequenceNumber() {
        return bootSequenceNumber;
    }

    @Override
    public void start(boolean created) throws DataStorageManagerException {

        Map<Long, DataPageMetaData> activePagesAtBoot = new HashMap<>();
        dataStorageManager.initTable(tableSpaceUUID, table.uuid);
        keyToPage.init();
        bootSequenceNumber = LogSequenceNumber.START_OF_TIME;

        boolean requireLoadAtStartup = keyToPage.requireLoadAtStartup();

        if (requireLoadAtStartup) {
            if (created) {
                // this is a fresh new table, with in memory key-to-page
                TableStatus tableStatus = TableStatus.buildTableStatusForNewCreatedTable(table.uuid);
                nextPrimaryKeyValue.set(Bytes.toLong(tableStatus.nextPrimaryKeyValue, 0));
                nextPageId = tableStatus.nextPageId;
                bootSequenceNumber = tableStatus.sequenceNumber;
                activePagesAtBoot.putAll(tableStatus.activePages);
            } else {
                // non persistent primary key index, we need a full table scan
                LOGGER.log(Level.INFO, "loading in memory all the keys for table {0}", new Object[]{table.name});
                dataStorageManager.fullTableScan(tableSpaceUUID, table.uuid,
                        new FullTableScanConsumer() {

                    @Override
                    public void acceptTableStatus(TableStatus tableStatus) {
                        LOGGER.log(Level.INFO, "recovery table at {0}", tableStatus.sequenceNumber);
                        nextPrimaryKeyValue.set(Bytes.toLong(tableStatus.nextPrimaryKeyValue, 0));
                        nextPageId = tableStatus.nextPageId;
                        bootSequenceNumber = tableStatus.sequenceNumber;
                        activePagesAtBoot.putAll(tableStatus.activePages);
                    }

                    @Override
                    public void acceptPage(long pageId, List<Record> records) {
                        for (Record record : records) {
                            keyToPage.put(record.key.nonShared(), pageId, null /* PK is empty */);
                        }
                    }

                    @Override
                    public void endTable() {
                    }

                });
            }
        } else {
            LOGGER.log(Level.INFO, "loading table {0}, uuid {1}", new Object[]{table.name, table.uuid});
            TableStatus tableStatus = dataStorageManager.getLatestTableStatus(tableSpaceUUID, table.uuid);
            if (!tableStatus.sequenceNumber.isStartOfTime()) {
                LOGGER.log(Level.INFO, "recovery table at {0}", tableStatus.sequenceNumber);
            }
            nextPrimaryKeyValue.set(Bytes.toLong(tableStatus.nextPrimaryKeyValue, 0));
            nextPageId = tableStatus.nextPageId;
            bootSequenceNumber = tableStatus.sequenceNumber;
            activePagesAtBoot.putAll(tableStatus.activePages);
        }
        keyToPage.start(bootSequenceNumber, created);

        dataStorageManager.cleanupAfterTableBoot(tableSpaceUUID, table.uuid, activePagesAtBoot.keySet());

        pageSet.setActivePagesAtBoot(activePagesAtBoot);

        initNewPages();
        if (!created) {
            LOGGER.log(Level.INFO, "loaded {0} keys for table {1}, newPageId {2}, nextPrimaryKeyValue {3}, activePages {4}",
                    new Object[]{keyToPage.size(), table.name, nextPageId, nextPrimaryKeyValue.get(), pageSet.getActivePages() + ""});
        }
        tableSpaceManager.rebuildForeignKeyReferences(table);
        started = true;
    }

    @Override
    public void rebuildForeignKeyReferences(Table tableThatWasModified) {
        this.childrenTables = tableSpaceManager.collectChildrenTables(this.table);
    }

    @Override
    public CompletableFuture<StatementExecutionResult> executeStatementAsync(Statement statement, Transaction transaction, StatementEvaluationContext context) {
        CompletableFuture<StatementExecutionResult> res;
        long lockStamp = checkpointLock.readLock();
        if (statement instanceof UpdateStatement) {
            UpdateStatement update = (UpdateStatement) statement;
            res = executeUpdateAsync(update, transaction, context);
        } else if (statement instanceof InsertStatement) {
            InsertStatement insert = (InsertStatement) statement;
            res = executeInsertAsync(insert, transaction, context);
        } else if (statement instanceof GetStatement) {
            GetStatement get = (GetStatement) statement;
            res = executeGetAsync(get, transaction, context);
        } else if (statement instanceof DeleteStatement) {
            DeleteStatement delete = (DeleteStatement) statement;
            res = executeDeleteAsync(delete, transaction, context);
        } else if (statement instanceof TruncateTableStatement) {
            try {
                TruncateTableStatement truncate = (TruncateTableStatement) statement;
                res = CompletableFuture.completedFuture(executeTruncate(truncate, transaction, context));
            } catch (StatementExecutionException err) {
                LOGGER.log(Level.SEVERE, "Truncate table failed", err);
                res = Futures.exception(err);
            }
        } else if (statement instanceof TableConsistencyCheckStatement) {
            DBManager manager = this.tableSpaceManager.getDbmanager();
            res = CompletableFuture.completedFuture(manager.createTableCheckSum((TableConsistencyCheckStatement) statement, context));
        } else {
            res = Futures.exception(new StatementExecutionException("not implemented " + statement.getClass()));
        }
        res = res.whenComplete((r, error) -> {
            checkpointLock.unlockRead(lockStamp);
        });
        if (statement instanceof TruncateTableStatement) {
            res = res.whenComplete((r, error) -> {
                if (error == null) {
                    try {
                        flush();
                    } catch (DataStorageManagerException err) {
                        throw new HerdDBInternalException(new StatementExecutionException("internal data error: " + err, err));
                    }
                }
            });
        }
        return res;
    }

    private Long allocateLivePage(Long lastKnownPageId) {
        /* This method expect that a new page actually exists! */
        nextPageLock.lock();

        final Long newId;
        Page.Metadata unload = null;
        try {

            /*
             * Use currentDirtyRecordsPage to check because nextPageId could be advanced for other needings
             * like rebuild a dirty page during checkpoint
             */
            if (lastKnownPageId == currentDirtyRecordsPage.get()) {

                final DataPage lastKnownPage;

                /* Is really a new page! */
                newId = nextPageId++;

                if (pages.containsKey(newId)) {
                    throw new IllegalStateException("invalid newpage id " + newId + ", " + newPages.keySet() + "/" + pages.keySet());
                }

                /*
                 * If really was the last new page id it MUST be in pages. It cannot be unloaded before the
                 * creation of a new page!
                 */
                lastKnownPage = pages.get(lastKnownPageId);
                if (lastKnownPage == null) {
                    throw new IllegalStateException("invalid last known new page id " + lastKnownPageId + ", " + newPages.keySet() + "/" + pages.keySet());
                }

                createNewPage(newId);

                /*
                 * Now we must add the "lastKnownPage" to page replacement policy. There is only one page living
                 * outside replacement policy (the currentDirtyRecordsPage)
                 */
                unload = pageReplacementPolicy.add(lastKnownPage);

            } else {

                /* The page has been published for sure */
                newId = currentDirtyRecordsPage.get();
            }

        } finally {
            nextPageLock.unlock();
        }

        /* Dereferenced page unload. Out of locking */
        if (unload != null) {
            unload.owner.unload(unload.pageId);
        }

        /* Both created now or already created */
        return newId;
    }

    /**
     * Initialize newPages map
     * <p>
     * Will not place any lock, this method should be invoked <b>during "stop-the-world" procedures!</b>
     * </p>
     */
    private void initNewPages() {

        if (!newPages.isEmpty()) {
            throw new IllegalStateException("invalid new page initialization, other new pages already exist: " + newPages.keySet());
        }

        createNewPage(nextPageId++);
    }

    /**
     * Create a new page and set it as the target page for dirty records.
     * <p>
     * Will not place any lock, this method should be invoked <b>during "stop-the-world" procedures!</b>
     * </p>
     */
    private void createNewPage(long newId) {

        final DataPage newPage = new DataPage(this, newId, maxLogicalPageSize, 0, new ConcurrentHashMap<>(), false);

        newPages.put(newId, newPage);
        pages.put(newId, newPage);

        /* From this moment on the page has been published */
        /* The lock is needed to block other threads up to this point */
        currentDirtyRecordsPage.set(newId);
    }

    /**
     * Create a new mutable page <b>not considered a <i>new page</i></b>.
     * <p>
     * This method should be invoked only during checkpoint to handle pages rebuild
     * </p>
     *
     * @param newId       new mutable page id
     * @param initialData initial page data
     * @param newPageSize initial page size
     * @return new mutable page
     */
    private DataPage createMutablePage(long newId, int expectedSize, long initiaPageSize) {

        LOGGER.log(Level.FINER, "creating mutable page table {0}, pageId={1} with {2} records, {3} logical page size",
                new Object[]{table.name, newId, expectedSize, initiaPageSize});

        final DataPage newPage = new DataPage(this, newId, maxLogicalPageSize, initiaPageSize, new HashMap<Bytes, Record>(expectedSize), false);

        pages.put(newId, newPage);

        /* From this moment the page has been published */
        return newPage;
    }

    /**
     * Returns currently loaded pages. To be used only for test to inspect current pages!
     */
    public Collection<DataPage> getLoadedPages() {
        return pages.values();
    }

    @Override
    public void unload(long pageId) {
        pages.computeIfPresent(pageId, (k, remove) -> {

                    unloadedPagesCount.increment();
                    if (LOGGER.isLoggable(Level.FINER)) {
                        LOGGER.log(Level.FINER, "table {0} removed page {1}, {2}", new Object[]{table.name, pageId, remove.getUsedMemory() / (1024 * 1024) + " MB"});
                    }

                    boolean dataFlushed = false;
                    if (!remove.immutable) {
                        dataFlushed = flushNewPageForUnload(remove);
                    }

                    if (LOGGER.isLoggable(Level.FINER)) {
                        if (dataFlushed) {
                            LOGGER.log(Level.FINER, "table {0} remove and save 'new' page {1}, {2}",
                                    new Object[]{table.name, remove.pageId, remove.getUsedMemory() / (1024 * 1024) + " MB"});
                        } else {
                            LOGGER.log(Level.FINER, "table {0} unload page {1}, {2}",
                                    new Object[]{table.name, pageId, remove.getUsedMemory() / (1024 * 1024) + " MB"});
                        }
                    }

                    return null;
                }
        );
    }

    private enum FlushNewPageResult {
        FLUSHED,
        ALREADY_FLUSHED,
        EMPTY_FLUSH
    }

    /**
     * Remove the page from {@link #newPages}, set it as "not writable" and write it
     * to disk
     * <p>
     * Add as much spare data as possible to fillup the page. If added must
     * change key to page pointers too for spare data
     * </p>
     *
     * @param page          new page to flush
     * @param spareDataPage old spare data to fit in the new page if possible
     */
    private void flushNewPageForCheckpoint(DataPage page, DataPage spareDataPage) {
        final FlushNewPageResult flush = flushNewPage(page, spareDataPage);
        switch (flush) {
            case FLUSHED:

                /*
                 * Replace the page in memory with his immutable version (faster modification checks). We can
                 * replace the page with the immutable one from memory running during a checkpoint and no other
                 * page write could happen (thus our page copy is fully equivalent to really flushed one).
                 *
                 * We replace the page only if we have actually flushed it.. if it was flushed by another thread
                 * it was flushed due to unload request (no concurrent checkpoints) and then removed from pages
                 * (or the thread is going to remove it). We don't want to keep knowledge of a page not know
                 * anymore to page replacement policy (It can't be remove again)
                 *
                 * For similar reason we replace the page only if there actually is a page in the first place. If
                 * a concurrent thread flushed and removed the page we don't want to add it again.
                 */
                pages.computeIfPresent(page.pageId, (i, p) -> p.toImmutable());
                return;

            case ALREADY_FLUSHED:
                /* Already flushed (and possibly unloaded) by another thread */
                LOGGER.log(Level.INFO, "New page {0} already flushed in a concurrent thread", page.pageId);
                return;

            case EMPTY_FLUSH:
                /* Attempted and empty flush. The page has been dropped and we must remove it from page knowledge */
                pageReplacementPolicy.remove(page);
                pages.remove(page.pageId);
                return;

            default:
                throw new IllegalArgumentException("Unknown new page flush result: " + flush);
        }
    }

    /**
     * Remove the page from {@link #newPages}, set it as "unloaded" and write it to disk
     *
     * @param page new page to flush
     * @return {@code true} if the page has been flushed, {@code false} if not flushed by any other means
     * (concurrent flush or empty page flush)
     */
    private boolean flushNewPageForUnload(DataPage page) {
        return FlushNewPageResult.FLUSHED == flushNewPage(page, null);
    }

    /**
     * Remove the page from {@link #newPages}, set it as "unloaded" and write it to disk
     * <p>
     * Add as much spare data as possible to fillup the page. If added must change key to page pointers
     * too for spare data
     * </p>
     *
     * @param page          new page to flush
     * @param spareDataPage old spare data to fit in the new page if possible
     * @return {@code false} if no flush has been done because the page isn't writable anymore,
     * {@code true} otherwise
     */
    private FlushNewPageResult flushNewPage(DataPage page, DataPage spareDataPage) {

        if (page.immutable) {
            LOGGER.log(Level.SEVERE, "Attempt to flush an immutable page {0} as it was mutable", page.pageId);
            throw new IllegalStateException("page " + page.pageId + " is not a new page!");
        }

        page.pageLock.readLock().lock();
        try {

            if (!page.writable) {
                LOGGER.log(Level.INFO, "Mutable page {0} already flushed in a concurrent thread", page.pageId);
                return FlushNewPageResult.ALREADY_FLUSHED;
            }

        } finally {
            page.pageLock.readLock().unlock();
        }

        /*
         * We need to keep the page lock just to write the unloaded flag... after that write any other
         * thread that check the page will avoid writes (thus using page data is safe).
         */
        final Lock lock = page.pageLock.writeLock();
        lock.lock();
        try {
            if (!page.writable) {
                LOGGER.log(Level.INFO, "Mutable page {0} already flushed in a concurrent thread", page.pageId);
                return FlushNewPageResult.ALREADY_FLUSHED;
            }

            /*
             * If there isn't any data to flush we'll avoid flush at all and just remove the page. We avoid to
             * copy data from spareDataPage to an empty page because it would just be an additional roundtrip
             * for spare data with an additional PK write too (consider that you have just one empty page and
             * an half full spareDataPage: why copy all data to the first and update PK records when you can
             * just save spareDataPage directly?)
             */
            boolean drop = page.isEmpty();

            /*
             * NewPages removal technically could be done before current write lock but
             * doing it in lock permit to check safely if a page has been removed. Without
             * lock we can't known if no page has been removed because already removed by a
             * concurrent thread or because it wasn't present in the first place.
             */
            /* Set the new page as a fully active page */
            if (!drop) {
                pageSet.pageCreated(page.pageId, page);
            }

            /* Remove it from "new" pages */
            DataPage remove = newPages.remove(page.pageId);
            if (remove == null) {
                LOGGER.log(Level.SEVERE, "Detected concurrent flush of page {0}, writable: {1}",
                        new Object[]{page.pageId, page.writable});
                throw new IllegalStateException("page " + page.pageId + " is not a new page!");
            }

            page.writable = false;

            if (drop) {
                LOGGER.log(Level.INFO, "Deleted empty mutable page {0} instead of flushing it", page.pageId);
                return FlushNewPageResult.EMPTY_FLUSH;
            }

        } finally {
            lock.unlock();
        }

        // Try to steal records from another temporary page
        if (spareDataPage != null) {
            /* Save current memory size */
            final long usedMemory = page.getUsedMemory();
            final long buildingPageMemory = spareDataPage.getUsedMemory();

            /* Flag to enable spare data addition to currently flushed page */
            boolean add = true;
            final Iterator<Record> records = spareDataPage.getRecordsForFlush().iterator();
            while (add && records.hasNext()) {
                Record record = records.next().nonShared();
                add = page.put(record);
                if (add) {
                    boolean moved = keyToPage.put(record.key, page.pageId, spareDataPage.pageId);
                    if (!moved) {
                        LOGGER.log(Level.SEVERE,
                                "Detected a dirty page as spare data page while flushing new page. Flushing new page {0}. Spare data page {1}",
                                new Object[]{page, spareDataPage});
                        throw new IllegalStateException(
                                "Expected a clean page for stealing records, got a dirty record " + record.key
                                        + ". Flushing new page " + page.pageId + ". Spare data page "
                                        + spareDataPage.pageId);
                    }

                    /* We remove "cleaned" record from the stealingDataPage */
                    records.remove();
                }
            }

            long spareUsedMemory = page.getUsedMemory() - usedMemory;

            /* Fix spare page data removing used memory */
            spareDataPage.setUsedMemory(buildingPageMemory - spareUsedMemory);
        }

        LOGGER.log(Level.FINER, "flushNewPage table {0}, pageId={1} with {2} records, {3} logical page size",
                new Object[]{table.name, page.pageId, page.size(), page.getUsedMemory()});
        dataStorageManager.writePage(tableSpaceUUID, table.uuid, page.pageId, page.getRecordsForFlush());

        return FlushNewPageResult.FLUSHED;
    }

    /**
     * Write a <i>mutable</i> but not <i>new</i> page.
     * <p>
     * <i>Mutable</i> but not <i>new</i> pages exists only during checkpoint (rebuilt pages due to dirtiness or because
     * too small).
     * </p>
     *
     * @param page             page to flush
     * @param keepPageInMemory add to page replacement policy as known "loaded" page or fully unload it
     */
    private void flushMutablePage(DataPage page, boolean keepPageInMemory) {

        LOGGER.log(Level.FINER, "flushing mutable page table {0}, pageId={1} with {2} records, {3} logical page size",
                new Object[]{table.name, page.pageId, page.size(), page.getUsedMemory()});

        if (page.immutable) {
            LOGGER.log(Level.SEVERE, "Attempt to flush an immutable page " + page.pageId + " as it was mutable");
            throw new IllegalStateException("page " + page.pageId + " is not an immutable page!");
        }

        page.pageLock.readLock().lock();
        try {

            if (!page.writable) {
                LOGGER.log(Level.SEVERE, "Attempt to flush a not writable mutable page " + page.pageId + " as it was writable");
                throw new IllegalStateException("page " + page.pageId + " is not a writable page!");
            }

        } finally {
            page.pageLock.readLock().unlock();
        }

        /*
         * We need to keep the page lock just to write the unloaded flag... after that write any other
         * thread that check the page will avoid writes (thus using page data is safe).
         */
        final Lock lock = page.pageLock.writeLock();
        lock.lock();

        try {

            try {

                page.writable = false;

            } finally {

                /* Fast unlock if we must keep the page in memory */
                if (keepPageInMemory) {
                    lock.unlock();
                }
            }

            dataStorageManager.writePage(tableSpaceUUID, table.uuid, page.pageId, page.getRecordsForFlush());

            /* Set the page as a fully active page */
            pageSet.pageCreated(page.pageId, page);

            if (keepPageInMemory) {
                /* If we must keep the page in memory we "covert" the page to immutable */
                pages.put(page.pageId, page.toImmutable());

                /*
                 * And we load to page replacement policy. This is a critic point: after adding page to page
                 * replacement policy knowledge it can be unloaded from another thread, we should finished any work
                 * on the page before of that.
                 */
                final Page.Metadata unload = pageReplacementPolicy.add(page);
                if (unload != null) {
                    unload.owner.unload(unload.pageId);
                }
            }


        } finally {

            /* If we must unload the page we have to keep the page "live" and locked until page write (or a concurrent
             * load could not find the page!!!!) */
            if (!keepPageInMemory) {
                /* Wipe out the page from memory (was just a temp page) */
                pages.remove(page.pageId);

                lock.unlock();
            }
        }

    }

    private LockHandle lockForWrite(Bytes key, Transaction transaction) {
        return lockForWrite(key, transaction, table.name, locksManager);
    }

    private static LockHandle lockForWrite(Bytes key, Transaction transaction, String lockKey, ILocalLockManager locksManager) {
//        LOGGER.log(Level.SEVERE, "lockForWrite for " + key + " tx " + transaction);
        try {
            if (transaction != null) {
                LockHandle lock = transaction.lookupLock(lockKey, key);
                if (lock != null) {
                    if (lock.write) {
                        // transaction already locked the key for writes
                        return lock;
                    } else {
                        // transaction already locked the key, but we need to upgrade the lock
                        locksManager.releaseLock(lock);
                        transaction.unregisterUpgradedLocksOnTable(lockKey, lock);
                        lock = locksManager.acquireWriteLockForKey(key);
                        transaction.registerLockOnTable(lockKey, lock);
                        return lock;
                    }
                } else {
                    lock = locksManager.acquireWriteLockForKey(key);
                    transaction.registerLockOnTable(lockKey, lock);
                    return lock;
                }
            } else {
                return locksManager.acquireWriteLockForKey(key);
            }
        } catch (HerdDBInternalException err) { // locktimeout or other internal lockmanager error
            throw err;
        } catch (RuntimeException err) { // locktimeout or other internal lockmanager error
            throw new StatementExecutionException(err);
        }
    }

    private LockHandle lockForRead(Bytes key, Transaction transaction) {
        return lockForRead(key, transaction, table.name, locksManager);
    }

    private static LockHandle lockForRead(Bytes key, Transaction transaction, String lockKey, ILocalLockManager locksManager) {
        try {
            if (transaction != null) {
                LockHandle lock = transaction.lookupLock(lockKey, key);
                if (lock != null) {
                    // transaction already locked the key
                    return lock;
                } else {
                    lock = locksManager.acquireReadLockForKey(key);
                    transaction.registerLockOnTable(lockKey, lock);
                    return lock;
                }
            } else {
                return locksManager.acquireReadLockForKey(key);
            }
        } catch (RuntimeException err) { // locktimeout or other internal lockmanager error
            throw new StatementExecutionException(err);
        }
    }

    private CompletableFuture<StatementExecutionResult> executeInsertAsync(InsertStatement insert, Transaction transaction, StatementEvaluationContext context) {
        /*
         an insert can succeed only if the row is valid and the "keys" structure  does not contain the requested key
         the insert will add the row in the 'buffer' without assigning a page to it
         locks: the insert uses global 'insert' lock on the table
         the insert will update the 'maxKey' for auto_increment primary keys
         */
        Bytes key;
        byte[] value;
        try {
            key = Bytes.from_array(insert.getKeyFunction().computeNewValue(null, context, tableContext));
            value = insert.getValuesFunction().computeNewValue(new Record(key, null), context, tableContext);
        } catch (StatementExecutionException validationError) {
            return Futures.exception(validationError);
        } catch (Throwable validationError) {
            return Futures.exception(new StatementExecutionException(validationError));
        }
        List<UniqueIndexLockReference> uniqueIndexes = null;
        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null || table.foreignKeys != null) {
            try {
                DataAccessor values = new Record(key, Bytes.from_array(value)).getDataAccessor(table);
                if (table.foreignKeys != null) {
                    for (ForeignKeyDef fk : table.foreignKeys) {
                        checkForeignKeyConstraintsAsChildTable(fk, values, context, transaction);
                    }
                }
                if (indexes != null) {
                    for (AbstractIndexManager index : indexes.values()) {
                        if (index.isUnique()) {
                            Bytes indexKey = RecordSerializer.serializeIndexKey(values, index.getIndex(), index.getColumnNames());
                            if (uniqueIndexes == null) {
                                uniqueIndexes = new ArrayList<>(1);
                            }
                            uniqueIndexes.add(new UniqueIndexLockReference(index, indexKey));
                        } else {
                            RecordSerializer.validateIndexableValue(values, index.getIndex(), index.getColumnNames());
                        }
                    }
                }
            } catch (IllegalArgumentException | herddb.utils.IllegalDataAccessException | StatementExecutionException err) {
                if (err instanceof StatementExecutionException) {
                    return Futures.exception(err);
                } else {
                    return Futures.exception(new StatementExecutionException(err.getMessage(), err));
                }
            }
        }


        final long size = DataPage.estimateEntrySize(key, value);
        if (size > maxLogicalPageSize) {
            return Futures
                    .exception(new RecordTooBigException("New record " + key + " is to big to be inserted: size " + size + ", max size " + maxLogicalPageSize));
        }

        CompletableFuture<StatementExecutionResult> res = null;
        LockHandle lock = null;
        try {
            lock = lockForWrite(key, transaction);
            if (uniqueIndexes != null) {
                for (UniqueIndexLockReference uniqueIndexLock : uniqueIndexes) {
                    AbstractIndexManager index = uniqueIndexLock.indexManager;
                    LockHandle lockForIndex = lockForWrite(uniqueIndexLock.key, transaction, index.getIndexName(), index.getLockManager());
                    if (transaction == null) {
                        uniqueIndexLock.lockHandle = lockForIndex;
                    }
                    if (index.valueAlreadyMapped(uniqueIndexLock.key, null)) {
                        res = Futures.exception(new UniqueIndexContraintViolationException(index.getIndexName(), key,
                                "key " + key + ", already exists in table " + table.name + " on UNIQUE index " + index.getIndexName()));
                    }
                    if (res != null) {
                        break;
                    }
                }
            }
        } catch (HerdDBInternalException err) {
            res = Futures.exception(err);
        }
        boolean fallbackToUpsert = false;
        if (res == null) {
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    // OK, INSERT on a DELETED record inside this transaction
                } else if (transaction.recordInserted(table.name, key) != null) {
                    // ERROR, INSERT on a INSERTED record inside this transaction
                    res = Futures.exception(new DuplicatePrimaryKeyException(key,
                            "key " + key + ", decoded as " + RecordSerializer.deserializePrimaryKey(key, table) + ", already exists in table " + table.name + " inside transaction " + transaction.transactionId));
                } else if (keyToPage.containsKey(key)) {
                    if (insert.isUpsert()) {
                        fallbackToUpsert = true;
                    } else {
                        res = Futures.exception(new DuplicatePrimaryKeyException(key,
                                "key " + key + ", decoded as " + RecordSerializer.deserializePrimaryKey(key, table) + ", already exists in table " + table.name + " during transaction " + transaction.transactionId));
                    }
                }
            } else if (keyToPage.containsKey(key)) {
                if (insert.isUpsert()) {
                    fallbackToUpsert = true;
                } else {
                    res = Futures.exception(new DuplicatePrimaryKeyException(key,
                            "key " + key + ", decoded as " + RecordSerializer.deserializePrimaryKey(key, table) + ", already exists in table " + table.name));
                }
            }
        }

        if (res == null) {
            LogEntry entry;
            if (fallbackToUpsert) {
                entry = LogEntryFactory.update(table, key, Bytes.from_array(value), transaction);
            } else {
                entry = LogEntryFactory.insert(table, key, Bytes.from_array(value), transaction);
            }
            CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
            res = pos.logSequenceNumber.thenApplyAsync((lsn) -> {
                apply(pos, entry, false);
                return new DMLStatementExecutionResult(entry.transactionId, 1, key,
                        insert.isReturnValues() ? Bytes.from_array(value) : null);
            }, tableSpaceManager.getCallbacksExecutor());
        }
        if (uniqueIndexes != null) {
            // TODO: reverse order
            for (UniqueIndexLockReference uniqueIndexLock : uniqueIndexes) {
                res = releaseWriteLock(res, uniqueIndexLock.lockHandle, uniqueIndexLock.indexManager.getLockManager());
            }
        }
        if (transaction == null) {
            res = releaseWriteLock(res, lock);
        }
        return res;
    }

    private void executeForeignKeyConstraintsAsParentTable(Table childTable, DataAccessor previousValuesOnParentTable,
                                                                             StatementEvaluationContext context,
                                                                             Transaction transaction, boolean delete) throws StatementExecutionException {
        // We are creating a SQL query and then using DBManager
        // using an SQL query will let us leverage the SQL Planner
        // and use the best index to perform the execution
        // the SQL Planner will cache the plan, and the plan will also be
        // invalidated consistently during DML operations.
        for (ForeignKeyDef fk : childTable.foreignKeys) {
            String query = parentForeignKeyQueries.computeIfAbsent(childTable.name + "." + fk.name + ".#" + delete, (l -> {
                if (fk.onDeleteAction == ForeignKeyDef.ACTION_CASCADE && delete) {
                    StringBuilder q = new StringBuilder("DELETE FROM ");
                    q.append(delimit(childTable.tablespace));
                    q.append(".");
                    q.append(delimit(childTable.name));
                    q.append(" WHERE ");
                    for (int i = 0; i < fk.columns.length; i++) {
                        if (i > 0) {
                            q.append(" AND ");
                        }
                        q.append(delimit(fk.columns[i]));
                        q.append("=?");
                    }
                    return q.toString();
                } else if (fk.onUpdateAction == ForeignKeyDef.ACTION_CASCADE && !delete) {
                    // we are not supporting ON UPDATE CASCADE because we should update the
                    // child table AFTER appling the change in the parent table
                    // the change is more complex, let's keep it for a future work
                    throw new StatementExecutionException("No supported ON UPDATE CASCADE");
                } else if ((fk.onDeleteAction == ForeignKeyDef.ACTION_SETNULL && delete)
                           || (fk.onUpdateAction == ForeignKeyDef.ACTION_SETNULL && !delete)) { // delete or update it is the same for SET NULL
                    StringBuilder q = new StringBuilder("UPDATE ");
                    q.append(delimit(childTable.tablespace));
                    q.append(".");
                    q.append(delimit(childTable.name));
                    q.append(" SET ");
                    for (int i = 0; i < fk.columns.length; i++) {
                        if (i > 0) {
                            q.append(",");
                        }
                        q.append(delimit(fk.columns[i]));
                        q.append("= NULL ");
                    }
                    q.append(" WHERE ");
                    for (int i = 0; i < fk.columns.length; i++) {
                        if (i > 0) {
                            q.append(" AND ");
                        }
                        q.append(delimit(fk.columns[i]));
                        q.append("=?");
                    }
                    return q.toString();
                } else {
                    // NO ACTION case, check that there is no matching record in the child table that wouble be invalidated
                    // with '*' we are not going to perform projections or copies
                    StringBuilder q = new StringBuilder("SELECT * FROM ");
                    q.append(delimit(childTable.tablespace));
                    q.append(".");
                    q.append(delimit(childTable.name));
                    q.append(" WHERE ");
                    for (int i = 0; i < fk.columns.length; i++) {
                        if (i > 0) {
                            q.append(" AND ");
                        }
                        q.append(delimit(fk.columns[i]));
                        q.append("=?");
                    }
                    return q.toString();
                }
            }));

            final List<Object> valuesToMatch = new ArrayList<>(fk.parentTableColumns.length);
            for (int i = 0; i < fk.parentTableColumns.length; i++) {
                valuesToMatch.add(previousValuesOnParentTable.get(fk.parentTableColumns[i]));
            }

            TransactionContext tx = transaction != null ? new TransactionContext(transaction.transactionId) : TransactionContext.NO_TRANSACTION;
            if (fk.onDeleteAction == ForeignKeyDef.ACTION_CASCADE && delete
                || fk.onUpdateAction == ForeignKeyDef.ACTION_CASCADE && !delete
                || fk.onUpdateAction == ForeignKeyDef.ACTION_SETNULL && !delete
                || fk.onDeleteAction == ForeignKeyDef.ACTION_SETNULL && delete) {
                tableSpaceManager.getDbmanager().executeSimpleStatement(tableSpaceManager.getTableSpaceName(),
                        query,
                        valuesToMatch,
                        -1, // every record
                        true, // keep read locks in TransactionContext
                        tx,
                        null);
            } else {
                boolean fkOk;
                try (DataScanner scan = tableSpaceManager.getDbmanager().executeSimpleQuery(tableSpaceManager.getTableSpaceName(),
                        query,
                        valuesToMatch,
                        1, // only one record
                        true, // keep read locks in TransactionContext
                        tx,
                        null);) {
                    List<DataAccessor> resultSet = scan.consume();
                    // we are on the parent side of the relation
                    // we are okay if there is no matching record
                    // TODO: return the list of PKs in order to implement CASCADE operations
                    fkOk = resultSet.isEmpty();
                } catch (DataScannerException err) {
                    throw new StatementExecutionException(err);
                }
                if (!fkOk) {
                    throw new ForeignKeyViolationException(fk.name, "foreignKey " + childTable.name + "." + fk.name + " violated");
                }
            }
        }
    }

    private void validateForeignKeyConsistency(ForeignKeyDef fk, StatementEvaluationContext context, Transaction transaction) throws StatementExecutionException {
        if (!tableSpaceManager.getDbmanager().isFullSQLSupportEnabled()) {
            // we cannot perform this validation without Calcite
            return;
        }
        Table parentTable = tableSpaceManager.getTableManagerByUUID(fk.parentTableId).getTable();
        StringBuilder query = new StringBuilder("SELECT * "
                + "      FROM " + delimit(this.tableSpaceManager.getTableSpaceName()) + "." + delimit(this.table.name) + " childtable "
                + "      WHERE NOT EXISTS (SELECT * "
                + "                        FROM " + delimit(this.tableSpaceManager.getTableSpaceName()) + "." + delimit(parentTable.name) + " parenttable "
                + "                        WHERE ");
        for (int i = 0; i < fk.columns.length; i++) {
            if (i > 0) {
                query.append(" AND ");
            }
            query.append("childtable.")
                    .append(delimit(fk.columns[i]))
                    .append(" = parenttable.")
                    .append(delimit(fk.parentTableColumns[i]));
        }
        query.append(")");
        TransactionContext tx = transaction != null ? new TransactionContext(transaction.transactionId) : TransactionContext.NO_TRANSACTION;
        boolean fkOk;
        try (DataScanner scan = tableSpaceManager.getDbmanager().executeSimpleQuery(tableSpaceManager.getTableSpaceName(),
                query.toString(),
                Collections.emptyList(),
                1, // only one record
                true, // keep read locks in TransactionContext
                tx,
                context);
        ) {
            List<DataAccessor> resultSet = scan.consume();
            fkOk = resultSet.isEmpty();
        } catch (DataScannerException err) {
            throw new StatementExecutionException(err);
        }
        if (!fkOk) {
            throw new ForeignKeyViolationException(fk.name, "foreignKey " + table.name + "." + fk.name + " violated");
        }
    }

    private void checkForeignKeyConstraintsAsChildTable(ForeignKeyDef fk, DataAccessor values, StatementEvaluationContext context, Transaction transaction) throws StatementExecutionException {
        // We are creating a SQL query and then using DBManager
        // using an SQL query will let us leverage the SQL Planner
        // and use the best index to perform the execution
        // the SQL Planner will cache the plan, and the plan will also be
        // invalidated consistently during DML operations.
        String query = childForeignKeyQueries.computeIfAbsent(fk.name, (l -> {
            Table parentTable = tableSpaceManager.getTableManagerByUUID(fk.parentTableId).getTable();
            // with '*' we are not going to perform projections or copies
            StringBuilder q = new StringBuilder("SELECT * FROM ");
            q.append(delimit(parentTable.tablespace));
            q.append(".");
            q.append(delimit(parentTable.name));
            q.append(" WHERE ");
            for (int i = 0; i < fk.parentTableColumns.length; i++) {
                if (i > 0) {
                    q.append(" AND ");
                }
                q.append(delimit(fk.parentTableColumns[i]));
                q.append("=?");
            }
            return q.toString();
        }));

        final List<Object> valuesToMatch = new ArrayList<>(fk.columns.length);
        boolean allNulls = true;
        for (int i = 0; i < fk.columns.length; i++) {
            Object value = values.get(fk.columns[i]);
            allNulls = allNulls && value == null;
            valuesToMatch.add(value);
        }
        if (allNulls) {
            // all of the values are null, so no check on the parent table
            return;
        }

        TransactionContext tx = transaction != null ? new TransactionContext(transaction.transactionId) : TransactionContext.NO_TRANSACTION;
        boolean fkOk;
        try (DataScanner scan = tableSpaceManager.getDbmanager().executeSimpleQuery(tableSpaceManager.getTableSpaceName(),
                query,
                valuesToMatch,
                1, // only one record
                true, // keep read locks in TransactionContext
                tx,
                null);
        ) {
            List<DataAccessor> resultSet = scan.consume();
            fkOk = !resultSet.isEmpty();
        } catch (DataScannerException err) {
            throw new StatementExecutionException(err);
        }
        if (!fkOk) {
            throw new ForeignKeyViolationException(fk.name, "foreignKey " + table.name + "." + fk.name + " violated");
        }
    }

    private CompletableFuture<StatementExecutionResult> releaseWriteLock(
            CompletableFuture<StatementExecutionResult> promise, LockHandle lock
    ) {
       return releaseWriteLock(promise, lock, locksManager);
    }

    private static <T> CompletableFuture<T> releaseWriteLock(
            CompletableFuture<T> promise, LockHandle lock, ILocalLockManager locksManager
    ) {
        if (lock == null) {
            return promise;
        }
        return promise.whenComplete((tr, error) -> {
            locksManager.releaseWriteLock(lock);
        });
    }

    @SuppressWarnings("serial")
    static class ExitLoop extends RuntimeException {

        final boolean continueWithTransactionData;

        public ExitLoop(boolean continueWithTransactionData) {
            /* Disable stacktrace generation */
            super(null, null, true, false);

            this.continueWithTransactionData = continueWithTransactionData;
        }

    }

    interface ScanResultOperation {

        /**
         * This function is expected to release the lock
         *
         * @param record
         * @param lockHandle lock on the record, maybe null
         * @throws StatementExecutionException
         * @throws DataStorageManagerException
         * @throws LogNotAvailableException
         */
        default void accept(Record record, LockHandle lockHandle) throws StatementExecutionException, DataStorageManagerException, LogNotAvailableException {
        }

        default void beginNewRecordsInTransactionBlock() {
        }
    }

    interface StreamResultOperationStatus {

        default void beginNewRecordsInTransactionBlock() {
        }

        default void endNewRecordsInTransactionBlock() {
        }
    }

    private CompletableFuture<StatementExecutionResult> executeUpdateAsync(UpdateStatement update, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
//        LOGGER.log(Level.SEVERE, "executeUpdateAsync, " + update + ", transaction " + transaction);
        AtomicInteger updateCount = new AtomicInteger();
        Holder<Bytes> lastKey = new Holder<>();
        Holder<byte[]> lastValue = new Holder<>();
        /*
         an update can succeed only if the row is valid, the key is contains in the "keys" structure
         the update will simply override the value of the row, assigning a null page to the row
         the update can have a 'where' predicate which is to be evaluated against the decoded row, the update will be executed only if the predicate returns boolean 'true' value  (CAS operation)
         locks: the update  uses a lock on the the key
         */
        RecordFunction function = update.getFunction();
        long transactionId = transaction != null ? transaction.transactionId : 0;
        Predicate predicate = update.getPredicate();

        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        ScanStatement scan = new ScanStatement(table.tablespace, table, predicate);
        List<CompletableFuture<PendingLogEntryWork>> writes = new ArrayList<>();
        try {
            accessTableData(scan, context, new ScanResultOperation() {
                @Override
                public void accept(Record current, LockHandle lockHandle) throws StatementExecutionException, LogNotAvailableException, DataStorageManagerException {

                    List<UniqueIndexLockReference> uniqueIndexes = null;
                    byte[] newValue;
                    try {
                        if (childrenTables != null) {
                            DataAccessor currentValues = current.getDataAccessor(table);
                            for (Table childTable : childrenTables) {
                                executeForeignKeyConstraintsAsParentTable(childTable, currentValues, context, transaction, false);
                            }
                        }
                        newValue = function.computeNewValue(current, context, tableContext);
                        if (indexes != null || table.foreignKeys != null) {
                            DataAccessor values = new Record(current.key, Bytes.from_array(newValue)).getDataAccessor(table);
                            if (table.foreignKeys != null) {
                                for (ForeignKeyDef fk : table.foreignKeys) {
                                    checkForeignKeyConstraintsAsChildTable(fk, values, context, transaction);
                                }
                            }
                            if (indexes != null) {
                                for (AbstractIndexManager index : indexes.values()) {
                                    if (index.isUnique()) {
                                        Bytes indexKey = RecordSerializer.serializeIndexKey(values, index.getIndex(), index.getColumnNames());
                                        if (uniqueIndexes == null) {
                                            uniqueIndexes = new ArrayList<>(1);
                                        }
                                        UniqueIndexLockReference uniqueIndexLock = new UniqueIndexLockReference(index, indexKey);
                                        uniqueIndexes.add(uniqueIndexLock);
                                        LockHandle lockForIndex = lockForWrite(uniqueIndexLock.key, transaction, index.getIndexName(), index.getLockManager());
                                        if (transaction == null) {
                                            uniqueIndexLock.lockHandle = lockForIndex;
                                        }
                                        if (index.valueAlreadyMapped(indexKey, current.key)) {
                                            throw new UniqueIndexContraintViolationException(index.getIndexName(), indexKey, "Value " + indexKey + " already present in index " + index.getIndexName());
                                        }
                                    } else {
                                        RecordSerializer.validateIndexableValue(values, index.getIndex(), index.getColumnNames());
                                    }
                                }
                            }
                        }
                    } catch (IllegalArgumentException | herddb.utils.IllegalDataAccessException | StatementExecutionException err) {
                        locksManager.releaseLock(lockHandle);
                        StatementExecutionException finalError;
                        if (!(err instanceof StatementExecutionException)) {
                            finalError = new StatementExecutionException(err.getMessage(), err);
                        } else {
                            finalError = (StatementExecutionException) err;
                        }
                        CompletableFuture<PendingLogEntryWork> res = Futures.exception(finalError);
                        if (uniqueIndexes != null) {
                            for (UniqueIndexLockReference lock : uniqueIndexes) {
                                res = releaseWriteLock(res, lock.lockHandle, lock.indexManager.getLockManager());
                            }
                        }
                        writes.add(res);
                        return;
                    }

                    final long size = DataPage.estimateEntrySize(current.key, newValue);
                    if (size > maxLogicalPageSize) {
                        locksManager.releaseLock(lockHandle);
                        writes.add(Futures.exception(new RecordTooBigException("New version of record " + current.key
                                + " is to big to be update: new size " + size + ", actual size " + DataPage.estimateEntrySize(current)
                                + ", max size " + maxLogicalPageSize)));
                        return;
                    }

                    LogEntry entry = LogEntryFactory.update(table, current.key, Bytes.from_array(newValue), transaction);
                    CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                    final List<UniqueIndexLockReference> _uniqueIndexes = uniqueIndexes;
                    writes.add(pos.logSequenceNumber.thenApply(lsn -> new PendingLogEntryWork(entry, pos, lockHandle, _uniqueIndexes)));
                    lastKey.value = current.key;
                    lastValue.value = newValue;
                    updateCount.incrementAndGet();
                }
            }, transaction, true, true);
        } catch (HerdDBInternalException err) {
            LOGGER.log(Level.SEVERE, "bad error during an update", err);
            return Futures.exception(err);
        }

        if (writes.isEmpty()) {
            return CompletableFuture
                    .completedFuture(new DMLStatementExecutionResult(transactionId, 0, null, null));
        }

        if (writes.size() == 1) {
            return writes.get(0)
                    .whenCompleteAsync((pending, error) -> {
                        try {
                            // in case of any error (write to log + validations) we do not
                            // apply any of the DML operations
                            if (error == null) {
                                apply(pending.pos, pending.entry, false);
                            }
                        } finally {
                            releaseMultiplePendingLogEntryWorks(writes);
                        }
                    }, tableSpaceManager.getCallbacksExecutor())
                    .thenApply((pending) -> {
                        return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
                                update.isReturnValues() ? (lastValue.value != null ? Bytes.from_array(lastValue.value) : null) : null);
                    });
        } else {
            return Futures
                    .collect(writes)
                    .whenCompleteAsync((pendings, error) -> {
                        try {
                            // in case of any error (write to log + validations) we do not
                            // apply any of the DML operations
                            if (error == null) {
                                for (PendingLogEntryWork pending : pendings) {
                                    apply(pending.pos, pending.entry, false);
                                }
                            }
                        } finally {
                            releaseMultiplePendingLogEntryWorks(writes);
                        }
                    }, tableSpaceManager.getCallbacksExecutor())
                    .thenApply((pendings) -> {
                        return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
                                update.isReturnValues() ? (lastValue.value != null ? Bytes.from_array(lastValue.value) : null) : null);
                    });
        }

    }

    private void releaseMultiplePendingLogEntryWorks(List<CompletableFuture<PendingLogEntryWork>> writes) {
        for (CompletableFuture<PendingLogEntryWork> pending : writes) {
            PendingLogEntryWork now = Futures.getIfSuccess(pending);
            if (now != null) {
                releasePendingLogEntryWorkLocks(now);
            }
        }
    }

    private static class PendingLogEntryWork {
        final LogEntry entry;
        final CommitLogResult pos;
        final LockHandle lockHandle;
        final List<UniqueIndexLockReference> uniqueIndexes;

        public PendingLogEntryWork(LogEntry entry, CommitLogResult pos, LockHandle lockHandle, List<UniqueIndexLockReference> uniqueIndexes) {
            super();
            this.entry = entry;
            this.pos = pos;
            this.lockHandle = lockHandle;
            this.uniqueIndexes = uniqueIndexes;
        }
    }

    private CompletableFuture<StatementExecutionResult> executeDeleteAsync(DeleteStatement delete, Transaction transaction, StatementEvaluationContext context) {

        AtomicInteger updateCount = new AtomicInteger();
        Holder<Bytes> lastKey = new Holder<>();
        Holder<Bytes> lastValue = new Holder<>();

        long transactionId = transaction != null ? transaction.transactionId : 0;
        Predicate predicate = delete.getPredicate();
        List<CompletableFuture<PendingLogEntryWork>> writes = new ArrayList<>();

        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);

        ScanStatement scan = new ScanStatement(table.tablespace, table, predicate);
        try {
            accessTableData(scan, context, new ScanResultOperation() {
                @Override
                public void accept(Record current, LockHandle lockHandle) throws StatementExecutionException, LogNotAvailableException, DataStorageManagerException {
                    // ensure we are holding the write locks on every unique index
                    List<UniqueIndexLockReference> uniqueIndexes = null;
                    try {
                        if (indexes != null || childrenTables != null) {
                            DataAccessor dataAccessor = current.getDataAccessor(table);
                            if (childrenTables != null) {
                                for (Table childTable : childrenTables) {
                                    executeForeignKeyConstraintsAsParentTable(childTable, dataAccessor, context, transaction, true);
                                }
                            }
                            if (indexes != null) {
                                for (AbstractIndexManager index : indexes.values()) {
                                    if (index.isUnique()) {
                                        Bytes indexKey = RecordSerializer.serializeIndexKey(dataAccessor, index.getIndex(), index.getColumnNames());
                                        if (uniqueIndexes == null) {
                                            uniqueIndexes = new ArrayList<>(1);
                                        }
                                        UniqueIndexLockReference uniqueIndexLock = new UniqueIndexLockReference(index, indexKey);
                                        uniqueIndexes.add(uniqueIndexLock);
                                        LockHandle lockForIndex = lockForWrite(uniqueIndexLock.key, transaction, index.getIndexName(), index.getLockManager());
                                        if (transaction == null) {
                                            uniqueIndexLock.lockHandle = lockForIndex;
                                        }
                                    }
                                }
                            }
                        }
                    } catch (IllegalArgumentException | herddb.utils.IllegalDataAccessException | StatementExecutionException err) {
                        locksManager.releaseLock(lockHandle);
                        StatementExecutionException finalError;
                        if (!(err instanceof StatementExecutionException)) {
                            finalError = new StatementExecutionException(err.getMessage(), err);
                        } else {
                            finalError = (StatementExecutionException) err;
                        }
                        CompletableFuture<PendingLogEntryWork> res = Futures.exception(finalError);
                        if (uniqueIndexes != null) {
                            for (UniqueIndexLockReference lock : uniqueIndexes) {
                                res = releaseWriteLock(res, lockHandle, lock.indexManager.getLockManager());
                            }
                        }
                        writes.add(res);
                        return;
                    }


                    LogEntry entry = LogEntryFactory.delete(table, current.key, transaction);
                    CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                    final List<UniqueIndexLockReference> _uniqueIndexes = uniqueIndexes;
                    writes.add(pos.logSequenceNumber.thenApply(lsn -> new PendingLogEntryWork(entry, pos, lockHandle, _uniqueIndexes)));
                    lastKey.value = current.key;
                    lastValue.value = current.value;
                    updateCount.incrementAndGet();
                }
            }, transaction, true, true);
        } catch (HerdDBInternalException err) {
            LOGGER.log(Level.SEVERE, "bad error during a delete", err);
            return Futures.exception(err);
        }
        if (writes.isEmpty()) {
            return CompletableFuture
                    .completedFuture(new DMLStatementExecutionResult(transactionId, 0, null, null));
        }

        if (writes.size() == 1) {
            return writes.get(0)
                    .whenCompleteAsync((pending, error) -> {
                        try {
                            // in case of any error (write to log + validations) we do not
                            // apply any of the DML operations
                            if (error == null) {
                                apply(pending.pos, pending.entry, false);
                            }
                        } finally {
                            releaseMultiplePendingLogEntryWorks(writes);
                        }
                    }, tableSpaceManager.getCallbacksExecutor())
                    .thenApply((pending) -> {
                        return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
                                delete.isReturnValues() ? lastValue.value : null);
                    });
        } else {
            return Futures
                    .collect(writes)
                    .whenCompleteAsync((pendings, error) -> {
                        try {
                            // in case of any error (write to log + validations) we do not
                            // apply any of the DML operations
                            if (error == null) {
                                for (PendingLogEntryWork pending : pendings) {
                                    apply(pending.pos, pending.entry, false);
                                }
                            }
                        } finally {
                            releaseMultiplePendingLogEntryWorks(writes);
                        }
                    }, tableSpaceManager.getCallbacksExecutor())
                    .thenApply((pendings) -> {
                        return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
                                delete.isReturnValues() ? lastValue.value : null);
                    });
        }
    }

    private void releasePendingLogEntryWorkLocks(PendingLogEntryWork pending) {
        if (pending.uniqueIndexes != null) {
            for (UniqueIndexLockReference ref : pending.uniqueIndexes) {
                ref.indexManager.getLockManager().releaseLock(ref.lockHandle);
            }
        }
        if (pending.lockHandle != null) {
            locksManager.releaseLock(pending.lockHandle);
        }
    }

    private StatementExecutionResult executeTruncate(TruncateTableStatement truncate, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
        if (transaction != null) {
            throw new StatementExecutionException("TRUNCATE TABLE cannot be executed within the context of a Transaction");
        }
        try {
            long estimatedSize = keyToPage.size();
            LOGGER.log(Level.INFO, "TRUNCATING TABLE {0} with approx {1} records", new Object[]{table.name, estimatedSize});
            LogEntry entry = LogEntryFactory.truncate(table, null);
            CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);
            return new DMLStatementExecutionResult(0, estimatedSize > Integer.MAX_VALUE
                    ? Integer.MAX_VALUE : (int) estimatedSize, null, null);
        } catch (LogNotAvailableException | DataStorageManagerException error) {
            LOGGER.log(Level.SEVERE, "Error during TRUNCATE table " + table.tablespace + "." + table.name, error);
            throw new StatementExecutionException(error);
        }
    }

    private void applyTruncate() throws DataStorageManagerException {
        if (createdInTransaction > 0) {
            throw new DataStorageManagerException("TRUNCATE TABLE cannot be executed on an uncommitted table");
        }
        if (checkPointRunning) {
            throw new DataStorageManagerException("TRUNCATE TABLE cannot be executed during a checkpoint");
        }
        if (tableSpaceManager.isTransactionRunningOnTable(table.name)) {
            throw new DataStorageManagerException("TRUNCATE TABLE cannot be executed table " + table.tablespace + "."
                    + table.name + ": at least one transaction is pending on it");
        }
        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            for (AbstractIndexManager index : indexes.values()) {
                if (!index.isAvailable()) {
                    throw new DataStorageManagerException("index " + index.getIndexName()
                            + " in not full available. Cannot TRUNCATE table " + table.tablespace + "." + table.name);
                }
            }
        }

        unloadAllPagesForTruncate();

        pageSet.truncate();

        pages.clear();
        newPages.clear();

        initNewPages();

        locksManager.clear();
        keyToPage.truncate();
        if (indexes != null) {
            for (AbstractIndexManager index : indexes.values()) {
                index.truncate();
            }
        }

    }

    private void unloadAllPagesForTruncate() {
        /* Do not unload the current working page not known to replacement policy */
        final long currentDirtyPageId = currentDirtyRecordsPage.get();
        final List<DataPage> unload = pages.values().stream()
                .filter(page -> page.pageId != currentDirtyPageId)
                .collect(Collectors.toList());

        pageReplacementPolicy.remove(unload);
    }

    @Override
    public void onTransactionCommit(Transaction transaction, boolean recovery) throws DataStorageManagerException {
        if (transaction == null) {
            throw new DataStorageManagerException("transaction cannot be null");
        }
        boolean forceFlushTableData = false;
        if (createdInTransaction > 0) {
            if (transaction.transactionId != createdInTransaction) {
                throw new DataStorageManagerException("table " + table.tablespace + "." + table.name + " is available only on transaction " + createdInTransaction);
            }
            createdInTransaction = 0;
            forceFlushTableData = true;
        }

        if (!transaction.lastSequenceNumber.after(bootSequenceNumber)) {
            if (recovery) {
                LOGGER.log(Level.FINER,
                        "ignoring transaction {0} commit on recovery, {1}.{2} data is newer: transaction {3}, table {4}",
                        new Object[]{transaction.transactionId, table.tablespace, table.name,
                                transaction.lastSequenceNumber, bootSequenceNumber});
                return;
            } else {
                throw new DataStorageManagerException("corrupted commit log " + table.tablespace + "." + table.name
                        + " data is newer than transaction " + transaction.transactionId + " transaction "
                        + transaction.lastSequenceNumber + " table " + bootSequenceNumber);
            }
        }

        boolean lockAcquired;
        try {
            lockAcquired = checkpointLock.asReadLock().tryLock(CHECKPOINT_LOCK_READ_TIMEOUT, SECONDS);
        } catch (InterruptedException err) {
            throw new DataStorageManagerException("interrupted while acquiring checkpoint lock during a commit", err);
        }
        if (!lockAcquired) {
            throw new DataStorageManagerException("timed out while acquiring checkpoint lock during a commit");
        }
        try {
            Map<Bytes, Record> changedRecords = transaction.changedRecords.get(table.name);
            // transaction is still holding locks on each record, so we can change records
            Map<Bytes, Record> newRecords = transaction.newRecords.get(table.name);
            if (newRecords != null) {
                for (Record record : newRecords.values()) {
                    applyInsert(record.key, record.value, true);
                }
            }
            if (changedRecords != null) {
                for (Record r : changedRecords.values()) {
                    applyUpdate(r.key, r.value);
                }
            }
            Set<Bytes> deletedRecords = transaction.deletedRecords.get(table.name);
            if (deletedRecords != null) {
                for (Bytes key : deletedRecords) {
                    applyDelete(key);
                }
            }
        } finally {
            checkpointLock.asReadLock().unlock();
        }
        transaction.releaseLocksOnTable(table.name, locksManager);
        if (forceFlushTableData) {
            LOGGER.log(Level.FINE, "forcing local checkpoint, table " + table.name + " will be visible to all transactions now");
            checkpoint(false);
        }
    }

    @Override
    public void onTransactionRollback(Transaction transaction) {
        transaction.releaseLocksOnTable(table.name, locksManager);
    }

    @Override
    public void apply(CommitLogResult writeResult, LogEntry entry, boolean recovery) throws DataStorageManagerException,
            LogNotAvailableException {
        if (recovery) {
            if (writeResult.deferred) {
                throw new DataStorageManagerException("impossibile to have a deferred CommitLogResult during recovery");
            }
            LogSequenceNumber position = writeResult.getLogSequenceNumber();
            if (dumpLogSequenceNumber != null && !position.after(dumpLogSequenceNumber)) {
                // in "restore mode" the 'position" parameter is from the 'old' transaction log
                Transaction transaction = null;
                if (entry.transactionId > 0) {
                    transaction = tableSpaceManager.getTransaction(entry.transactionId);
                }
                if (transaction != null) {
                    transaction.touch();
                    LOGGER.log(Level.FINER, "{0}.{1} keep {2} at {3}, table restored from position {4}, it belongs to transaction {5} which was in progress during the dump of the table", new Object[]{table.tablespace, table.name, entry, position, dumpLogSequenceNumber, entry.transactionId});
                } else {
                    LOGGER.log(Level.FINER, "{0}.{1} skip {2} at {3}, table restored from position {4}", new Object[]{table.tablespace, table.name, entry, position, dumpLogSequenceNumber});
                    return;
                }
            } else if (!position.after(bootSequenceNumber)) {
                // recovery mode
                Transaction transaction = null;
                if (entry.transactionId > 0) {
                    transaction = tableSpaceManager.getTransaction(entry.transactionId);
                }
                if (transaction != null) {
                    transaction.touch();
                    LOGGER.log(Level.FINER, "{0}.{1} keep {2} at {3}, table booted at {4}, it belongs to transaction {5} which was in progress during the flush of the table", new Object[]{table.tablespace, table.name, entry, position, bootSequenceNumber, entry.transactionId});
                } else {
                    LOGGER.log(Level.FINER, "{0}.{1} skip {2} at {3}, table booted at {4}", new Object[]{table.tablespace, table.name, entry, position, bootSequenceNumber});
                    return;
                }
            }
        }
        if (writeResult.sync) {
            // wait for data to be stored to log
            writeResult.getLogSequenceNumber();
        }

        switch (entry.type) {
            case LogEntryType.DELETE: {
                // remove the record from the set of existing records
                Bytes key = entry.key;
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    if (transaction == null) {
                        /* Ignore missing transaction only if during recovery and ignore property is active */
                        if (recovery && ignoreMissingTransactionsOnRecovery) {
                            LOGGER.log(Level.WARNING, "Ignoring delete of {0} due to missing transaction {1}",
                                    new Object[]{entry.key, entry.transactionId});
                        } else {
                            throw new DataStorageManagerException("no such transaction " + entry.transactionId);
                        }
                    } else {
                        transaction.registerDeleteOnTable(this.table.name, key, writeResult);
                    }
                } else {
                    applyDelete(key);
                }
                break;
            }
            case LogEntryType.UPDATE: {
                Bytes key = entry.key;
                Bytes value = entry.value;
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    if (transaction == null) {
                        /* Ignore missing transaction only if during recovery and ignore property is active */
                        if (recovery && ignoreMissingTransactionsOnRecovery) {
                            LOGGER.log(Level.WARNING, "Ignoring update of {0} due to missing transaction {1}",
                                    new Object[]{entry.key, entry.transactionId});
                        } else {
                            throw new DataStorageManagerException("no such transaction " + entry.transactionId);
                        }
                    } else {
                        transaction.registerRecordUpdate(this.table.name, key, value, writeResult);
                    }
                } else {
                    applyUpdate(key, value);
                }
                break;
            }
            case LogEntryType.INSERT: {
                Bytes key = entry.key;
                Bytes value = entry.value;
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    if (transaction == null) {
                        /* Ignore missing transaction only if during recovery and ignore property is active */
                        if (recovery && ignoreMissingTransactionsOnRecovery) {
                            LOGGER.log(Level.WARNING, "Ignoring insert of {0} due to missing transaction {1}",
                                    new Object[]{entry.key, entry.transactionId});
                        } else {
                            throw new DataStorageManagerException("no such transaction " + entry.transactionId);
                        }
                    } else {
                        transaction.registerInsertOnTable(table.name, key, value, writeResult);
                    }
                } else {
                    applyInsert(key, value, false);
                }
                break;
            }
            case LogEntryType.TRUNCATE_TABLE: {
                applyTruncate();
            }
            break;
            default:
                throw new IllegalArgumentException("unhandled entry type " + entry.type);
        }

    }

    private void applyDelete(Bytes key) throws DataStorageManagerException {
        /* This could be a normal or a temporary modifiable page */
        final Long pageId = keyToPage.remove(key);
        if (pageId == null) {
            throw new IllegalStateException("corrupted transaction log: key " + key + " is not present in table "
                    + table.tablespace + "." + table.name);
        }

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Deleted key " + key + " from page " + pageId + " from table " + table.tablespace
                    + "." + table.name);
        }


        /*
         * We'll try to remove the record if in a writable page, otherwise we'll simply set the old page
         * as dirty.
         */
        final Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);

        /*
         * When index is enabled we need the old value to update them, we'll force the page load only if that
         * record is really needed.
         */
        final DataPage page;
        final Record previous;
        if (indexes == null) {
            /* We don't need the page if isn't loaded or isn't a mutable new page */
            page = newPages.get(pageId);

            if (page != null) {
                pageReplacementPolicy.pageHit(page);
                previous = page.get(key);

                if (previous == null) {
                    throw new IllegalStateException("corrupted PK: old page " + pageId + " for deleted record at " + key
                            + " was not found in table " + table.tablespace + "." + table.name);
                }
            } else {
                previous = null;
            }
        } else {
            /* We really need the page for update index old values */
            page = loadPageToMemory(pageId, false);
            previous = page.get(key);

            if (previous == null) {
                throw new IllegalStateException("corrupted PK: old page " + pageId + " for deleted record at " + key
                        + " was not found in table " + table.tablespace + "." + table.name);
            }
        }

        if (page == null || page.immutable) {
            /* Unloaded or immutable, set it as dirty */
            pageSet.setPageDirty(pageId, previous);
        } else {
            /* Mutable page, need to check if still modifiable or already unloaded */
            final Lock lock = page.pageLock.readLock();
            lock.lock();
            try {
                if (page.writable) {
                    /* We can modify the page directly */
                    page.remove(key);
                } else {
                    /* Unfortunately is not writable (anymore), set it as dirty */
                    pageSet.setPageDirty(pageId, previous);
                }
            } finally {
                lock.unlock();
            }
        }

        if (indexes != null) {

            /* If there are indexes e have already forced a page load and previous record has been loaded */
            DataAccessor values = previous.getDataAccessor(table);
            for (AbstractIndexManager index : indexes.values()) {
                Bytes indexKey = RecordSerializer.serializeIndexKey(values, index.getIndex(), index.getColumnNames());
                index.recordDeleted(key, indexKey);
            }
        }
    }

    private void applyUpdate(Bytes key, Bytes value) throws DataStorageManagerException {
        // do not want to retain shared buffers as keys
        key = key.nonShared();

        /*
         * New record to be updated, it will always updated if there aren't errors thus is simpler to create
         * the record now
         */
        final Record record = new Record(key, value);

        /* This could be a normal or a temporary modifiable page */
        final Long prevPageId = keyToPage.get(key);
        if (prevPageId == null) {
            throw new IllegalStateException("corrupted transaction log: key " + key + " is not present in table "
                    + table.tablespace + "." + table.name);
        }

        /*
         * We'll try to replace the record if in a writable page, otherwise we'll simply set the old page
         * as dirty and continue like a normal insertion
         */
        final Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);

        /*
         * When index is enabled we need the old value to update them, we'll force the page load only if that
         * record is really needed.
         */
        final DataPage prevPage;
        final Record previous;
        boolean insertedInSamePage = false;
        if (indexes == null) {
            /* We don't need the page if isn't loaded or isn't a mutable new page*/
            prevPage = newPages.get(prevPageId);

            if (prevPage != null) {
                pageReplacementPolicy.pageHit(prevPage);
                previous = prevPage.get(key);

                if (previous == null) {
                    throw new IllegalStateException("corrupted PK: old page " + prevPageId + " for updated record at "
                            + key + " was not found in table " + table.tablespace + "." + table.name);
                }
            } else {
                previous = null;
            }

        } else {
            /* We really need the page for update index old values */
            prevPage = loadPageToMemory(prevPageId, false);
            previous = prevPage.get(key);

            if (previous == null) {
                throw new IllegalStateException("corrupted PK: old page " + prevPageId + " for updated record at " + key
                        + " was not found in table" + table.tablespace + "." + table.name);
            }
        }

        if (prevPage == null || prevPage.immutable) {
            /* Unloaded or immutable, set it as dirty */
            pageSet.setPageDirty(prevPageId, previous);
        } else {
            /* Mutable page, need to check if still modifiable or already unloaded */
            final Lock lock = prevPage.pageLock.readLock();
            lock.lock();
            try {
                if (prevPage.writable) {
                    /* We can try to modify the page directly */
                    insertedInSamePage = prevPage.put(record);
                } else {
                    /* Unfortunately is not writable (anymore), set it as dirty */
                    pageSet.setPageDirty(prevPageId, previous);
                }
            } finally {
                lock.unlock();
            }
        }

        /* Insertion page */
        Long insertionPageId;

        if (insertedInSamePage) {
            /* Inserted in temporary mutable previous page, no need to alter keyToPage too: no record page change */
            insertionPageId = prevPageId;
        } else {
            /* Do real insertion */
            insertionPageId = currentDirtyRecordsPage.get();

            while (true) {
                final DataPage newPage = newPages.get(insertionPageId);

                if (newPage != null) {
                    pageReplacementPolicy.pageHit(newPage);

                    /* The temporary memory page could have been unloaded and loaded again in meantime */
                    if (!newPage.immutable) {
                        /* Mutable page, need to check if still modifiable or already unloaded */
                        final Lock lock = newPage.pageLock.readLock();
                        lock.lock();
                        try {
                            if (newPage.writable) {
                                /* We can try to modify the page directly */
                                if (newPage.put(record)) {
                                    break;
                                }
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                }

                /* Try allocate a new page if no already done */
                insertionPageId = allocateLivePage(insertionPageId);
            }

            /* Update the value on keyToPage */
            keyToPage.put(key, insertionPageId);
        }

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Updated key " + key + " from page " + prevPageId + " to page " + insertionPageId
                    + " on table " + table.tablespace + "." + table.name);
        }

        if (indexes != null) {

            /* If there are indexes e have already forced a page load and previous record has been loaded */
            DataAccessor prevValues = previous.getDataAccessor(table);
            DataAccessor newValues = record.getDataAccessor(table);
            for (AbstractIndexManager index : indexes.values()) {
                Index indexDef = index.getIndex();
                String[] indexColumnNames = index.getColumnNames();
                Bytes indexKeyRemoved = RecordSerializer.serializeIndexKey(prevValues, indexDef, indexColumnNames);
                Bytes indexKeyAdded = RecordSerializer.serializeIndexKey(newValues, indexDef, indexColumnNames);
                index.recordUpdated(key, indexKeyRemoved, indexKeyAdded);
            }
        }
    }

    @Override
    public void dropTableData() throws DataStorageManagerException {
        dataStorageManager.dropTable(tableSpaceUUID, table.uuid);
        keyToPage.dropData();
        final Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            for (AbstractIndexManager indexManager : indexes.values()) {
                indexManager.dropIndexData();
            }
        }
        unloadAllPagesForTruncate();
    }

    @Override
    public void scanForIndexRebuild(Consumer<Record> records) throws DataStorageManagerException {
        LocalScanPageCache localPageCache = new LocalScanPageCache();
        Consumer<Map.Entry<Bytes, Long>> scanExecutor = (Map.Entry<Bytes, Long> entry) -> {
            Bytes key = entry.getKey();
            LockHandle lock = lockForRead(key, null);
            try {
                Long pageId = entry.getValue();
                if (pageId != null) {
                    Record record = fetchRecord(key, pageId, localPageCache);
                    if (record != null) {
                        records.accept(record);
                    }
                }
            } catch (DataStorageManagerException | StatementExecutionException error) {
                throw new RuntimeException(error);
            } finally {
                locksManager.releaseReadLock(lock);
            }
        };
        try {
            Stream<Map.Entry<Bytes, Long>> scanner = keyToPage.scanner(null, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), tableContext, null);
            scanner.forEach(scanExecutor);
        } catch (StatementExecutionException impossible) {
            throw new DataStorageManagerException(impossible);
        }

    }

    @Override
    public void dump(LogSequenceNumber sequenceNumber, FullTableScanConsumer receiver) throws DataStorageManagerException {
        dataStorageManager.fullTableScan(tableSpaceUUID, table.uuid, sequenceNumber, receiver);
    }

    public void writeFromDump(List<Record> record) throws DataStorageManagerException {
        LOGGER.log(Level.INFO, "{0} received {1} records", new Object[]{table.name, record.size()});
        checkpointLock.asReadLock().lock();
        try {
            for (Record r : record) {
                applyInsert(r.key, r.value, false);
            }
        } finally {
            checkpointLock.asReadLock().unlock();
        }
    }

    private void rebuildNextPrimaryKeyValue() throws DataStorageManagerException {
        LOGGER.log(Level.INFO, "rebuildNextPrimaryKeyValue");
        try {
            Stream<Entry<Bytes, Long>> scanner = keyToPage.scanner(null,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    tableContext,
                    null);
            scanner.forEach((Entry<Bytes, Long> t) -> {
                Bytes key = t.getKey();
                long pk_logical_value;
                if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.INTEGER || table.getColumn(table.primaryKey[0]).type == ColumnTypes.NOTNULL_INTEGER) {
                    pk_logical_value = key.to_int();
                } else {
                    pk_logical_value = key.to_long();
                }
                nextPrimaryKeyValue.accumulateAndGet(pk_logical_value + 1, EnsureLongIncrementAccumulator.INSTANCE);
            });
            LOGGER.log(Level.INFO, "rebuildNextPrimaryKeyValue, newPkValue : " + nextPrimaryKeyValue.get());
        } catch (StatementExecutionException impossible) {
            throw new DataStorageManagerException(impossible);
        }
    }

    private void applyInsert(Bytes key, Bytes value, boolean onTransaction) throws DataStorageManagerException {
        // don't want to keep strong references to shared buffers in the keyToPages
        key = key.nonShared();

        if (table.auto_increment) {
            // the next auto_increment value MUST be greater than every other explict value
            long pk_logical_value;
            if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.INTEGER || table.getColumn(table.primaryKey[0]).type == ColumnTypes.NOTNULL_INTEGER) {
                pk_logical_value = key.to_int();
            } else {
                pk_logical_value = key.to_long();
            }
            nextPrimaryKeyValue.accumulateAndGet(pk_logical_value + 1, EnsureLongIncrementAccumulator.INSTANCE);
        }

        /*
         * New record to be added, it will always added if there aren't DataStorageManagerException thus is
         * simpler to create the record now
         */
        final Record record = new Record(key, value);

        /* Do real insertion */
        Long insertionPageId = currentDirtyRecordsPage.get();

        while (true) {
            final DataPage newPage = newPages.get(insertionPageId);

            if (newPage != null) {
                pageReplacementPolicy.pageHit(newPage);

                /* The temporary memory page could have been unloaded and loaded again in meantime */
                if (!newPage.immutable) {
                    /* Mutable page, need to check if still modifiable or already unloaded */
                    final Lock lock = newPage.pageLock.readLock();
                    lock.lock();
                    try {
                        if (newPage.writable) {
                            /* We can try to modify the page directly */
                            if (newPage.put(record)) {
                                break;
                            }
                        }
                    } finally {
                        lock.unlock();
                    }

                }
            }

            /* Try allocate a new page if no already done */
            insertionPageId = allocateLivePage(insertionPageId);
        }

        /* Insert  the value on keyToPage */
        if (!keyToPage.put(key, insertionPageId, null)) {
            throw new IllegalStateException("corrupted transaction log: key " + key + " is already present in table "
                            + table.tablespace + "." + table.name);
        }

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Inserted key " + key + " into page " + insertionPageId + " into table "
                    + table.tablespace + "." + table.name);
        }

        final Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            /* Standard insert */
            DataAccessor values = record.getDataAccessor(table);
            for (AbstractIndexManager index : indexes.values()) {
                Bytes indexKey = RecordSerializer.serializeIndexKey(values, index.getIndex(), index.getColumnNames());
                index.recordInserted(key, indexKey);
            }
        }

    }

    @Override
    public void flush() throws DataStorageManagerException {
        TableCheckpoint checkpoint = checkpoint(false);
        if (checkpoint != null) {
            for (PostCheckpointAction action : checkpoint.actions) {
                action.run();
            }
        }
    }

    @Override
    public void close() {

        closed = true;

        // unload all pages
        final List<DataPage> unload = pages.values().stream()
                .collect(Collectors.toList());
        pageReplacementPolicy.remove(unload);

        // unload keyToPage
        dataStorageManager.releaseKeyToPageMap(tableSpaceUUID, table.uuid, keyToPage);

    }

    private CompletableFuture<StatementExecutionResult> executeGetAsync(
            GetStatement get, Transaction transaction,
            StatementEvaluationContext context
    ) {
        Bytes key;
        try {
            key = Bytes.from_nullable_array(get.getKey().computeNewValue(null, context, tableContext));
        } catch (StatementExecutionException validationError) {
            return Futures.exception(validationError);
        }
        Predicate predicate = get.getPredicate();
        boolean requireLock = get.isRequireLock();
        boolean useWriteLock = requireLock && context.isForceAcquireWriteLock();
        long transactionId = transaction != null ? transaction.transactionId : 0;
        LockHandle lock = (transaction != null || requireLock) ? (useWriteLock ? lockForWrite(key, transaction) : lockForRead(key, transaction)) : null;
        CompletableFuture<StatementExecutionResult> res = null;
        try {
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    res = CompletableFuture.completedFuture(GetResult.NOT_FOUND(transactionId));
                } else {
                    Record loadedInTransaction = transaction.recordUpdated(table.name, key);
                    if (loadedInTransaction != null) {
                        if (predicate != null && !predicate.evaluate(loadedInTransaction, context)) {
                            res = CompletableFuture.completedFuture(GetResult.NOT_FOUND(transactionId));
                        } else {
                            res = CompletableFuture.completedFuture(new GetResult(transactionId, loadedInTransaction, table));
                        }
                    } else {
                        loadedInTransaction = transaction.recordInserted(table.name, key);
                        if (loadedInTransaction != null) {
                            if (predicate != null && !predicate.evaluate(loadedInTransaction, context)) {
                                res = CompletableFuture.completedFuture(GetResult.NOT_FOUND(transactionId));
                            } else {
                                res = CompletableFuture.completedFuture(new GetResult(transactionId, loadedInTransaction, table));
                            }
                        }
                    }
                }
            }
            if (res == null) {
                Long pageId = keyToPage.get(key);
                if (pageId == null) {
                    res = CompletableFuture.completedFuture(GetResult.NOT_FOUND(transactionId));
                } else {
                    Record loaded = fetchRecord(key, pageId, null);
                    if (loaded == null || (predicate != null && !predicate.evaluate(loaded, context))) {
                        res = CompletableFuture.completedFuture(GetResult.NOT_FOUND(transactionId));
                    } else {
                        res = CompletableFuture.completedFuture(new GetResult(transactionId, loaded, table));
                    }
                }
            }
            if (lock != null) {
                if (transaction == null) {
                    res.whenComplete((r, e) -> {
                        locksManager.releaseReadLock(lock);
                    });
                } else if (!context.isForceRetainReadLock() && !lock.write) {
                    transaction.releaseLockOnKey(table.name, key, locksManager);
                }
            }
            return res;
        } catch (HerdDBInternalException err) {
            return Futures.exception(err);
        }
    }

    /**
     * Just read a page from {@link DataStorageManager} and return it as an immutable page.
     *
     * @param pageId id of page to be loaded
     * @return loaded page
     * @throws DataStorageManagerException if requested page cannot be read
     */
    private DataPage temporaryLoadPageToMemory(Long pageId) throws DataStorageManagerException {

        long start = System.currentTimeMillis();

        maxCurrentPagesLoads.acquireUninterruptibly();

        long ioStart = System.currentTimeMillis();

        final List<Record> page;
        try {
            page = dataStorageManager.readPage(tableSpaceUUID, table.uuid, pageId);
        } catch (DataPageDoesNotExistException e) {
            return null;
        } finally {
            maxCurrentPagesLoads.release();
        }

        long ioStop = System.currentTimeMillis();

        final DataPage result = buildImmutableDataPage(pageId, page);

        if (LOGGER.isLoggable(Level.FINE)) {
            long stop = System.currentTimeMillis();
            LOGGER.log(Level.FINE, "table {0}.{1}, temporary loaded {2} records from page {4} in {5} ms, ({6} ms read)",
                    new Object[] { table.tablespace, table.name, result.size(), pageId, (stop - start),
                            (ioStop - ioStart) });
        }

        return result;
    }

    private DataPage loadPageToMemory(Long pageId, boolean recovery) throws DataStorageManagerException {
        DataPage result = pages.get(pageId);
        if (result != null) {
            pageReplacementPolicy.pageHit(result);
            return result;
        }

        long _start = System.currentTimeMillis();
        long _ioAndLock = 0;
        BooleanHolder computed = new BooleanHolder(false);

        try {
            result = pages.computeIfAbsent(pageId, (id) -> {
                try {
                    computed.value = true;
                    List<Record> page;
                    maxCurrentPagesLoads.acquireUninterruptibly();
                    try {
                        page = dataStorageManager.readPage(tableSpaceUUID, table.uuid, pageId);
                    } finally {
                        maxCurrentPagesLoads.release();
                    }

                    loadedPagesCount.increment();

                    return buildImmutableDataPage(pageId, page);
                } catch (DataStorageManagerException err) {
                    throw new RuntimeException(err);
                }
            });
            if (computed.value) {
                _ioAndLock = System.currentTimeMillis();

                final Page.Metadata unload = pageReplacementPolicy.add(result);
                if (unload != null) {
                    unload.owner.unload(unload.pageId);
                }
            }
        } catch (RuntimeException error) {
            if (error.getCause() != null) {
                Throwable cause = error.getCause();

                if (cause instanceof DataStorageManagerException) {
                    if (cause instanceof DataPageDoesNotExistException) {
                        return null;
                    }
                    throw (DataStorageManagerException) cause;
                }
            }
            throw new DataStorageManagerException(error);
        }
        if (computed.value && LOGGER.isLoggable(Level.FINE)) {
            long _stop = System.currentTimeMillis();
            LOGGER.log(Level.FINE,
                    "table {0}.{1}, loaded {2} records from page {3} in {4} ms, ({5} ms read + plock, {6} ms unlock)",
                    new Object[] { table.tablespace, table.name, result.size(), pageId, (_stop - _start),
                            (_ioAndLock - _start), (_stop - _ioAndLock) });
        }
        return result;
    }

    private DataPage buildImmutableDataPage(long pageId, List<Record> page) {
        Map<Bytes, Record> newPageMap = new HashMap<>(page.size());
        long estimatedPageSize = 0;
        for (Record r : page) {
            newPageMap.put(r.key, r);
            estimatedPageSize += DataPage.estimateEntrySize(r);
        }
        return new DataPage(this, pageId, maxLogicalPageSize, estimatedPageSize, newPageMap, true);
    }

    @Override
    public TableCheckpoint fullCheckpoint(boolean pin) throws DataStorageManagerException {
        return checkpoint(Double.NEGATIVE_INFINITY, fillThreshold, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, pin);
    }

    @Override
    public TableCheckpoint checkpoint(boolean pin) throws DataStorageManagerException {
        return checkpoint(dirtyThreshold, fillThreshold, checkpointTargetTime, cleanupTargetTime, compactionTargetTime, pin);
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        /* Unpin secondary indexes too */
        final Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            for (AbstractIndexManager indexManager : indexes.values()) {
                // Checkpointed at the same position of current TableManager
                indexManager.unpinCheckpoint(sequenceNumber);
            }
        }

        keyToPage.unpinCheckpoint(sequenceNumber);

        dataStorageManager.unPinTableCheckpoint(tableSpaceUUID, table.uuid, sequenceNumber);
    }

    private static final class CheckpointingPage {

        public static final Comparator<CheckpointingPage> ASCENDING_ORDER = (a, b) -> Long.compare(a.weight, b.weight);
        public static final Comparator<CheckpointingPage> DESCENDING_ORDER = (a, b) -> Long.compare(b.weight, a.weight);

        private final Long pageId;
        private final long weight;
        private final boolean dirty;

        public CheckpointingPage(Long pageId, long weight, boolean dirty) {
            super();
            this.pageId = pageId;
            this.weight = weight;
            this.dirty = dirty;
        }

        @Override
        public String toString() {
            return pageId + ":" + weight;
        }

    }

    /**
     * Sums two long values caring of overflows. Assumes that both values are
     * positive!
     */
    private static long sumOverflowWise(long a, long b) {
        long total = a + b;
        return total < 0 ? Long.MAX_VALUE : total;
    }

    private static class CleanAndCompactResult {

        private final DataPage buildingPage;
        private final boolean keepFlushedPageInMemory;
        private final List<Long> flushedPages;
        private final long flushedRecords;

        public CleanAndCompactResult(DataPage buildingPage, boolean keepFlushedPageInMemory, List<Long> flushedPages, long flushedRecords) {
            super();
            this.buildingPage = buildingPage;
            this.keepFlushedPageInMemory = keepFlushedPageInMemory;
            this.flushedPages = flushedPages;
            this.flushedRecords = flushedRecords;
        }

    }

    /**
     * Fully clean a list of pages, removing dirty records and compacting multiple pages into one (or
     * more).
     * <p>
     * For page compaction will be initially used given half-full DataPage and will return a possibly
     * half-full DataPage to continue other processess (could be even the same page). Returned DataPage
     * will not be flushed, other filled pages will be flushed to disk.
     * </p>
     *
     * @param workingPages            pages to handle for cleanup and compaction
     * @param buildingPage            currently building page (possibly partially filled)
     * @param keepFlushedPageInMemory should currently (provided) building page kept on memory or
     *                                discarded?
     * @param processLimitInstant     timestamp at which blocking method invocation and return a result
     *                                (at least a page will be handled)
     * @return clean and compact result object
     */
    private CleanAndCompactResult cleanAndCompactPages(
            List<CheckpointingPage> workingPages, DataPage buildingPage,
            boolean keepFlushedPageInMemory, long processLimitInstant
    ) {

        long flushedRecords = 0;
        List<Long> flushedPages = new ArrayList<>();

        long buildingPageSize = buildingPage.getUsedMemory();

        /* Building page lock (not needed on clean small pages) */
        Lock lock = null;

        try {


            /* Rebuild dirty pages with only records to be kept */
            for (CheckpointingPage page : workingPages) {

                /* Page flushed */
                flushedPages.add(page.pageId);

                if (lock == null) {

                    /*
                     * Lock the building page if we know that checkpointing page has some dirty record and the building
                     * page wasn't already locked.
                     *
                     * We need to lock building page when the checkpoint page is dirty because we have to push the key
                     * on the PK before pushing it on buildingPage map, with no lock a concurrent reader would find the
                     * record on the PK but then fail to read from page (not already pushed on it).
                     *
                     * If the page isn't dirty we can safely write any record on the DataPage and then on the PK (every
                     * record is a good one!)
                     */

                    if (page.dirty) {
                        lock = buildingPage.pageLock.writeLock();
                        lock.lock();
                    }

                } else {

                    /*
                     * Unlock the building page if it was locked and we known that checkpointing page is clean. The lock
                     * has been held enough dirty records from previous page has been already handled and correctly
                     * pushed on both page and PK
                     */
                    if (!page.dirty) {
                        lock.unlock();
                        lock = null;
                    }

                }

                final boolean currentPageWasInMemory;
                final Collection<Record> records;

                final DataPage dataPage = pages.get(page.pageId);
                if (dataPage == null) {
                    records = dataStorageManager.readPage(tableSpaceUUID, table.uuid, page.pageId);
                    currentPageWasInMemory = false;
                    LOGGER.log(Level.FINEST, "loaded dirty page {0} for table {1}.{2} on tmp buffer: {3} records",
                            new Object[] { page.pageId, table.tablespace, table.name, records.size() });
                } else {
                    records = dataPage.getRecordsForFlush();

                    /* The page was found in memory. Currently built page should go into memory */
                    currentPageWasInMemory = true;
                }

                for (Record record : records) {

                    /* Flush the page if it would exceed max page size */
                    final long recordSize = DataPage.estimateEntrySize(record);

                    if (buildingPageSize + recordSize > maxLogicalPageSize) {

                        /* Set forcefully used memory to evaluated size */
                        buildingPage.setUsedMemory(buildingPageSize);

                        /* If there was an active lock unlock the page */
                        if (lock != null) {
                            lock.unlock();
                            lock = null;
                        }

                        flushMutablePage(buildingPage, keepFlushedPageInMemory);

                        /* Reset next rebuilt page status */
                        keepFlushedPageInMemory = false;

                        flushedRecords += buildingPage.size();
                        buildingPageSize = 0;

                        /* Get a new building page */
                        buildingPage = createMutablePage(nextPageId++, buildingPage.size(), 0);

                        /* And if needed lock again the new building page */
                        if (page.dirty && lock == null) {
                            lock = buildingPage.pageLock.writeLock();
                            lock.lock();
                        }

                    }

                    /* Current rebuilt page will be kept in memory if current page was in memory */
                    keepFlushedPageInMemory |= currentPageWasInMemory;

                    Record unshared = record.nonShared();

                    if (page.dirty) {

                        /*
                         * Attempt to update PK references (if are still valid). If KeyToPage have another [newer]
                         * mapping do not update it. (Single read&update lookup). If the conditional put succedes
                         * readers will look for the record inside buildingPage
                         */
                        boolean handled = keyToPage.put(unshared.key, buildingPage.pageId, page.pageId);

                        /* Avoid the record if has been modified or deleted */
                        if (handled) {

                            /* Move the record to the new page */
                            buildingPage.putNoMemoryHandle(unshared);

                            /*
                             * Handle size externally (avoid double size check because we already have to check if the
                             * page could host the data BEFORE updating PK and putting the value into page)
                             */
                            buildingPageSize += recordSize;
                        } else {

                            /*
                             * The put failed, track this event, readers won't ever look for the record inside
                             * buildingPage
                             */
                            checkpointProcessedDirtyRecords.add(1);
                        }

                    } else {

                        /* Move the record to the new page */
                        buildingPage.putNoMemoryHandle(unshared);

                        /*
                         * Handle size externally (avoid double size check because we already have to check if the page
                         * could host the data BEFORE updating PK and putting the value into page)
                         */
                        buildingPageSize += recordSize;

                        /*
                         * If the conditional put succeedes readers will look for the record inside buildingPage,
                         * otherwise we fail the procedure, the record should be clean!!!
                         */
                        boolean handled = keyToPage.put(unshared.key, buildingPage.pageId, page.pageId);
                        if (!handled) {
                            final IllegalStateException ex = new IllegalStateException(
                                    "Data inconsistency! Found a clean page with dirty records based on PK data. "
                                            + "It could be a key to page inconsistency (broken PK) or a page metadata "
                                            + "inconsistency (failed to track dirty record on page metadata). "
                                            + "Page: " + page + ", Record: " + unshared + ", Table: " + table.tablespace
                                            + "." + table.name);
                            LOGGER.log(Level.SEVERE, ex.getMessage());
                            throw ex;
                        }

                    }

                }

                if (dataPage != null) {

                    /* Current dirty record page isn't known to page replacement policy */
                    if (currentDirtyRecordsPage.get() != dataPage.pageId) {
                        pageReplacementPolicy.remove(dataPage);
                    }

                    final DataPage removedDataPage = pages.remove(page.pageId);

                    unloadedPagesCount.increment();

                    if (removedDataPage != null && removedDataPage != dataPage) {
                        /*
                         * DataPage can be removed due to an unload request from PageReplacementPolicy and
                         * could be event reloaded again in the meantime due to a concurrent read.
                         */
                        final long start = System.nanoTime();

                        /*
                         * In the rare event that a page was unloaded and then loaded again we check that page
                         * content is the same (it should but better be on the safe side). This is a safety
                         * check and is really rare
                         */
                        final boolean deepEquals = removedDataPage.deepEquals(dataPage);

                        final long end = System.nanoTime();

                        LOGGER.log(Level.INFO, "Checked reloaded page during checkpoint deep equality in {0} ms",
                                TimeUnit.NANOSECONDS.toMillis(end - start));

                        if (!deepEquals) {
                            final IllegalStateException ex = new IllegalStateException(
                                    "Data inconsistency! Failed to remove the right page from page knowledge during "
                                            + "checkpoint. It could be an illegal concurrent write during checkpoint or "
                                            + "the reloaded page doesn't match in memory one. " + "Expected page "
                                            + dataPage + ", found page " + removedDataPage + " on table "
                                            + table.tablespace + "." + table.name);
                            LOGGER.log(Level.SEVERE, ex.getMessage());
                            throw ex;
                        }
                    }

                }

                /* Do not continue if we have used up all given time */
                if (processLimitInstant <= System.currentTimeMillis()) {
                    break;
                }
            }

        } finally {

            /* Unlock if there is a lock left open */
            if (lock != null) {
                lock.unlock();
            }
        }

        /* Set forcefully used memory to evaluated size (returns a page with aligned data) */
        buildingPage.setUsedMemory(buildingPageSize);

        return new CleanAndCompactResult(buildingPage, keepFlushedPageInMemory, flushedPages, flushedRecords);

    }

    /**
     * @param sequenceNumber
     * @param dirtyThreshold
     * @param fillThreshold
     * @param checkpointTargetTime checkpoint target max milliseconds
     * @param cleanupTargetTime    cleanup target max milliseconds
     * @param compactionTargetTime compaction target max milliseconds
     * @return
     * @throws DataStorageManagerException
     */
    private TableCheckpoint checkpoint(
            double dirtyThreshold, double fillThreshold,
            long checkpointTargetTime, long cleanupTargetTime, long compactionTargetTime, boolean pin
    ) throws DataStorageManagerException {
        LOGGER.log(Level.FINE, "tableCheckpoint dirtyThreshold: " + dirtyThreshold + ", {0}.{1} (pin: {2})", new Object[]{tableSpaceUUID, table.name, pin});
        if (createdInTransaction > 0) {
            LOGGER.log(Level.FINE, "checkpoint for table " + table.name + " skipped,"
                    + "this table is created on transaction " + createdInTransaction + " which is not committed");
            return null;
        }

        final long fillPageThreshold = (long) (fillThreshold * maxLogicalPageSize);
        final long dirtyPageThreshold = dirtyThreshold > 0 ? (long) (dirtyThreshold * maxLogicalPageSize) : -1;

        long start = System.currentTimeMillis();
        long end;
        long getlock;
        long pageAnalysis;
        long dirtyPagesFlush;
        long smallPagesFlush;
        long newPagesFlush;
        long keytopagecheckpoint;
        long indexcheckpoint;
        long tablecheckpoint;
        final List<PostCheckpointAction> actions = new ArrayList<>();

        TableCheckpoint result;

        boolean lockAcquired;
        try {
            lockAcquired = checkpointLock.asWriteLock().tryLock(CHECKPOINT_LOCK_WRITE_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException err) {
            throw new DataStorageManagerException("interrupted while waiting for checkpoint lock", err);
        }
        if (!lockAcquired) {
            throw new DataStorageManagerException("timed out while waiting for checkpoint lock, write lock " + checkpointLock.writeLock());
        }
        try {

            LogSequenceNumber sequenceNumber = log.getLastSequenceNumber();

            getlock = System.currentTimeMillis();
            checkPointRunning = true;

            final long checkpointLimitInstant = sumOverflowWise(getlock, checkpointTargetTime);

            final Map<Long, DataPageMetaData> activePages = pageSet.getActivePages();

            long flushedRecords = 0;

            List<CheckpointingPage> flushingDirtyPages = new ArrayList<>();
            List<CheckpointingPage> flushingSmallPages = new ArrayList<>();

            final Set<Long> flushedPages = new HashSet<>();
            int flushedDirtyPages = 0;
            int flushedSmallPages = 0;

            for (Entry<Long, DataPageMetaData> ref : activePages.entrySet()) {

                final Long pageId = ref.getKey();
                final DataPageMetaData metadata = ref.getValue();

                final long dirt = metadata.dirt.sum();

                /* Check dirtiness (flush here even small pages if enough dirty) */
                if (dirt > 0 && dirt >= dirtyPageThreshold) {
                    flushingDirtyPages.add(new CheckpointingPage(pageId, dirt, dirt > 0));
                    continue;
                }

                /* Check emptiness (with a really dirty check to avoid to rewrite an unfillable page) */
                if (metadata.size <= fillPageThreshold
                        && maxLogicalPageSize - metadata.avgRecordSize >= fillPageThreshold) {
                    flushingSmallPages.add(new CheckpointingPage(pageId, metadata.size, dirt > 0));
                    continue;
                }

            }

            /* Clean dirtier first */
            flushingDirtyPages.sort(CheckpointingPage.DESCENDING_ORDER);

            /* Clean smaller first */
            flushingSmallPages.sort(CheckpointingPage.ASCENDING_ORDER);

            pageAnalysis = System.currentTimeMillis();

            /* Should currently new rebuild page kept on memory or discarded? */
            boolean keepFlushedPageInMemory = false;

            /* New page actually rebuilt */
            DataPage buildingPage = createMutablePage(nextPageId++, 0, 0);


            /* **************************** */
            /* *** Dirty pages handling *** */
            /* **************************** */

            if (!flushingDirtyPages.isEmpty()) {

                final long timeLimit = Math.min(checkpointLimitInstant,
                        sumOverflowWise(pageAnalysis, cleanupTargetTime));

                /*
                 * Do not continue if we have used up all configured cleanup or checkpoint time (but still compact
                 * at least the smaller page (normally the leftover from last checkpoint)
                 */
                CleanAndCompactResult dirtyResult = cleanAndCompactPages(flushingDirtyPages, buildingPage,
                        keepFlushedPageInMemory, timeLimit);

                flushedDirtyPages = dirtyResult.flushedPages.size();
                flushedPages.addAll(dirtyResult.flushedPages);
                flushedRecords += dirtyResult.flushedRecords;
                keepFlushedPageInMemory = dirtyResult.keepFlushedPageInMemory;

                buildingPage = dirtyResult.buildingPage;

            }

            dirtyPagesFlush = System.currentTimeMillis();


            /* **************************** */
            /* *** Small pages handling *** */
            /* **************************** */

            /*
             * Small pages could be dirty pages too so we need to check every page if has been already handled
             * during dirty pages cleanup. Small pages should be a really small set (normally just last flushed
             * page), the filter is then no critical or heavy to require some optimization
             */

            /* Filter out dirty pages flushed from flushing small pages (a page could be "small" and "dirty") */
            flushingSmallPages = flushingSmallPages.stream()
                    .filter(wp -> !flushedPages.contains(wp.pageId)).collect(Collectors.toList());

            /*
             * If there is only one clean small page without additional data to add rebuilding the page make no
             * sense: is too probable to rebuild an identical page!
             */
            if (/* Just one small page */ flushingSmallPages.size() == 1
                    /* Not dirty */ && !flushingSmallPages.get(0).dirty
                    /* No spare data remaining */ && buildingPage.isEmpty()
                    /* No new data */ && !newPages.values().stream().filter(p -> !p.isEmpty()).findAny().isPresent()) {

                /* Avoid small page compaction */
                flushingSmallPages.clear();
            }

            if (!flushingSmallPages.isEmpty()) {

                final long timeLimit = Math.min(checkpointLimitInstant,
                        sumOverflowWise(dirtyPagesFlush, compactionTargetTime));

                /*
                 * Do not continue if we have used up all configured compaction or checkpoint time (but still
                 * compact at least the smaller page (normally the leftover from last checkpoint)
                 */
                CleanAndCompactResult smallResult = cleanAndCompactPages(flushingSmallPages, buildingPage,
                        keepFlushedPageInMemory, timeLimit);

                flushedSmallPages = smallResult.flushedPages.size();
                flushedPages.addAll(smallResult.flushedPages);
                flushedRecords += smallResult.flushedRecords;
                keepFlushedPageInMemory = smallResult.keepFlushedPageInMemory;

                buildingPage = smallResult.buildingPage;
            }

            smallPagesFlush = System.currentTimeMillis();


            /* ************************** */
            /* *** New pages handling *** */
            /* ************************** */

            /*
             * Retrieve the "current" new page. It can be held in memory because no writes are executed during
             * a checkpoint and thus the page cannot change (nor be flushed due to an unload because it isn't
             * known to page replacement policy)
             */
            final long lastKnownPageId = currentDirtyRecordsPage.get();

            /*
             * Flush dirty records (and remaining records from previous step).
             *
             * Any newpage remaining here is unflushed and is not set as dirty (if "dirty" were unloaded!).
             * Just write the pages as they are.
             *
             * New empty pages won't be written
             */
            long flushedNewPages = 0;
            for (DataPage dataPage : newPages.values()) {
                /* Flush every dirty page (but not the "current" dirty page if empty) */
                if (lastKnownPageId != dataPage.pageId || !dataPage.isEmpty()) {
                    flushNewPageForCheckpoint(dataPage, buildingPage);
                    ++flushedNewPages;
                    flushedRecords += dataPage.size();
                }
            }


            /* ************************************* */
            /* *** Remaining spare data handling *** */
            /* ************************************* */

            /*
             * Flush remaining records.
             *
             * To keep or not flushed page in memory is a "best guess" here: we don't known if records that
             * needed to be kept in memory were already be flushed during newPage filling (see
             * flushNewPageForCheckpoint). So we still use keepFlushedPageInMemory (possibily true) even if
             * remaining records came from an old unused page.
             */
            if (!buildingPage.isEmpty()) {

                flushMutablePage(buildingPage, keepFlushedPageInMemory);

            } else {

                /* Remove unused empty building page from memory */
                pages.remove(buildingPage.pageId);
            }

            /*
             * Never Never Never revert unused nextPageId! Even if we didn't used booked nextPageId is better to
             * throw it away, reverting generated id could be "strange" for now but simply wrong in the future
             * (if checkpoint will permit concurrent page creation for example..)
             */

            newPagesFlush = System.currentTimeMillis();

            if (flushedDirtyPages > 0 || flushedSmallPages > 0 || flushedNewPages > 0 || flushedRecords > 0) {
                LOGGER.log(Level.INFO, "checkpoint {0}, logpos {1}, flushed: {2} dirty pages, {3} small pages, {4} new pages, {5} records",
                    new Object[]{table.name, sequenceNumber, flushedDirtyPages, flushedSmallPages, flushedNewPages, flushedRecords});

            }

            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, "checkpoint {0}, logpos {1}, flushed pages: {2}",
                        new Object[]{table.name, sequenceNumber, flushedPages.toString()});
            }

            /* Checkpoint the key to page too */
            actions.addAll(keyToPage.checkpoint(sequenceNumber, pin));
            keytopagecheckpoint = System.currentTimeMillis();

            /* Checkpoint secondary indexes too */
            final Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
            if (indexes != null) {
                for (AbstractIndexManager indexManager : indexes.values()) {
                    // Checkpoint at the same position of current TableManager
                    actions.addAll(indexManager.checkpoint(sequenceNumber, pin));
                }
            }

            indexcheckpoint = System.currentTimeMillis();

            pageSet.checkpointDone(flushedPages);

            TableStatus tableStatus = new TableStatus(table.name, sequenceNumber,
                    Bytes.longToByteArray(nextPrimaryKeyValue.get()), nextPageId,
                    pageSet.getActivePages());

            actions.addAll(dataStorageManager.tableCheckpoint(tableSpaceUUID, table.uuid, tableStatus, pin));
            tablecheckpoint = System.currentTimeMillis();

            /*
             * Can happen when at checkpoint start all pages are set as dirty or immutable (immutable or
             * unloaded) due do a deletion: all pages will be removed and no page will remain alive.
             */
            if (newPages.isEmpty()) {
                /* Allocate live handles the correct policy load/unload of last dirty page */
                allocateLivePage(lastKnownPageId);
            }

            checkPointRunning = false;

            result = new TableCheckpoint(table.name, sequenceNumber, actions);

            end = System.currentTimeMillis();
            if (flushedRecords > 0) {
                LOGGER.log(Level.INFO, "checkpoint {0} finished, logpos {1}, {2} active pages, {3} dirty pages, "
                        + "flushed {4} records, total time {5} ms",
                        new Object[]{table.name, sequenceNumber, pageSet.getActivePagesCount(),
                            pageSet.getDirtyPagesCount(), flushedRecords, Long.toString(end - start)});
            }

            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, "checkpoint {0} finished, logpos {1}, pageSet: {2}",
                        new Object[]{table.name, sequenceNumber, pageSet.toString()});
            }

        } finally {
            checkpointLock.asWriteLock().unlock();
        }

        long delta = end - start;
        if (delta > 1000) {

            long delta_lock = getlock - start;
            long delta_pageAnalysis = pageAnalysis - getlock;
            long delta_dirtyPagesFlush = dirtyPagesFlush - pageAnalysis;
            long delta_smallPagesFlush = smallPagesFlush - dirtyPagesFlush;
            long delta_newPagesFlush = newPagesFlush - smallPagesFlush;
            long delta_keytopagecheckpoint = keytopagecheckpoint - newPagesFlush;
            long delta_indexcheckpoint = indexcheckpoint - keytopagecheckpoint;
            long delta_tablecheckpoint = tablecheckpoint - indexcheckpoint;
            long delta_unload = end - tablecheckpoint;

            LOGGER.log(Level.INFO, "long checkpoint for {0}, time {1}", new Object[]{table.name,
                    delta + " ms (" + delta_lock
                            + "+" + delta_pageAnalysis
                            + "+" + delta_dirtyPagesFlush
                            + "+" + delta_smallPagesFlush
                            + "+" + delta_newPagesFlush
                            + "+" + delta_keytopagecheckpoint
                            + "+" + delta_indexcheckpoint
                            + "+" + delta_tablecheckpoint
                            + "+" + delta_unload + ")"});
        }

        return result;
    }

    @Override
    public DataScanner scan(
            ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {

        forWrite = forWrite || context.isForceAcquireWriteLock();

        TupleComparator comparator = statement.getComparator();
        if (!ENABLE_STREAMING_DATA_SCANNER || (comparator != null
                && this.stats.getTablesize() > HUGE_TABLE_SIZE_FORCE_MATERIALIZED_RESULTSET)) {
            boolean sortedByClusteredIndex = comparator != null
                    && comparator.isOnlyPrimaryKeyAndAscending()
                    && keyToPageSortedAscending;
            if (!sortedByClusteredIndex) {
                return scanNoStream(statement, context, transaction, lockRequired, forWrite);
            }
        }
        return scanWithStream(statement, context, transaction, lockRequired, forWrite);
    }

    private DataScanner scanNoStream(
            ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        if (transaction != null) {
            transaction.increaseRefcount();
        }
        try {
            boolean sorted = statement.getComparator() != null;
            boolean sortedByClusteredIndex = statement.getComparator() != null
                    && statement.getComparator().isOnlyPrimaryKeyAndAscending()
                    && keyToPageSortedAscending;
            final Projection projection = statement.getProjection();
            boolean applyProjectionDuringScan = !sorted && projection != null;
            MaterializedRecordSet recordSet;
            if (applyProjectionDuringScan) {
                recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                        .createRecordSet(projection.getFieldNames(), projection.getColumns());
            } else {
                recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                        .createRecordSet(table.columnNames, table.columns);
            }
            ScanLimits limits = statement.getLimits();
            int maxRows = limits == null ? 0 : limits.computeMaxRows(context);
            int offset = limits == null ? 0 : limits.computeOffset(context);
            boolean sortDone = false;
            if (maxRows > 0) {
                if (sortedByClusteredIndex) {
                    // leverage the sorted nature of the clustered primary key index
                    AtomicInteger remaining = new AtomicInteger(maxRows);
                    if (offset > 0) {
                        remaining.getAndAdd(offset);
                    }
                    accessTableData(statement, context, new ScanResultOperation() {

                        private boolean inTransactionData;

                        @Override
                        public void beginNewRecordsInTransactionBlock() {
                            inTransactionData = true;
                        }

                        @Override
                        public void accept(Record record, LockHandle lockHandle) throws StatementExecutionException {
                            try {
                                if (applyProjectionDuringScan) {
                                    DataAccessor tuple = projection.map(record.getDataAccessor(table), context);
                                    recordSet.add(tuple);
                                } else {
                                    recordSet.add(record.getDataAccessor(table));
                                }
                                if (!inTransactionData) {
                                    // we have scanned the table and kept top K record already sorted by the PK
                                    // we can exit now from the loop on the primary key
                                    // we have to keep all data from the transaction buffer, because it is not sorted
                                    // in the same order as the clustered index
                                    if (remaining.decrementAndGet() == 0) {
                                        // we want to receive transaction data uncommitted records too
                                        throw new ExitLoop(true);
                                    }
                                }
                            } finally {
                                locksManager.releaseLock(lockHandle);
                            }

                        }
                    }, transaction, lockRequired, forWrite);
                    // we have to sort data any way, because accessTableData will return partially sorted data
                    sortDone = transaction == null;
                } else if (sorted) {
                    InStreamTupleSorter sorter = new InStreamTupleSorter(offset + maxRows, statement.getComparator());
                    accessTableData(statement, context, new ScanResultOperation() {
                        @Override
                        public void accept(Record record, LockHandle lockHandle) throws StatementExecutionException {
                            try {
                                if (applyProjectionDuringScan) {
                                    DataAccessor tuple = projection.map(record.getDataAccessor(table), context);
                                    sorter.collect(tuple);
                                } else {
                                    sorter.collect(record.getDataAccessor(table));
                                }
                            } finally {
                                locksManager.releaseLock(lockHandle);
                            }
                        }
                    }, transaction, lockRequired, forWrite);
                    sorter.flushToRecordSet(recordSet);
                    sortDone = true;
                } else {
                    // if no sort is present the limits can be applying during the scan and perform an early exit
                    AtomicInteger remaining = new AtomicInteger(maxRows);

                    if (offset > 0) {
                        remaining.getAndAdd(offset);
                    }
                    accessTableData(statement, context, new ScanResultOperation() {
                        @Override
                        public void accept(Record record, LockHandle lockHandle) throws StatementExecutionException {
                            try {
                                if (applyProjectionDuringScan) {
                                    DataAccessor tuple = projection.map(record.getDataAccessor(table), context);
                                    recordSet.add(tuple);
                                } else {
                                    recordSet.add(record.getDataAccessor(table));
                                }
                                if (remaining.decrementAndGet() == 0) {
                                    throw new ExitLoop(false);
                                }
                            } finally {
                                locksManager.releaseLock(lockHandle);
                            }
                        }
                    }, transaction, lockRequired, forWrite);
                }
            } else {
                accessTableData(statement, context, new ScanResultOperation() {
                    @Override
                    public void accept(Record record, LockHandle lockHandle) throws StatementExecutionException {
                        try {
                            if (applyProjectionDuringScan) {
                                DataAccessor tuple = projection.map(record.getDataAccessor(table), context);
                                recordSet.add(tuple);
                            } else {
                                recordSet.add(record.getDataAccessor(table));
                            }
                        } finally {
                            locksManager.releaseLock(lockHandle);
                        }
                    }
                }, transaction, lockRequired, forWrite);
            }

            recordSet.writeFinished();
            if (!sortDone) {
                recordSet.sort(statement.getComparator());
            }
            recordSet.applyLimits(statement.getLimits(), context);
            if (!applyProjectionDuringScan) {
                recordSet.applyProjection(statement.getProjection(), context);
            }
            return new SimpleDataScanner(transaction, recordSet);
        } finally {
            if (transaction != null) {
                transaction.decreaseRefCount();
            }
        }
    }

    private DataScanner scanWithStream(
            ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        if (transaction != null) {
            transaction.increaseRefcount();
        }
        try {
            final TupleComparator comparator = statement.getComparator();
            boolean sorted = comparator != null;
            boolean sortedByClusteredIndex = comparator != null
                    && comparator.isOnlyPrimaryKeyAndAscending()
                    && keyToPageSortedAscending;
            final Projection projection = statement.getProjection();
            final boolean applyProjectionDuringScan = projection != null && !sorted;
            ScanLimits limits = statement.getLimits();
            int maxRows = limits == null ? 0 : limits.computeMaxRows(context);
            int offset = limits == null ? 0 : limits.computeOffset(context);
            Stream<DataAccessor> result;
            Function<Record, DataAccessor> mapper = (Record record) -> {
                DataAccessor tuple;
                if (applyProjectionDuringScan) {
                    tuple = projection.map(record.getDataAccessor(table), context);
                } else {
                    tuple = record.getDataAccessor(table);
                }
                return tuple;
            };

            Stream<Record> recordsFromTransactionSorted =
                    streamTransactionData(transaction, statement.getPredicate(), context);
            Stream<DataAccessor> fromTransactionSorted = recordsFromTransactionSorted != null
                    ? recordsFromTransactionSorted.map(mapper) : null;
            if (fromTransactionSorted != null && comparator != null) {
                fromTransactionSorted = fromTransactionSorted.sorted(comparator);
            }

            Stream<DataAccessor> tableData = streamTableData(statement, context, transaction, lockRequired, forWrite)
                    .map(mapper);
            if (maxRows > 0) {
                if (sortedByClusteredIndex) {
                    // already sorted if needed
                    if (fromTransactionSorted != null) {
                        // already sorted from index
                        tableData = tableData.limit(maxRows + offset);

                        fromTransactionSorted = fromTransactionSorted.limit(maxRows + offset);

                        // we need to re-sort after merging the data
                        result = Stream.concat(fromTransactionSorted, tableData)
                                .sorted(comparator);
                    } else {
                        // already sorted from index
                        tableData = tableData.limit(maxRows + offset);

                        // no need to re-sort
                        result = tableData;
                    }
                } else if (sorted) {
                    // need to sort
                    tableData = tableData.sorted(comparator);
                    // already sorted if needed
                    if (fromTransactionSorted != null) {
                        tableData = tableData.limit(maxRows + offset);

                        fromTransactionSorted = fromTransactionSorted.limit(maxRows + offset);
                        // we need to re-sort after merging the data
                        result = Stream.concat(fromTransactionSorted, tableData)
                                .sorted(comparator);
                    } else {
                        tableData = tableData.limit(maxRows + offset);
                        // no need to sort again
                        result = tableData;
                    }
                } else if (fromTransactionSorted == null) {
                    result = tableData;
                } else {
                    result = Stream.concat(fromTransactionSorted, tableData);
                }
            } else {
                if (sortedByClusteredIndex) {
                    // already sorted from index
                    if (fromTransactionSorted != null) {
                        tableData = tableData.sorted(comparator);
                        // fromTransactionSorted is already sorted
                        // we need to re-sort
                        result = Stream.concat(fromTransactionSorted, tableData)
                                .sorted(comparator);
                    } else {
                        result = tableData;
                    }
                } else if (sorted) {
                    // tableData already sorted from index
                    // fromTransactionSorted is already sorted
                    // we need to re-sort
                    if (fromTransactionSorted != null) {
                        result = Stream.concat(fromTransactionSorted, tableData)
                                .sorted(comparator);
                    } else {
                        result = tableData
                                .sorted(comparator);
                    }
                } else if (fromTransactionSorted != null) {
                    // no need to sort
                    result = Stream.concat(fromTransactionSorted, tableData);
                } else {
                    result = tableData;
                }
            }
            if (offset > 0) {
                result = result.skip(offset);
            }
            if (maxRows > 0) {
                result = result.limit(maxRows);
            }
            if (!applyProjectionDuringScan && projection != null) {
                result = result.map(r -> projection.map(r, context));
            }
            String[] fieldNames;
            Column[] columns;
            if (projection != null) {
                fieldNames = projection.getFieldNames();
                columns = projection.getColumns();
            } else {
                fieldNames = table.columnNames;
                columns = table.columns;
            }
            return new StreamDataScanner(transaction, fieldNames, columns, result);
        } finally {
            if (transaction != null) {
                transaction.decreaseRefCount();
            }
        }
    }

    private void accessTableData(
            ScanStatement statement, StatementEvaluationContext context, ScanResultOperation consumer, Transaction transaction,
            boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        statement.validateContext(context);
        Predicate predicate = statement.getPredicate();
        long _start = System.currentTimeMillis();
        boolean acquireLock = transaction != null || forWrite || lockRequired;
        LocalScanPageCache lastPageRead = acquireLock ? null : new LocalScanPageCache();
        AtomicInteger count = new AtomicInteger();
        try {

            IndexOperation indexOperation = predicate != null ? predicate.getIndexOperation() : null;
            boolean primaryIndexSeek = indexOperation instanceof PrimaryIndexSeek;
            AbstractIndexManager useIndex = getIndexForTbleAccess(indexOperation);

            class RecordProcessor implements BatchOrderedExecutor.Executor<Entry<Bytes, Long>>,
                    Consumer<Map.Entry<Bytes, Long>> {

                @Override
                public void execute(List<Map.Entry<Bytes, Long>> batch) throws HerdDBInternalException {
                    batch.forEach((entry) -> {
                        accept(entry);
                    });
                }

                @Override
                public void accept(Entry<Bytes, Long> entry) throws DataStorageManagerException, StatementExecutionException, LogNotAvailableException {
                    if (transaction != null && count.incrementAndGet() % 1000 == 0) {
                        transaction.touch();
                    }
                    Bytes key = entry.getKey();
                    boolean already_locked = transaction != null && transaction.lookupLock(table.name, key) != null;
                    boolean record_discarded = !already_locked;
                    LockHandle lock = acquireLock ? (forWrite ? lockForWrite(key, transaction) : lockForRead(key, transaction)) : null;
//                    LOGGER.log(Level.SEVERE, "CREATED LOCK " + lock + " for " + key);
                    try {
                        if (transaction != null) {
                            if (transaction.recordDeleted(table.name, key)) {
                                // skip this record. inside current transaction it has been deleted
                                return;
                            }
                            Record record = transaction.recordUpdated(table.name, key);
                            if (record != null) {
                                // use current transaction version of the record
                                if (predicate == null || predicate.evaluate(record, context)) {
                                    // now the consumer is the owner of the lock on the record
                                    record_discarded = false;
                                    consumer.accept(record, null /* transaction holds the lock */);

                                }
                                return;
                            }
                        }
                        Long pageId = entry.getValue();
                        if (pageId != null) {
                            boolean pkFilterCompleteMatch = false;
                            if (!primaryIndexSeek && predicate != null) {
                                Predicate.PrimaryKeyMatchOutcome outcome =
                                        predicate.matchesRawPrimaryKey(key, context);
                                if (outcome == Predicate.PrimaryKeyMatchOutcome.FAILED) {
                                    return;
                                } else if (outcome == Predicate.PrimaryKeyMatchOutcome.FULL_CONDITION_VERIFIED) {
                                    pkFilterCompleteMatch = true;
                                }
                            }
                            Record record = fetchRecord(key, pageId, lastPageRead);
                            if (record != null && (pkFilterCompleteMatch || predicate == null || predicate.evaluate(record, context))) {
                                // now the consumer is the owner of the lock on the record
                                record_discarded = false;
                                consumer.accept(record, transaction == null ? lock : null);
                            }
                        }
                    } finally {
                        // release the lock on the key if it did not match scan criteria
                        if (record_discarded) {
                            if (transaction == null) {
                                locksManager.releaseLock(lock);
                            } else if (!already_locked) {
                                transaction.releaseLockOnKey(table.name, key, locksManager);
                            }
                        }
                    }
                }
            }

            RecordProcessor scanExecutor = new RecordProcessor();
            boolean exit = false;
            try {
                if (primaryIndexSeek) {
                    // we are expecting at most one record, no need for BatchOrderedExecutor
                    // this is the most common case for UPDATE-BY-PK and SELECT-BY-PK
                    // no need to craete and use Streams
                    PrimaryIndexSeek seek = (PrimaryIndexSeek) indexOperation;
                    Bytes value = Bytes.from_array(seek.value.computeNewValue(null, context, tableContext));
                    Long page = keyToPage.get(value);
                    if (page != null) {
                        Map.Entry<Bytes, Long> singleEntry =
                                new AbstractMap.SimpleImmutableEntry<>(value, page);
                        scanExecutor.accept(singleEntry);
                    }
                } else {
                    Stream<Map.Entry<Bytes, Long>> scanner = keyToPage.scanner(indexOperation, context, tableContext, useIndex);
                    BatchOrderedExecutor<Map.Entry<Bytes, Long>> executor = new BatchOrderedExecutor<>(SORTED_PAGE_ACCESS_WINDOW_SIZE,
                            scanExecutor, SORTED_PAGE_ACCESS_COMPARATOR);
                    scanner.forEach(executor);
                    executor.finish();
                }
            } catch (ExitLoop exitLoop) {
                exit = !exitLoop.continueWithTransactionData;
                if (LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.log(Level.FINEST, "exit loop during scan {0}, started at {1}: {2}", new Object[]{statement, new java.sql.Timestamp(_start), exitLoop.toString()});
                }
            } catch (final HerdDBInternalException error) {
                LOGGER.log(Level.SEVERE, "error during scan", error);
                if (error.getCause() instanceof StatementExecutionException) {
                    throw (StatementExecutionException) error.getCause();
                } else if (error.getCause() instanceof DataStorageManagerException) {
                    throw (DataStorageManagerException) error.getCause();
                } else if (error instanceof StatementExecutionException) {
                    throw error;
                } else if (error instanceof DataStorageManagerException) {
                    throw error;
                } else {
                    throw new StatementExecutionException(error);
                }
            }

            if (!exit && transaction != null) {
                consumer.beginNewRecordsInTransactionBlock();
                Collection<Record> newRecordsForTable = transaction.getNewRecordsForTable(table.name);
                if (newRecordsForTable != null) {
                    newRecordsForTable.forEach(record -> {
                        if (!transaction.recordDeleted(table.name, record.key)
                                && (predicate == null || predicate.evaluate(record, context))) {
                            consumer.accept(record, null);
                        }
                    });
                }
            }

        } catch (ExitLoop exitLoop) {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(Level.FINEST, "exit loop during scan {0}, started at {1}: {2}", new Object[]{statement, new java.sql.Timestamp(_start), exitLoop.toString()});
            }
        } catch (StatementExecutionException err) {
            LOGGER.log(Level.SEVERE, "error during scan {0}, started at {1}: {2}", new Object[]{statement, new java.sql.Timestamp(_start), err.toString()});
            throw err;
        } catch (HerdDBInternalException err) {
            LOGGER.log(Level.SEVERE, "error during scan {0}, started at {1}: {2}", new Object[]{statement, new java.sql.Timestamp(_start), err.toString()});
            throw new StatementExecutionException(err);
        }
    }

    private Stream<Record> streamTableData(
            ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction,
            boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        statement.validateContext(context);
        Predicate predicate = statement.getPredicate();
        boolean acquireLock = transaction != null || forWrite || lockRequired;
        LocalScanPageCache lastPageRead = acquireLock ? null : new LocalScanPageCache();
        IndexOperation indexOperation = predicate != null ? predicate.getIndexOperation() : null;
        boolean primaryIndexSeek = indexOperation instanceof PrimaryIndexSeek;
        AbstractIndexManager useIndex = getIndexForTbleAccess(indexOperation);
        Stream<Map.Entry<Bytes, Long>> scanner = keyToPage.scanner(indexOperation, context, tableContext, useIndex);

        Stream<Record> resultFromTable = scanner.map(entry -> {
            return accessRecord(entry, predicate, context,
                    transaction, lastPageRead, primaryIndexSeek, forWrite, acquireLock);
        }).filter(r -> r != null);
        return resultFromTable;
    }

    /**
     * Data from new records INSERTed during current transaction
     *
     * @param transaction
     * @param predicate
     * @param context
     * @return a stream over new records, which match the given predicate, null
     * if no transaction or other simple empty cases
     */
    private Stream<Record> streamTransactionData(Transaction transaction, Predicate predicate, StatementEvaluationContext context) {
        if (transaction != null) {
            transaction.touch();
            Collection<Record> newRecordsForTable = transaction.getNewRecordsForTable(table.name);
            if (newRecordsForTable == null || newRecordsForTable.isEmpty()) {
                return null;
            }
            return newRecordsForTable
                    .stream()
                    .map(record -> {
                        if (!transaction.recordDeleted(table.name, record.key)
                                && (predicate == null || predicate.evaluate(record, context))) {
                            return record;
                        } else {
                            return null;
                        }
                    }).filter(r -> r != null);
        } else {
            return null;
        }
    }

    public Record accessRecord(
            Map.Entry<Bytes, Long> entry,
            Predicate predicate, StatementEvaluationContext context,
            Transaction transaction, LocalScanPageCache lastPageRead, boolean primaryIndexSeek,
            boolean forWrite, boolean acquireLock
    ) {

        Bytes key = entry.getKey();
        boolean keep_lock = false;
        boolean already_locked = transaction != null && transaction.lookupLock(table.name, key) != null;
        LockHandle lock = acquireLock ? (forWrite ? lockForWrite(key, transaction) : lockForRead(key, transaction)) : null;
        try {
            if (transaction != null) {
                transaction.touch();
                if (transaction.recordDeleted(table.name, key)) {
                    // skip this record. inside current transaction it has been deleted
                    return null;
                }
                Record record = transaction.recordUpdated(table.name, key);
                if (record != null) {
                    // use current transaction version of the record
                    if (predicate == null || predicate.evaluate(record, context)) {
                        keep_lock = context.isForceRetainReadLock() || (lock != null && lock.write);
                        return record;
                    }
                    return null;
                }
            }
            Long pageId = entry.getValue();
            if (pageId != null) {
                boolean pkFilterCompleteMatch = false;
                if (!primaryIndexSeek && predicate != null) {
                    Predicate.PrimaryKeyMatchOutcome outcome =
                            predicate.matchesRawPrimaryKey(key, context);
                    if (outcome == Predicate.PrimaryKeyMatchOutcome.FAILED) {
                        return null;
                    } else if (outcome == Predicate.PrimaryKeyMatchOutcome.FULL_CONDITION_VERIFIED) {
                        pkFilterCompleteMatch = true;
                    }
                }
                Record record = fetchRecord(key, pageId, lastPageRead);
                if (record != null && (pkFilterCompleteMatch || predicate == null || predicate.evaluate(record, context))) {

                    keep_lock = context.isForceRetainReadLock() || (lock != null && lock.write);
                    return record;
                }
            }
            return null;
        } finally {
            // release the lock on the key if it did not match scan criteria
            if (transaction == null) {
                if (lock != null) {
                    locksManager.releaseLock(lock);
                }
            } else if (!keep_lock && !already_locked) {
                transaction.releaseLockOnKey(table.name, key, locksManager);
            }
        }
    }

    private AbstractIndexManager getIndexForTbleAccess(IndexOperation indexOperation) {
        AbstractIndexManager useIndex = null;
        if (indexOperation != null) {
            Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
            if (indexes != null) {
                useIndex = indexes.get(indexOperation.getIndexName());
                if (useIndex != null && !useIndex.isAvailable()) {
                    useIndex = null;
                }
            }
        }
        return useIndex;
    }

    @Override
    public List<Index> getAvailableIndexes() {
        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes == null) {
            return Collections.emptyList();
        }
        return indexes.values().stream().filter(AbstractIndexManager::isAvailable).map(AbstractIndexManager::getIndex)
                .collect(Collectors.toList());
    }

    @Override
    public KeyToPageIndex getKeyToPageIndex() {
        return keyToPage;
    }

    private Record fetchRecord(Bytes key, Long pageId, LocalScanPageCache localScanPageCache) throws StatementExecutionException, DataStorageManagerException {
        int maxTrials = 2;
        while (true) {
            DataPage dataPage = fetchDataPage(pageId, localScanPageCache);
            if (dataPage != null) {
                Record record = dataPage.get(key);
                if (record != null) {
                    /* Record found */
                    return record;
                } else {

                    /*
                     * If not found and current page is not immutable lock the page and check for the record again. It
                     * can happen during checkpoint that a record has been published on PK but not on datapage, we have
                     * to wait for datapage publish.
                     *
                     * This second attempt should be attempted only around checkpoint
                     */
                    if (!dataPage.immutable) {
                        final Lock lock = dataPage.pageLock.readLock();
                        lock.lock();

                        try {
                            record = dataPage.get(key);
                            if (record != null) {
                                /* Record found on second attempt */
                                return record;
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }

            Long relocatedPageId = keyToPage.get(key);
            LOGGER.log(Level.FINE, table.name + " fetchRecord " + key + " failed,"
                    + "checkPointRunning:" + checkPointRunning + " pageId:" + pageId + " relocatedPageId:" + relocatedPageId);
            if (relocatedPageId == null) {
                // deleted
                LOGGER.log(Level.FINE, "table " + table.name + ", activePages " + pageSet.getActivePages() + ", record " + key + " deleted during data access");
                return null;
            }
            pageId = relocatedPageId;
            if (maxTrials-- == 0) {
                if (dataPage != null) {
                    Collection<Bytes> keysForDebug = dataPage.getKeysForDebug(); // this may in an inconsistent state
                    throw new DataStorageManagerException("inconsistency! table " + table.name + " no record in memory for " + key + " page " + pageId + ", activePages " + pageSet.getActivePages() + ", dataPage " + dataPage + ", dataPageKeys =" + keysForDebug + " after many trials");
                } else {
                    throw new DataStorageManagerException("inconsistency! table " + table.name + " no record in memory for " + key + " page " + pageId + ", activePages " + pageSet.getActivePages() + ", dataPage = null after many trials");
                }
            }
        }
    }

    private DataPage fetchDataPage(Long pageId, LocalScanPageCache localScanPageCache) throws DataStorageManagerException {
        DataPage dataPage;
        if (localScanPageCache == null
                || !ENABLE_LOCAL_SCAN_PAGE_CACHE
                || pages.containsKey(pageId)) {
            dataPage = loadPageToMemory(pageId, false);
        } else {
            if (pageId.equals(localScanPageCache.pageId)) {
                // same page needed twice
                dataPage = localScanPageCache.value;
            } else {
                // TODO: add good heuristics and choose whether to load
                // the page in the main buffer
                dataPage = pages.get(pageId);
                if (dataPage == null) {
                    if (ThreadLocalRandom.current().nextInt(10) < 4) {
                        // 25% of pages will be loaded to main buffer
                        dataPage = loadPageToMemory(pageId, false);
                    } else {
                        // 75% of pages will be loaded only to current scan buffer
                        dataPage = temporaryLoadPageToMemory(pageId);
                        localScanPageCache.value = dataPage;
                        localScanPageCache.pageId = pageId;
                    }
                } else {
                    pageReplacementPolicy.pageHit(dataPage);
                }
            }
        }
        return dataPage;
    }

    @Override
    public TableManagerStats getStats() {
        return stats;
    }

    @Override
    public long getNextPrimaryKeyValue() {
        return nextPrimaryKeyValue.get();
    }

    @Override
    public boolean isSystemTable() {
        return false;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public void validateAlterTable(Table table, StatementEvaluationContext context) throws StatementExecutionException {
        List<String> columnsChangedFromNullToNotNull = new ArrayList<>();
        for (Column c : this.table.columns) {
            Column newColumnSpecs = table.getColumn(c.name);
            if (newColumnSpecs == null) {
                // dropped column
                LOGGER.log(Level.INFO, "Table {0}.{1} dropping column {2}", new Object[]{table.tablespace, table.name, c.name});
            } else if (newColumnSpecs.type == c.type) {
                // no data type change
            } else if (ColumnTypes.isNotNullToNullConversion(c.type, newColumnSpecs.type)) {
                LOGGER.log(Level.INFO, "Table {0}.{1} making column {2} NULLABLE", new Object[]{table.tablespace, table.name, newColumnSpecs.name});
            } else if (ColumnTypes.isNullToNotNullConversion(c.type, newColumnSpecs.type)) {
                LOGGER.log(Level.INFO, "Table {0}.{1} making column {2} NOT NULL", new Object[]{table.tablespace, table.name, newColumnSpecs.name});
                columnsChangedFromNullToNotNull.add(c.name);
            }
        }
        for (final String column : columnsChangedFromNullToNotNull) {
            LOGGER.log(Level.INFO, "Table {0}.{1} validating column {2}, check for NULL values", new Object[]{table.tablespace, table.name, column});
            ScanStatement scan = new ScanStatement(this.table.tablespace,
                    this.table, new Predicate() {
                @Override
                public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
                    return record.getDataAccessor(table).get(column) == null;
                }
            });
            // fast fail
            scan.setLimits(new ScanLimitsImpl(1, 0));
            boolean foundOneNull = false;
            try (DataScanner scanner = this.scan(scan, context, null, false, false);) {
                foundOneNull = scanner.hasNext();
            } catch (DataScannerException err) {
                throw new StatementExecutionException(err);
            }
            if (foundOneNull) {
                throw new StatementExecutionException("Found a record in table " + table.name + " that contains a NULL value for column " + column + " ALTER command is not possible");
            }
        }
        // if we are adding new FK we have to check that the FK is not violated
        if (table.foreignKeys != null) {
            List<ForeignKeyDef> newForeignKeys;
            if (this.table.foreignKeys == null) {
                newForeignKeys = Arrays.asList(table.foreignKeys);
            } else {
                Set<String> currentKfs = Stream.of(this.table.foreignKeys).map(f->f.name.toLowerCase()).collect(Collectors.toSet());
                newForeignKeys = Stream
                        .of(table.foreignKeys)
                        .filter(fk -> !currentKfs.contains(fk.name))
                        .collect(Collectors.toList());
            }
            for (ForeignKeyDef newFk : newForeignKeys) {
                validateForeignKeyConsistency(newFk, context, null);
            }
        }
    }

    @Override
    public void tableAltered(Table table, Transaction transaction) throws DDLException {
        // compute diff, if some column as been dropped we need to remove the value from each record
        List<String> droppedColumns = new ArrayList<>();
        for (Column c : this.table.columns) {
            if (table.getColumn(c.name) == null) {
                droppedColumns.add(c.name);
            }
        }
        this.table = table;
        if (!droppedColumns.isEmpty()) {
            // no lock is necessary
            pages.values().forEach(p -> {
                p.flushRecordsCache();
            });
        }

        if (this.table.auto_increment) {
            rebuildNextPrimaryKeyValue();
        }

        // clear FK caches, foreignkeys may change
        parentForeignKeyQueries.clear();
        childForeignKeyQueries.clear();
    }

    @Override
    public long getCreatedInTransaction() {
        return createdInTransaction;
    }

    private static final Comparator<Map.Entry<Bytes, Long>> SORTED_PAGE_ACCESS_COMPARATOR = (a, b) -> {
        return a.getValue().compareTo(b.getValue());
    };

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TableManager [table=").append(table).append("]");
        return builder.toString();
    }

    @Override
    public boolean isKeyToPageSortedAscending() {
        return keyToPageSortedAscending;
    }

    private static class UniqueIndexLockReference {
        final AbstractIndexManager indexManager;
        final Bytes key;
        private LockHandle lockHandle;

        public UniqueIndexLockReference(AbstractIndexManager indexManager, Bytes key) {
            this.indexManager = indexManager;
            this.key = key;
        }

    }
}
