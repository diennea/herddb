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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Index;
import herddb.model.Predicate;
import herddb.model.Projection;
import herddb.model.Record;
import herddb.model.RecordFunction;
import herddb.model.RecordTooBigException;
import herddb.model.ScanLimits;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.model.Transaction;
import herddb.model.TupleComparator;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.TruncateTableStatement;
import herddb.model.commands.UpdateStatement;
import herddb.server.ServerConfiguration;
import herddb.storage.DataPageDoesNotExistException;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.BatchOrderedExecutor;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.EnsureLongIncrementAccumulator;
import herddb.utils.Holder;
import herddb.utils.ILocalLockManager;
import herddb.utils.LegacyLocalLockManager;
import herddb.utils.LocalLockManager;
import herddb.utils.LockHandle;
import herddb.utils.SystemProperties;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

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

    private static final boolean USE_LEGACY_LOCK_MANAGER = SystemProperties
            .getBooleanSystemProperty("herddb.tablemanager.legacylocks", false);

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
    private final ILocalLockManager locksManager
            = USE_LEGACY_LOCK_MANAGER ? new LegacyLocalLockManager() : new LocalLockManager();

    /**
     * Set to {@code true} when this {@link TableManage} is fully started
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
     * Compaction (small pages) target max milliseconds
     */
    private final long compactionTargetTime;

    private final TableManagerStats stats;

    void prepareForRestore(LogSequenceNumber dumpLogSequenceNumber) {
        LOGGER.log(Level.SEVERE, "Table " + table.name + ", receiving dump,"
                + "done at external logPosition " + dumpLogSequenceNumber);
        this.dumpLogSequenceNumber = dumpLogSequenceNumber;
    }

    void restoreFinished() {
        dumpLogSequenceNumber = null;
        LOGGER.log(Level.SEVERE, "Table " + table.name + ", received dump");
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

    TableManager(Table table, CommitLog log, MemoryManager memoryManager,
            DataStorageManager dataStorageManager, TableSpaceManager tableSpaceManager, String tableSpaceUUID,
            long createdInTransaction) throws DataStorageManagerException {
        this.stats = new TableManagerStatsImpl();

        this.log = log;

        this.table = table;
        this.tableSpaceManager = tableSpaceManager;

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

        long compactionTargetTime = tableSpaceManager.getDbmanager().getServerConfiguration().getLong(
                ServerConfiguration.PROPERTY_COMPACTION_DURATION,
                ServerConfiguration.PROPERTY_COMPACTION_DURATION_DEFAULT);

        this.compactionTargetTime = compactionTargetTime < 0 ? Long.MAX_VALUE : compactionTargetTime;
    }

    private TableContext buildTableContext() {
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
        } else if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.INTEGER) {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    return Bytes.from_int((int) nextPrimaryKeyValue.getAndIncrement()).data;
                }

                @Override
                public Table getTable() {
                    return table;
                }
            };
        } else if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.LONG) {
            tableContext = new TableContext() {
                @Override
                public byte[] computeNewPrimaryKeyValue() {
                    return Bytes.from_long((int) nextPrimaryKeyValue.getAndIncrement()).data;
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
    public void start() throws DataStorageManagerException {

        Map<Long, DataPageMetaData> activePagesAtBoot = new HashMap<>();

        bootSequenceNumber = LogSequenceNumber.START_OF_TIME;
        boolean requireLoadAtStartup = keyToPage.requireLoadAtStartup();

        if (requireLoadAtStartup) {
            // non persistent primary key index, we need a full table scan
            LOGGER.log(Level.SEVERE, "loading in memory all the keys for table {0}", new Object[]{table.name});
            dataStorageManager.fullTableScan(tableSpaceUUID, table.uuid,
                    new FullTableScanConsumer() {

                Long currentPage;

                @Override
                public void acceptTableStatus(TableStatus tableStatus) {
                    LOGGER.log(Level.SEVERE, "recovery table at " + tableStatus.sequenceNumber);
                    nextPrimaryKeyValue.set(Bytes.toLong(tableStatus.nextPrimaryKeyValue, 0));
                    nextPageId = tableStatus.nextPageId;
                    bootSequenceNumber = tableStatus.sequenceNumber;
                    activePagesAtBoot.putAll(tableStatus.activePages);
                }

                @Override
                public void startPage(long pageId) {
                    currentPage = pageId;
                }

                @Override
                public void acceptRecord(Record record) {
                    if (currentPage < 0) {
                        throw new IllegalStateException();
                    }
                    keyToPage.put(record.key, currentPage);
                }

                @Override
                public void endPage() {
                    currentPage = null;
                }

                @Override
                public void endTable() {
                }

            });
        } else {
            LOGGER.log(Level.SEVERE, "loading table {0}, uuid {1}", new Object[]{table.name, table.uuid});
            TableStatus tableStatus = dataStorageManager.getLatestTableStatus(tableSpaceUUID, table.uuid);
            LOGGER.log(Level.SEVERE, "recovery table at " + tableStatus.sequenceNumber);
            nextPrimaryKeyValue.set(Bytes.toLong(tableStatus.nextPrimaryKeyValue, 0));
            nextPageId = tableStatus.nextPageId;
            bootSequenceNumber = tableStatus.sequenceNumber;
            activePagesAtBoot.putAll(tableStatus.activePages);
        }

        keyToPage.start(bootSequenceNumber);

        dataStorageManager.cleanupAfterBoot(tableSpaceUUID, table.uuid, activePagesAtBoot.keySet());

        pageSet.setActivePagesAtBoot(activePagesAtBoot);

        initNewPage();
        LOGGER.log(Level.SEVERE, "loaded {0} keys for table {1}, newPageId {2}, nextPrimaryKeyValue {3}, activePages {4}",
                new Object[]{keyToPage.size(), table.name, nextPageId, nextPrimaryKeyValue.get(), pageSet.getActivePages() + ""});

        started = true;
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
            try {
                GetStatement get = (GetStatement) statement;
                res = CompletableFuture.completedFuture(executeGet(get, transaction, context));
            } catch (StatementExecutionException err) {
                res = FutureUtils.exception(err);
            }
        } else if (statement instanceof DeleteStatement) {
            DeleteStatement delete = (DeleteStatement) statement;
            res = executeDeleteAsync(delete, transaction, context);
            LOGGER.log(Level.SEVERE, "COMPLETABLE FUTURE FOR DELETE is " + res);
        } else if (statement instanceof TruncateTableStatement) {
            try {
                TruncateTableStatement truncate = (TruncateTableStatement) statement;
                res = CompletableFuture.completedFuture(executeTruncate(truncate, transaction, context));
            } catch (StatementExecutionException err) {
                LOGGER.log(Level.SEVERE, "Truncate table failed", err);
                res = FutureUtils.exception(err);
            }
        } else {
            res = FutureUtils.exception(new StatementExecutionException("not implemented " + statement.getClass()));
        }

        res = res.whenComplete((r, error) -> {
            LOGGER.log(Level.SEVERE, "COMPLETED " + statement + ": " + r, error);
            checkpointLock.unlockRead(lockStamp);
        });
        if (statement instanceof TruncateTableStatement) {
            res = res.whenComplete((r, error) -> {
                LOGGER.log(Level.SEVERE, "Handling " + statement, error);
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

    /**
     * Create a new page with given data, save it and update keyToPage records
     * <p>
     * Will not place any lock, this method should be invoked at startup time or
     * during checkpoint: <b>during "stop-the-world" procedures!</b>
     * </p>
     */
    private long createImmutablePage(Map<Bytes, Record> newPage, long newPageSize) throws DataStorageManagerException {
        final Long pageId = nextPageId++;
        final DataPage dataPage = buildImmutableDataPage(pageId, newPage, newPageSize);

        LOGGER.log(Level.FINER, "createNewPage table {0}, pageId={1} with {2} records, {3} logical page size",
                new Object[]{table.name, pageId, newPage.size(), newPageSize});
        dataStorageManager.writePage(tableSpaceUUID, table.uuid, pageId, newPage.values());
        pageSet.pageCreated(pageId, dataPage);
        pages.put(pageId, dataPage);

        /* We mustn't update currentDirtyRecordsPage. This page isn't created to host live dirty data */
        final Page.Metadata unload = pageReplacementPolicy.add(dataPage);
        if (unload != null) {
            unload.owner.unload(unload.pageId);
        }

        for (Bytes key : newPage.keySet()) {
            keyToPage.put(key, pageId);
        }
        return pageId;
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

                final DataPage newPage = new DataPage(this, newId, maxLogicalPageSize, 0, new ConcurrentHashMap<>(), false);
                newPages.put(newId, newPage);
                pages.put(newId, newPage);

                /* From this moment on the page has been published */
 /* The lock is needed to block other threads up to this point */
                currentDirtyRecordsPage.set(newId);

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
     * Create a new page and set it as the target page for dirty records.
     * <p>
     * Will not place any lock, this method should be invoked at startup time:
     * <b>during "stop-the-world" procedures!</b>
     * </p>
     */
    private void initNewPage() {

        final Long newId = nextPageId++;
        final DataPage newPage = new DataPage(this, newId, maxLogicalPageSize, 0, new ConcurrentHashMap<>(), false);

        if (!newPages.isEmpty()) {
            throw new IllegalStateException("invalid new page initialization, other new pages already exist: " + newPages.keySet());
        }
        newPages.put(newId, newPage);
        pages.put(newId, newPage);

        /* From this moment on the page has been published */
 /* The lock is needed to block other threads up to this point */
        currentDirtyRecordsPage.set(newId);
    }

    @Override
    public void unload(long pageId) {
        pages.computeIfPresent(pageId, (k, remove) -> {

            unloadedPagesCount.increment();
            LOGGER.log(Level.FINER, "table {0} removed page {1}, {2}", new Object[]{table.name, pageId, remove.getUsedMemory() / (1024 * 1024) + " MB"});

            boolean dataFlushed = false;
            if (!remove.immutable) {
                dataFlushed = flushNewPageForUnload(remove);
            }

            if (dataFlushed) {
                LOGGER.log(Level.FINER, "table {0} remove and save 'new' page {1}, {2}",
                        new Object[]{table.name, remove.pageId, remove.getUsedMemory() / (1024 * 1024) + " MB"});
            } else {
                LOGGER.log(Level.FINER, "table {0} unload page {1}, {2}",
                        new Object[]{table.name, pageId, remove.getUsedMemory() / (1024 * 1024) + " MB"});
            }

            return null;
        }
        );
    }

    /**
     * Remove the page from {@link #newPages}, set it as "unloaded" and write it
     * to disk
     * <p>
     * Add as much spare data as possible to fillup the page. If added must
     * change key to page pointers too for spare data
     * </p>
     *
     * @param page new page to flush
     * @param spareData old spare data to fit in the new page if possible
     * @return spare memory size used (and removed), {@code 0} if no spare used
     */
    private long flushNewPageForCheckpoint(DataPage page, Map<Bytes, Record> spareData) {
        final long used = flushNewPage(page, spareData);

        /*
         * 0 = no memory; -1 = already flushed, they are the same for checkpoint (but
         * checkpoint need to known how much data has been used so 0 bytes has been used
         * if the page was already flushed)
         */
        return used > 0L ? used : 0L;
    }

    /**
     * Remove the page from {@link #newPages}, set it as "unloaded" and write it
     * to disk
     *
     * @param page new page to flush
     * @return {@code true} if the page has been flushed, {@code false} if
     * already flushed by another thread
     */
    private boolean flushNewPageForUnload(DataPage page) {
        return flushNewPage(page, Collections.emptyMap()) != -1;
    }

    /**
     * Remove the page from {@link #newPages}, set it as "unloaded" and write it
     * to disk
     * <p>
     * Add as much spare data as possible to fillup the page. If added must
     * change key to page pointers too for spare data
     * </p>
     *
     * @param page new page to flush
     * @param spareData old spare data to fit in the new page if possible
     * @return spare memory size used (and removed), {@code 0} if no spare used,
     * {@code -1L} if no flush has been done because the page isn't writable
     * anymore
     */
    private long flushNewPage(DataPage page, Map<Bytes, Record> spareData) {

        if (page.immutable) {
            LOGGER.log(Level.SEVERE, "Attempt to flush an immutable page " + page.pageId + " as it was mutable");
            throw new IllegalStateException("page " + page.pageId + " is not a new page!");
        }

        page.pageLock.readLock().lock();
        try {

            if (!page.writable) {
                return -1L;
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
                LOGGER.log(Level.INFO, "Mutable page " + page.pageId + " already flushed in a concurrent thread");
                return -1L;
            }

            /*
             * NewPages removal technically could be done before current write lock but
             * doing it in lock permit to check safely if a page has been removed. Without
             * lock we can't known if no page has been removed because already removed by a
             * concurrent thread or because it wasn't present in the first place.
             */
 /* Set the new page as a fully active page */
            pageSet.pageCreated(page.pageId, page);

            /* Remove it from "new" pages */
            DataPage remove = newPages.remove(page.pageId);
            if (remove == null) {
                LOGGER.log(Level.SEVERE,
                        "Detected concurrent flush of page " + page.pageId + ", writable: " + page.writable);
                throw new IllegalStateException("page " + page.pageId + " is not a new page!");
            }

            page.writable = false;

        } finally {
            lock.unlock();
        }

        long usedMemory = page.getUsedMemory();

        /* Flag to enable spare data addition to currently flushed page */
        boolean add = true;
        final Iterator<Record> records = spareData.values().iterator();
        while (add && records.hasNext()) {
            Record record = records.next();
            add = page.put(record);
            if (add) {
                keyToPage.put(record.key, page.pageId);
                records.remove();
            }
        }

        long spareUsedMemory = page.getUsedMemory() - usedMemory;
        LOGGER.log(Level.FINER, "flushNewPage table {0}, pageId={1} with {2} records, {3} logical page size",
                new Object[]{table.name, page.pageId, page.size(), page.getUsedMemory()});
        dataStorageManager.writePage(tableSpaceUUID, table.uuid, page.pageId, page.data.values());

        return spareUsedMemory;

    }

    private LockHandle lockForWrite(Bytes key, Transaction transaction) {
        if (transaction != null) {
            LockHandle lock = transaction.lookupLock(table.name, key);
            if (lock != null) {
                if (lock.write) {
                    // transaction already locked the key for writes
                    return lock;
                } else {
                    // transaction already locked the key, but we need to upgrade the lock
                    locksManager.releaseLock(lock);
                    transaction.unregisterUpgradedLocksOnTable(table.name, lock);
                    lock = locksManager.acquireWriteLockForKey(key);
                    transaction.registerLockOnTable(this.table.name, lock);
                    return lock;
                }
            } else {
                lock = locksManager.acquireWriteLockForKey(key);
                transaction.registerLockOnTable(this.table.name, lock);
                return lock;
            }
        } else {
            return locksManager.acquireWriteLockForKey(key);
        }
    }

    private LockHandle lockForRead(Bytes key, Transaction transaction) {
        if (transaction != null) {
            LockHandle lock = transaction.lookupLock(table.name, key);
            if (lock != null) {
                // transaction already locked the key
                return lock;
            } else {
                lock = locksManager.acquireReadLockForKey(key);
                transaction.registerLockOnTable(this.table.name, lock);
                return lock;
            }
        } else {
            return locksManager.acquireReadLockForKey(key);
        }
    }

    private CompletableFuture<StatementExecutionResult> executeInsertAsync(InsertStatement insert, Transaction transaction, StatementEvaluationContext context) {
        /*
         an insert can succeed only if the row is valid and the "keys" structure  does not contain the requested key
         the insert will add the row in the 'buffer' without assigning a page to it
         locks: the insert uses global 'insert' lock on the table
         the insert will update the 'maxKey' for auto_increment primary keys
         */
        Bytes key = new Bytes(insert.getKeyFunction().computeNewValue(null, context, tableContext));
        byte[] value = insert.getValuesFunction().computeNewValue(new Record(key, null), context, tableContext);
        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            try {
                DataAccessor values = new Record(key, Bytes.from_array(value)).getDataAccessor(table);
                for (AbstractIndexManager index : indexes.values()) {
                    RecordSerializer.validatePrimaryKey(values, index.getIndex(), index.getColumnNames());
                }
            } catch (IllegalArgumentException err) {
                return FutureUtils.exception(new StatementExecutionException(err.getMessage(), err));
            }
        }

        final long size = DataPage.estimateEntrySize(key, value);
        if (size > maxLogicalPageSize) {
            throw new RecordTooBigException("New record " + key + " is to big to be inserted: size " + size + ", max size " + maxLogicalPageSize);
        }

        LockHandle lock = lockForWrite(key, transaction);
        if (transaction != null) {
            if (transaction.recordDeleted(table.name, key)) {
                // OK, INSERT on a DELETED record inside this transaction
            } else if (transaction.recordInserted(table.name, key) != null) {
                // ERROR, INSERT on a INSERTED record inside this transaction
                return FutureUtils.exception(new DuplicatePrimaryKeyException(key, "key " + key + ", decoded as " + RecordSerializer.deserializePrimaryKey(key.data, table) + ", already exists in table " + table.name + " inside transaction " + transaction.transactionId));
            } else if (keyToPage.containsKey(key)) {
                return FutureUtils.exception(new DuplicatePrimaryKeyException(key, "key " + key + ", decoded as " + RecordSerializer.deserializePrimaryKey(key.data, table) + ", already exists in table " + table.name + " during transaction " + transaction.transactionId));
            }
        } else if (keyToPage.containsKey(key)) {
            return FutureUtils.exception(new DuplicatePrimaryKeyException(key, "key " + key + ", decoded as " + RecordSerializer.deserializePrimaryKey(key.data, table) + ", already exists in table " + table.name));
        }
        LogEntry entry = LogEntryFactory.insert(table, key.data, value, transaction);
        CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.thenApplyAsync((lsn) -> {
            apply(pos, entry, false);
            return new DMLStatementExecutionResult(entry.transactionId, 1, key,
                    insert.isReturnValues() ? Bytes.from_array(value) : null);
        });
        if (transaction == null) {
            res = releaseWriteLock(res, lock);
        }
        return res;
    }

    private CompletableFuture<StatementExecutionResult> releaseWriteLock(
            CompletableFuture<StatementExecutionResult> promise, LockHandle lock) {
        return promise.whenComplete((tr, error) -> {
            locksManager.releaseWriteLock(lock);
        });
    }

    @SuppressWarnings("serial")
    static class ExitLoop extends RuntimeException {

        final boolean continueWithTransactionData;

        public ExitLoop(boolean continueWithTransactionData) {
            this.continueWithTransactionData = continueWithTransactionData;
        }

    }

    static interface ScanResultOperation {

        /**
         * This function is expected to release the lock
         *
         * @param record
         * @param lockHandle lock on the record, maybe null
         * @throws StatementExecutionException
         * @throws DataStorageManagerException
         * @throws LogNotAvailableException
         */
        public default void accept(Record record, LockHandle lockHandle) throws StatementExecutionException, DataStorageManagerException, LogNotAvailableException {
        }

        public default void beginNewRecordsInTransactionBlock() {
        }
    }

    static interface StreamResultOperationStatus {

        public default void beginNewRecordsInTransactionBlock() {
        }

        public default void endNewRecordsInTransactionBlock() {
        }
    }

    private CompletableFuture<StatementExecutionResult> executeUpdateAsync(UpdateStatement update, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException, DataStorageManagerException {
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
        List<CompletableFuture<LogSequenceNumber>> writes = new ArrayList<>();
        accessTableData(scan, context, new ScanResultOperation() {
            @Override
            public void accept(Record actual, LockHandle lockHandle) throws StatementExecutionException, LogNotAvailableException, DataStorageManagerException {
                byte[] newValue = function.computeNewValue(actual, context, tableContext);

                if (indexes != null) {
                    try {
                        DataAccessor values = new Record(actual.key, Bytes.from_array(newValue)).getDataAccessor(table);
                        for (AbstractIndexManager index : indexes.values()) {
                            RecordSerializer.validatePrimaryKey(values, index.getIndex(), index.getColumnNames());
                        }
                    } catch (IllegalArgumentException err) {
                        locksManager.releaseLock(lockHandle);
                        writes.add(FutureUtils.exception(new StatementExecutionException(err.getMessage(), err)));
                        return;
                    }
                }
                final long size = DataPage.estimateEntrySize(actual.key, newValue);
                if (size > maxLogicalPageSize) {
                    locksManager.releaseLock(lockHandle);
                    writes.add(FutureUtils.exception(new RecordTooBigException("New version of record " + actual.key
                            + " is to big to be update: new size " + size + ", actual size " + DataPage.estimateEntrySize(actual)
                            + ", max size " + maxLogicalPageSize)));
                    return;
                }

                LogEntry entry = LogEntryFactory.update(table, actual.key.data, newValue, transaction);
                CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                CompletableFuture<LogSequenceNumber> promise = pos.logSequenceNumber
                        .thenApplyAsync((lsn) -> {
                            apply(pos, entry, false);
                            return lsn;
                        });
                promise.whenComplete((lns, error) -> {
                    LOGGER.log(Level.SEVERE, "UPDATE, release lock " + lockHandle);
                    locksManager.releaseLock(lockHandle);
                });
                writes.add(promise);
                lastKey.value = actual.key;
                lastValue.value = newValue;
                updateCount.incrementAndGet();
            }
        }, transaction, true, true);

        if (writes.isEmpty()) {
            return CompletableFuture
                    .completedFuture(new DMLStatementExecutionResult(transactionId, 0, null, null));
        } else if (writes.isEmpty()) {
            return writes.get(0).thenApply((lsn) -> {
                return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
                        update.isReturnValues() ? (lastValue.value != null ? Bytes.from_array(lastValue.value) : null) : null);
            });
        } else {
            return FutureUtils
                    .collect(writes)
                    .thenApply((lsn) -> {
                        return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
                                update.isReturnValues() ? (lastValue.value != null ? Bytes.from_array(lastValue.value) : null) : null);
                    });
        }

    }

    private CompletableFuture<StatementExecutionResult> executeDeleteAsync(DeleteStatement delete, Transaction transaction, StatementEvaluationContext context) {

        AtomicInteger updateCount = new AtomicInteger();
        Holder<Bytes> lastKey = new Holder<>();
        Holder<byte[]> lastValue = new Holder<>();

        long transactionId = transaction != null ? transaction.transactionId : 0;
        Predicate predicate = delete.getPredicate();
        List<CompletableFuture<LogSequenceNumber>> writes = new ArrayList<>();
        ScanStatement scan = new ScanStatement(table.tablespace, table, predicate);
        accessTableData(scan, context, new ScanResultOperation() {
            @Override
            public void accept(Record actual, LockHandle lockHandle) throws StatementExecutionException, LogNotAvailableException, DataStorageManagerException {
                try {
                    LogEntry entry = LogEntryFactory.delete(table, actual.key.data, transaction);
                    CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                    CompletableFuture<LogSequenceNumber> promise = pos.logSequenceNumber
                            .thenApplyAsync((lsn) -> {
                                LOGGER.log(Level.SEVERE, "LOGGED " + delete + " to log");
                                apply(pos, entry, false);
                                LOGGER.log(Level.SEVERE, "APPLIED " + delete);
                                return lsn;
                            });
                    promise.whenComplete((lns, error) -> {
                        LOGGER.log(Level.SEVERE, "DELETE, release lock " + lockHandle);
                        try {
                            locksManager.releaseLock(lockHandle);
                        } catch (Throwable t) {
                            LOGGER.log(Level.SEVERE, "DELETE, release lock " + lockHandle, t);
                        }
                    });
                    writes.add(promise);
                    lastKey.value = actual.key;
                    lastValue.value = actual.value.data;
                    updateCount.incrementAndGet();

                } finally {
                    locksManager.releaseLock(lockHandle);
                }
            }
        }, transaction, true, true);
        if (writes.isEmpty()) {
            return CompletableFuture
                    .completedFuture(new DMLStatementExecutionResult(transactionId, 0, null, null));
        } else if (writes.isEmpty()) {
            return writes.get(0).thenApply((lsn) -> {
                return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
                        delete.isReturnValues() ? (lastValue.value != null ? Bytes.from_array(lastValue.value) : null) : null);
            });
        } else {
            return FutureUtils
                    .collect(writes)
                    .thenApply((lsn) -> {
                        return new DMLStatementExecutionResult(transactionId, updateCount.get(), lastKey.value,
                                delete.isReturnValues() ? (lastValue.value != null ? Bytes.from_array(lastValue.value) : null) : null);
                    });
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
            throw new DataStorageManagerException("TRUNCATE TABLE cannot be executed table " + table.name
                    + ": at least one transaction is pending on it");
        }
        Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            for (AbstractIndexManager index : indexes.values()) {
                if (!index.isAvailable()) {
                    throw new DataStorageManagerException("index " + index.getIndexName()
                            + " in not full available. Cannot TRUNCATE table " + table.name);
                }
            }
        }

        /* Do not unload the current working page not known to replacement policy */
        final long currentDirtyPageId = currentDirtyRecordsPage.get();
        final List<DataPage> unload = pages.values().stream()
                .filter(page -> page.pageId != currentDirtyPageId)
                .collect(Collectors.toList());

        pageReplacementPolicy.remove(unload);

        pageSet.truncate();

        pages.clear();
        newPages.clear();

        initNewPage();

        locksManager.clear();
        keyToPage.truncate();
        if (indexes != null) {
            for (AbstractIndexManager index : indexes.values()) {
                index.truncate();
            }
        }

    }

    @Override
    public void onTransactionCommit(Transaction transaction, boolean recovery) throws DataStorageManagerException {
        if (transaction == null) {
            throw new DataStorageManagerException("transaction cannot be null");
        }
        boolean forceFlushTableData = false;
        if (createdInTransaction > 0) {
            if (transaction.transactionId != createdInTransaction) {
                throw new DataStorageManagerException("this tableManager is available only on transaction " + createdInTransaction);
            }
            createdInTransaction = 0;
            forceFlushTableData = true;
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
            LOGGER.log(Level.SEVERE, "forcing local checkpoint, table " + table.name + " will be visible to all transactions now");
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
                Bytes key = new Bytes(entry.key);
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    if (transaction == null) {
                        throw new DataStorageManagerException("no such transaction " + entry.transactionId);
                    }
                    transaction.registerDeleteOnTable(this.table.name, key, writeResult);
                } else {
                    applyDelete(key);
                }
                break;
            }
            case LogEntryType.UPDATE: {
                Bytes key = new Bytes(entry.key);
                Bytes value = new Bytes(entry.value);
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    if (transaction == null) {
                        throw new DataStorageManagerException("no such transaction " + entry.transactionId);
                    }
                    transaction.registerRecordUpdate(this.table.name, key, value, writeResult);
                } else {
                    applyUpdate(key, value);
                }
                break;
            }
            case LogEntryType.INSERT: {
                Bytes key = new Bytes(entry.key);
                Bytes value = new Bytes(entry.value);
                if (entry.transactionId > 0) {
                    Transaction transaction = tableSpaceManager.getTransaction(entry.transactionId);
                    if (transaction == null) {
                        throw new DataStorageManagerException("no such transaction " + entry.transactionId);
                    }
                    transaction.registerInsertOnTable(table.name, key, value, writeResult);
                } else {
                    applyInsert(key, value, false);
                }
                break;
            }
            case LogEntryType.TRUNCATE_TABLE: {
                applyTruncate();
            }
            ;
            break;
            default:
                throw new IllegalArgumentException("unhandled entry type " + entry.type);
        }

    }

    private void applyDelete(Bytes key) throws DataStorageManagerException {
        /* This could be a normal or a temporary modifiable page */
        final Long pageId = keyToPage.remove(key);
        if (pageId == null) {
            throw new IllegalStateException("corrupted transaction log: key " + key + " is not present in table " + table.name);
        }

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Deleted key " + key + " from page " + pageId);
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
            previous = null;

            if (page != null) {
                pageReplacementPolicy.pageHit(page);
            }
        } else {
            /* We really need the page for update index old values */
            page = loadPageToMemory(pageId, false);
            previous = page.get(key);

            if (previous == null) {
                throw new RuntimeException("deleted record at " + key + " was not found?");
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
                Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index.getIndex(), index.getColumnNames());
                index.recordDeleted(key, indexKey);
            }
        }
    }

    private void applyUpdate(Bytes key, Bytes value) throws DataStorageManagerException {
        /*
         * New record to be updated, it will always updated if there aren't errors thus is simpler to create
         * the record now
         */
        final Record record = new Record(key, value);

        /* This could be a normal or a temporary modifiable page */
        final Long prevPageId = keyToPage.get(key);
        if (prevPageId == null) {
            throw new IllegalStateException("corrupted transaction log: key " + key + " is not present in table " + table.name);
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
            } else {
                previous = null;
            }

        } else {
            /* We really need the page for update index old values */
            prevPage = loadPageToMemory(prevPageId, false);
            previous = prevPage.get(key);

            if (previous == null) {
                throw new RuntimeException("updated record at " + key + " was not found?");
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
            LOGGER.log(Level.FINEST, "Updated key " + key + " from page " + prevPageId + " to page " + insertionPageId);
        }

        if (indexes != null) {

            /* If there are indexes e have already forced a page load and previous record has been loaded */
            DataAccessor prevValues = previous.getDataAccessor(table);
            DataAccessor newValues = record.getDataAccessor(table);
            for (AbstractIndexManager index : indexes.values()) {
                Index indexDef = index.getIndex();
                String[] indexColumnNames = index.getColumnNames();
                Bytes indexKeyRemoved = RecordSerializer.serializePrimaryKey(prevValues, indexDef, indexColumnNames);
                Bytes indexKeyAdded = RecordSerializer.serializePrimaryKey(newValues, indexDef, indexColumnNames);
                index.recordUpdated(key, indexKeyRemoved, indexKeyAdded);
            }
        }
    }

    @Override
    public void dropTableData() throws DataStorageManagerException {
        dataStorageManager.dropTable(tableSpaceUUID, table.uuid);
        keyToPage.truncate();
        final Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            for (AbstractIndexManager indexManager : indexes.values()) {
                indexManager.dropIndexData();
            }
        }
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
        LOGGER.log(Level.INFO, table.name + " received " + record.size() + " records");
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
        LOGGER.log(Level.SEVERE, "rebuildNextPrimaryKeyValue");
        try {
            Stream<Entry<Bytes, Long>> scanner = keyToPage.scanner(null,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    tableContext,
                    null);
            scanner.forEach((Entry<Bytes, Long> t) -> {
                Bytes key = t.getKey();
                long pk_logical_value;
                if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.INTEGER) {
                    pk_logical_value = key.to_int();
                } else {
                    pk_logical_value = key.to_long();
                }
                nextPrimaryKeyValue.accumulateAndGet(pk_logical_value + 1, EnsureLongIncrementAccumulator.INSTANCE);
            });
            LOGGER.log(Level.SEVERE, "rebuildNextPrimaryKeyValue, newPkValue : " + nextPrimaryKeyValue.get());
        } catch (StatementExecutionException impossible) {
            throw new DataStorageManagerException(impossible);
        }
    }

    private void applyInsert(Bytes key, Bytes value, boolean onTransaction) throws DataStorageManagerException {
        if (table.auto_increment) {
            // the next auto_increment value MUST be greater than every other explict value
            long pk_logical_value;
            if (table.getColumn(table.primaryKey[0]).type == ColumnTypes.INTEGER) {
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
        keyToPage.put(key, insertionPageId);

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "Inserted key " + key + " into page " + insertionPageId);
        }

        final Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
        if (indexes != null) {
            /* Standard insert */
            DataAccessor values = record.getDataAccessor(table);
            for (AbstractIndexManager index : indexes.values()) {
                Bytes indexKey = RecordSerializer.serializePrimaryKey(values, index.getIndex(), index.getColumnNames());
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
        dataStorageManager.releaseKeyToPageMap(tableSpaceUUID, table.uuid, keyToPage);
    }

    private StatementExecutionResult executeGet(GetStatement get, Transaction transaction,
            StatementEvaluationContext context)
            throws StatementExecutionException, DataStorageManagerException {
        Bytes key = new Bytes(get.getKey().computeNewValue(null, context, tableContext));
        Predicate predicate = get.getPredicate();
        boolean requireLock = get.isRequireLock();
        long transactionId = transaction != null ? transaction.transactionId : 0;
        LockHandle lock = (transaction != null || requireLock) ? lockForRead(key, transaction) : null;
        try {
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    return GetResult.NOT_FOUND(transactionId);
                }
                Record loadedInTransaction = transaction.recordUpdated(table.name, key);
                if (loadedInTransaction != null) {
                    if (predicate != null && !predicate.evaluate(loadedInTransaction, context)) {
                        return GetResult.NOT_FOUND(transactionId);
                    }
                    return new GetResult(transactionId, loadedInTransaction, table);
                }
                loadedInTransaction = transaction.recordInserted(table.name, key);
                if (loadedInTransaction != null) {
                    if (predicate != null && !predicate.evaluate(loadedInTransaction, context)) {
                        return GetResult.NOT_FOUND(transactionId);
                    }
                    return new GetResult(transactionId, loadedInTransaction, table);
                }
            }
            Long pageId = keyToPage.get(key);
            if (pageId == null) {
                return GetResult.NOT_FOUND(transactionId);
            }
            Record loaded = fetchRecord(key, pageId, null);
            if (loaded == null || (predicate != null && !predicate.evaluate(loaded, context))) {
                return GetResult.NOT_FOUND(transactionId);
            }
            return new GetResult(transactionId, loaded, table);

        } finally {
            if (transaction == null && lock != null) {
                locksManager.releaseReadLock(lock);
            }
        }
    }

    private DataPage temporaryLoadPageToMemory(Long pageId) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        List<Record> page;
        maxCurrentPagesLoads.acquireUninterruptibly();
        try {
            page = dataStorageManager.readPage(tableSpaceUUID, table.uuid, pageId);
        } catch (DataPageDoesNotExistException ex) {
            return null;
        } finally {
            maxCurrentPagesLoads.release();
        }
        long _io = System.currentTimeMillis();
        DataPage result = buildImmutableDataPage(pageId, page);
        if (LOGGER.isLoggable(Level.FINEST)) {
            long _stop = System.currentTimeMillis();
            LOGGER.log(Level.FINEST, "tmp table " + table.name + ","
                    + "loaded " + result.size() + " records from page " + pageId
                    + " in " + (_stop - _start) + " ms"
                    + ", (" + (_io - _start) + " ms read)");
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
        AtomicBoolean computed = new AtomicBoolean();

        try {
            result = pages.computeIfAbsent(pageId, (id) -> {
                try {
                    computed.set(true);
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
            if (computed.get()) {
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
        if (computed.get()) {
            long _stop = System.currentTimeMillis();
            LOGGER.log(Level.FINE,
                    "table " + table.name + ","
                    + "loaded " + result.size() + " records from page " + pageId
                    + " in " + (_stop - _start) + " ms"
                    + ", (" + (_ioAndLock - _start) + " ms read + plock"
                    + ", " + (_stop - _ioAndLock) + " ms unlock)");
        }
        return result;
    }

    private DataPage buildImmutableDataPage(long pageId, List<Record> page) {
        Map<Bytes, Record> newPageMap = new HashMap<>();
        long estimatedPageSize = 0;
        for (Record r : page) {
            newPageMap.put(r.key, r);
            estimatedPageSize += DataPage.estimateEntrySize(r);
        }
        return buildImmutableDataPage(pageId, newPageMap, estimatedPageSize);
    }

    private DataPage buildImmutableDataPage(long pageId, Map<Bytes, Record> page, long estimatedPageSize) {
        DataPage res = new DataPage(this, pageId, maxLogicalPageSize, estimatedPageSize, page, true);
        return res;
    }

    @Override
    public TableCheckpoint fullCheckpoint(boolean pin) throws DataStorageManagerException {
        return checkpoint(Double.NEGATIVE_INFINITY, fillThreshold, Long.MAX_VALUE, Long.MAX_VALUE, pin);
    }

    @Override
    public TableCheckpoint checkpoint(boolean pin) throws DataStorageManagerException {
        return checkpoint(dirtyThreshold, fillThreshold, checkpointTargetTime, compactionTargetTime, pin);
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

    private static final class WeightedPage {

        public static final Comparator<WeightedPage> ASCENDING_ORDER = (a, b) -> Long.compare(a.weight, b.weight);
        public static final Comparator<WeightedPage> DESCENDING_ORDER = (a, b) -> Long.compare(b.weight, a.weight);

        private final Long pageId;
        private final long weight;

        public WeightedPage(Long pageId, long weight) {
            super();
            this.pageId = pageId;
            this.weight = weight;
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
    private static final long sumOverflowWise(long a, long b) {
        long total = a + b;
        return total < 0 ? Long.MAX_VALUE : total;
    }

    /**
     *
     * @param sequenceNumber
     * @param dirtyThreshold
     * @param fillThreshold
     * @param checkpointTargetTime checkpoint target max milliseconds
     * @param compactionTargetTime compaction target max milliseconds
     * @return
     * @throws DataStorageManagerException
     */
    private TableCheckpoint checkpoint(double dirtyThreshold, double fillThreshold,
            long checkpointTargetTime, long compactionTargetTime, boolean pin) throws DataStorageManagerException {
        LOGGER.log(Level.INFO, "tableCheckpoint dirtyThreshold: " + dirtyThreshold + ", {0}.{1} (pin: {2})", new Object[]{tableSpaceUUID, table.name, pin});
        if (createdInTransaction > 0) {
            LOGGER.log(Level.SEVERE, "checkpoint for table " + table.name + " skipped,"
                    + "this table is created on transaction " + createdInTransaction + " which is not committed");
            return null;
        }

        final long fillPageThreshold = (long) (fillThreshold * maxLogicalPageSize);
        final long dirtyPageThreshold = (long) (dirtyThreshold * maxLogicalPageSize);

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

            Map<Bytes, Record> buffer = new HashMap<>();
            long bufferPageSize = 0;
            long flushedRecords = 0;

            final List<WeightedPage> flushingDirtyPages = new ArrayList<>();
            final List<WeightedPage> flushingSmallPages = new ArrayList<>();

            final List<Long> flushedPages = new ArrayList<>();
            int flushedDirtyPages = 0;
            int flushedSmallPages = 0;

            for (Entry<Long, DataPageMetaData> ref : activePages.entrySet()) {

                final Long pageId = ref.getKey();
                final DataPageMetaData metadata = ref.getValue();

                final long dirt = metadata.dirt.sum();

                /*
                 * Check dirtiness (flush here even small pages if dirty. Small pages flush IGNORES dirty data
                 * handling).
                 */
                if (dirt > 0 && (dirt >= dirtyPageThreshold || metadata.size <= fillPageThreshold)) {
                    flushingDirtyPages.add(new WeightedPage(pageId, dirt));
                    continue;
                }

                /* Check emptiness (with a really dirty check to avoid to rewrite an unfillable page) */
                if (metadata.size <= fillPageThreshold
                        && maxLogicalPageSize - metadata.avgRecordSize >= fillPageThreshold) {
                    flushingSmallPages.add(new WeightedPage(pageId, metadata.size));
                    continue;
                }

            }

            /* Clean dirtier first */
            flushingDirtyPages.sort(WeightedPage.DESCENDING_ORDER);

            /* Clean smaller first */
            flushingSmallPages.sort(WeightedPage.ASCENDING_ORDER);

            pageAnalysis = System.currentTimeMillis();

            /* Rebuild dirty pages with only records to be kept */
            for (WeightedPage weighted : flushingDirtyPages) {

                /* Page flushed */
                flushedPages.add(weighted.pageId);
                ++flushedDirtyPages;

                final DataPage dataPage = pages.get(weighted.pageId);
                final Collection<Record> records;
                if (dataPage == null) {
                    records = dataStorageManager.readPage(tableSpaceUUID, table.uuid, weighted.pageId);
                    LOGGER.log(Level.FINEST, "loaded dirty page {0} on tmp buffer: {1} records", new Object[]{weighted.pageId, records.size()});
                } else {
                    records = dataPage.data.values();
                }

                for (Record record : records) {
                    /* Avoid the record if has been modified or deleted */
                    final Long currentPageId = keyToPage.get(record.key);
                    if (currentPageId == null || !weighted.pageId.equals(currentPageId)) {
                        continue;
                    }

                    /* Flush the page if it would exceed max page size */
                    if (bufferPageSize + DataPage.estimateEntrySize(record) > maxLogicalPageSize) {
                        createImmutablePage(buffer, bufferPageSize);
                        flushedRecords += buffer.size();
                        bufferPageSize = 0;
                        /* Do not clean old buffer! It will used in generated pages to avoid too many copies! */
                        buffer = new HashMap<>(buffer.size());
                    }

                    buffer.put(record.key, record);
                    bufferPageSize += DataPage.estimateEntrySize(record);
                }

                /* Do not continue if we have used up all configured checkpoint time */
                if (checkpointLimitInstant <= System.currentTimeMillis()) {
                    break;
                }
            }

            dirtyPagesFlush = System.currentTimeMillis();

            /*
             * If there is only one without additional data to add
             * rebuilding the page make no sense: is too probable to rebuild an identical page!
             */
            if (flushingSmallPages.size() == 1 && buffer.isEmpty()) {
                boolean hasNewPagesData = newPages.values().stream().filter(p -> !p.isEmpty()).findAny().isPresent();
                if (!hasNewPagesData) {
                    flushingSmallPages.clear();
                }
            }

            final long compactionLimitInstant = sumOverflowWise(dirtyPagesFlush, compactionTargetTime);

            /* Rebuild too small pages */
            for (WeightedPage weighted : flushingSmallPages) {

                /* Page flushed */
                flushedPages.add(weighted.pageId);
                ++flushedSmallPages;

                final DataPage dataPage = pages.get(weighted.pageId);
                final Collection<Record> records;
                if (dataPage == null) {
                    records = dataStorageManager.readPage(tableSpaceUUID, table.uuid, weighted.pageId);
                    LOGGER.log(Level.FINEST, "loaded small page {0} on tmp buffer: {1} records", new Object[]{weighted.pageId, records.size()});
                } else {
                    records = dataPage.data.values();
                }

                for (Record record : records) {
                    /* Flush the page if it would exceed max page size */
                    if (bufferPageSize + DataPage.estimateEntrySize(record) > maxLogicalPageSize) {
                        createImmutablePage(buffer, bufferPageSize);
                        flushedRecords += buffer.size();
                        bufferPageSize = 0;
                        /* Do not clean old buffer! It will used in generated pages to avoid too many copies! */
                        buffer = new HashMap<>(buffer.size());
                    }

                    buffer.put(record.key, record);
                    bufferPageSize += DataPage.estimateEntrySize(record);
                }

                final long now = System.currentTimeMillis();

                /*
                 * Do not continue if we have used up all configured compaction or checkpoint time (but still compact at
                 * least the smaller page (normally the leftover from last checkpoint)
                 */
                if (compactionLimitInstant <= now || checkpointLimitInstant <= now) {
                    break;
                }
            }

            flushingSmallPages.clear();

            smallPagesFlush = System.currentTimeMillis();

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
                if (!dataPage.isEmpty()) {
                    bufferPageSize -= flushNewPageForCheckpoint(dataPage, buffer);
//                    dataPage.makeImmutable();
                    ++flushedNewPages;
                    flushedRecords += dataPage.size();
                }
            }

            /* Flush remaining records */
            if (!buffer.isEmpty()) {
                createImmutablePage(buffer, bufferPageSize);
                flushedRecords += buffer.size();
                bufferPageSize = 0;
                /* Do not clean old buffer! It will used in generated pages to avoid too many copies! */
            }

            newPagesFlush = System.currentTimeMillis();

            LOGGER.log(Level.INFO, "checkpoint {0}, logpos {1}, flushed: {2} dirty pages, {3} small pages, {4} new pages, {5} records",
                    new Object[]{table.name, sequenceNumber, flushedDirtyPages, flushedSmallPages, flushedNewPages, flushedRecords});

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
                    Bytes.from_long(nextPrimaryKeyValue.get()).data, nextPageId,
                    pageSet.getActivePages());

            actions.addAll(dataStorageManager.tableCheckpoint(tableSpaceUUID, table.uuid, tableStatus, pin));
            tablecheckpoint = System.currentTimeMillis();

            /* Writes done! Unloading and removing not needed pages and keys */

 /* Remove flushed pages handled */
            for (Long pageId : flushedPages) {
                final DataPage page = pages.remove(pageId);
                /* Current dirty record page isn't known to page replacement policy */
                if (page != null && currentDirtyRecordsPage.get() != page.pageId) {
                    pageReplacementPolicy.remove(page);
                }
            }

            /*
             * Can happen when at checkpoint start all pages are set as dirty or immutable (immutable or
             * unloaded) due do a deletion: all pages will be removed and no page will remain alive.
             */
            if (newPages.isEmpty()) {
                /* Allocate live handles the correct policy load/unload of last dirty page */
                allocateLivePage(currentDirtyRecordsPage.get());
            }

            checkPointRunning = false;

            result = new TableCheckpoint(table.name, sequenceNumber, actions);

            end = System.currentTimeMillis();

            LOGGER.log(Level.INFO, "checkpoint {0} finished, logpos {1}, {2} active pages, {3} dirty pages, "
                    + "flushed {4} records, total time {5} ms",
                    new Object[]{table.name, sequenceNumber, pageSet.getActivePagesCount(),
                        pageSet.getDirtyPagesCount(), flushedRecords, Long.toString(end - start)});

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
    public DataScanner scan(ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction, boolean lockRequired, boolean forWrite) throws StatementExecutionException {
        TupleComparator comparator = statement.getComparator();
        if (!ENABLE_STREAMING_DATA_SCANNER || (comparator != null
                && this.stats.getTablesize() > HUGE_TABLE_SIZE_FORCE_MATERIALIZED_RESULTSET)) {
            boolean sortedByClusteredIndex = comparator != null
                    && comparator.isOnlyPrimaryKeyAndAscending()
                    && keyToPage.isSortedAscending();
            if (!sortedByClusteredIndex) {
                return scanNoStream(statement, context, transaction, lockRequired, forWrite);
            }
        }
        return scanWithStream(statement, context, transaction, lockRequired, forWrite);
    }

    private DataScanner scanNoStream(ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction, boolean lockRequired, boolean forWrite) throws StatementExecutionException {

        boolean sorted = statement.getComparator() != null;
        boolean sortedByClusteredIndex = statement.getComparator() != null
                && statement.getComparator().isOnlyPrimaryKeyAndAscending()
                && keyToPage.isSortedAscending();
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
        return new SimpleDataScanner(transaction != null ? transaction.transactionId : 0, recordSet);
    }

    private DataScanner scanWithStream(ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction, boolean lockRequired, boolean forWrite) throws StatementExecutionException {
        final TupleComparator comparator = statement.getComparator();
        boolean sorted = comparator != null;
        boolean sortedByClusteredIndex = comparator != null
                && comparator.isOnlyPrimaryKeyAndAscending()
                && keyToPage.isSortedAscending();
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

        Stream<DataAccessor> fromTransactionSorted
                = streamTransactionData(transaction, statement.getPredicate(), context)
                        .map(mapper);
        if (comparator != null) {
            fromTransactionSorted = fromTransactionSorted.sorted(comparator);
        }

        Stream<DataAccessor> tableData = streamTableData(statement, context, transaction, lockRequired, forWrite)
                .map(mapper);
        if (maxRows > 0) {
            if (sortedByClusteredIndex) {
                // already sorted from index
                tableData = tableData.limit(maxRows + offset);
                // already sorted if needed
                fromTransactionSorted = fromTransactionSorted.limit(maxRows + offset);
                // we need to re-sort
                result = Stream.concat(fromTransactionSorted, tableData)
                        .sorted(comparator);
            } else if (sorted) {
                // need to sort
                tableData = tableData.sorted(comparator);
                tableData = tableData.limit(maxRows + offset);
                // already sorted if needed
                fromTransactionSorted = fromTransactionSorted.limit(maxRows + offset);
                // we need to re-sort
                result = Stream.concat(fromTransactionSorted, tableData)
                        .sorted(comparator);
            } else {
                result = Stream.concat(fromTransactionSorted, tableData);
            }
        } else {
            if (sortedByClusteredIndex) {
                // already sorted from index
                tableData = tableData.sorted(comparator);
                // fromTransactionSorted is already sorted
                // we need to re-sort
                result = Stream.concat(fromTransactionSorted, tableData)
                        .sorted(comparator);
            } else if (sorted) {
                // tableData already sorted from index
                // fromTransactionSorted is already sorted
                // we need to re-sort
                result = Stream.concat(fromTransactionSorted, tableData)
                        .sorted(comparator);
            } else {
                // no need to sort
                result = Stream.concat(fromTransactionSorted, tableData);
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
        return new StreamDataScanner(transaction != null ? transaction.transactionId : 0, fieldNames, columns, result);
    }

    private void accessTableData(ScanStatement statement, StatementEvaluationContext context, ScanResultOperation consumer, Transaction transaction,
            boolean lockRequired, boolean forWrite) throws StatementExecutionException {
        statement.validateContext(context);
        Predicate predicate = statement.getPredicate();
        long _start = System.currentTimeMillis();
        boolean acquireLock = transaction != null || forWrite || lockRequired;

        LocalScanPageCache lastPageRead = acquireLock ? null : new LocalScanPageCache();

        try {

            IndexOperation indexOperation = predicate != null ? predicate.getIndexOperation() : null;
            boolean primaryIndexSeek = indexOperation instanceof PrimaryIndexSeek;
            AbstractIndexManager useIndex = getIndexForTbleAccess(indexOperation);

            BatchOrderedExecutor.Executor<Map.Entry<Bytes, Long>> scanExecutor = (List<Map.Entry<Bytes, Long>> batch) -> {
                for (Map.Entry<Bytes, Long> entry : batch) {
                    Bytes key = entry.getKey();
                    boolean already_locked = transaction != null && transaction.lookupLock(table.name, key) != null;
                    boolean record_discarded = !already_locked;
                    LockHandle lock = acquireLock ? (forWrite ? lockForWrite(key, transaction) : lockForRead(key, transaction)) : null;
                    LOGGER.log(Level.SEVERE, "CREATED LOCK " + lock);
                    try {
                        if (transaction != null) {
                            if (transaction.recordDeleted(table.name, key)) {
                                // skip this record. inside current transaction it has been deleted
                                continue;
                            }
                            Record record = transaction.recordUpdated(table.name, key);
                            if (record != null) {
                                // use current transaction version of the record
                                if (predicate == null || predicate.evaluate(record, context)) {
                                    // now the consumer is the owner of the lock on the record
                                    record_discarded = false;
                                    consumer.accept(record, transaction == null ? lock : null);

                                }
                                continue;
                            }
                        }
                        Long pageId = entry.getValue();
                        if (pageId != null) {
                            boolean pkFilterCompleteMatch = false;
                            if (!primaryIndexSeek && predicate != null) {
                                Predicate.PrimaryKeyMatchOutcome outcome
                                        = predicate.matchesRawPrimaryKey(key, context);
                                if (outcome == Predicate.PrimaryKeyMatchOutcome.FAILED) {
                                    continue;
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
            };
            BatchOrderedExecutor<Map.Entry<Bytes, Long>> executor = new BatchOrderedExecutor<>(SORTED_PAGE_ACCESS_WINDOW_SIZE,
                    scanExecutor, SORTED_PAGE_ACCESS_COMPARATOR);
            Stream<Map.Entry<Bytes, Long>> scanner = keyToPage.scanner(indexOperation, context, tableContext, useIndex);
            boolean exit = false;
            try {
                scanner.forEach(executor);
                executor.finish();
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
                    throw (StatementExecutionException) error;
                } else if (error instanceof DataStorageManagerException) {
                    throw (DataStorageManagerException) error;
                } else {
                    throw new StatementExecutionException(error);
                }
            }

            if (!exit && transaction != null) {
                consumer.beginNewRecordsInTransactionBlock();
                Collection<Record> newRecordsForTable = transaction.getNewRecordsForTable(table.name);
                for (Record record : newRecordsForTable) {
                    if (!transaction.recordDeleted(table.name, record.key)
                            && (predicate == null || predicate.evaluate(record, context))) {
                        consumer.accept(record, null);
                    }
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

    private Stream<Record> streamTableData(ScanStatement statement, StatementEvaluationContext context,
            Transaction transaction,
            boolean lockRequired, boolean forWrite) throws StatementExecutionException {
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

    private Stream<Record> streamTransactionData(Transaction transaction, Predicate predicate, StatementEvaluationContext context) {
        if (transaction != null) {
            Collection<Record> newRecordsForTable = transaction.getNewRecordsForTable(table.name);
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
            return Stream.empty();
        }
    }

    public Record accessRecord(Map.Entry<Bytes, Long> entry,
            Predicate predicate, StatementEvaluationContext context,
            Transaction transaction, LocalScanPageCache lastPageRead, boolean primaryIndexSeek,
            boolean forWrite, boolean acquireLock) {

        Bytes key = entry.getKey();
        boolean keep_lock = false;
        boolean already_locked = transaction != null && transaction.lookupLock(table.name, key) != null;
        LockHandle lock = acquireLock ? (forWrite ? lockForWrite(key, transaction) : lockForRead(key, transaction)) : null;
        try {
            if (transaction != null) {
                if (transaction.recordDeleted(table.name, key)) {
                    // skip this record. inside current transaction it has been deleted
                    return null;
                }
                Record record = transaction.recordUpdated(table.name, key);
                if (record != null) {
                    // use current transaction version of the record
                    if (predicate == null || predicate.evaluate(record, context)) {
                        keep_lock = true;
                        return record;
                    }
                    return null;
                }
            }
            Long pageId = entry.getValue();
            if (pageId != null) {
                boolean pkFilterCompleteMatch = false;
                if (!primaryIndexSeek && predicate != null) {
                    Predicate.PrimaryKeyMatchOutcome outcome
                            = predicate.matchesRawPrimaryKey(key, context);
                    if (outcome == Predicate.PrimaryKeyMatchOutcome.FAILED) {
                        return null;
                    } else if (outcome == Predicate.PrimaryKeyMatchOutcome.FULL_CONDITION_VERIFIED) {
                        pkFilterCompleteMatch = true;
                    }
                }
                Record record = fetchRecord(key, pageId, lastPageRead);
                if (record != null && (pkFilterCompleteMatch || predicate == null || predicate.evaluate(record, context))) {

                    keep_lock = true;
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
                    return record;
                }
            }

            Long relocatedPageId = keyToPage.get(key);
            LOGGER.log(Level.SEVERE, table.name + " fetchRecord " + key + " failed,"
                    + "checkPointRunning:" + checkPointRunning + " pageId:" + pageId + " relocatedPageId:" + relocatedPageId);
            if (relocatedPageId == null) {
                // deleted
                LOGGER.log(Level.SEVERE, "table " + table.name + ", activePages " + pageSet.getActivePages() + ", record " + key + " deleted during data access");
                return null;
            }
            pageId = relocatedPageId;
            if (maxTrials-- == 0) {
                throw new DataStorageManagerException("inconsistency! table " + table.name + " no record in memory for " + key + " page " + pageId + ", activePages " + pageSet.getActivePages() + " after many trials");
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
                p.data.values().forEach(r -> r.clearCache());
            });
        }

        if (this.table.auto_increment) {
            rebuildNextPrimaryKeyValue();
        }
    }

    @Override
    public long getCreatedInTransaction() {
        return createdInTransaction;
    }

    private static final Comparator<Map.Entry<Bytes, Long>> SORTED_PAGE_ACCESS_COMPARATOR = (a, b) -> {
        return a.getValue().compareTo(b.getValue());
    };
}
