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

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.backup.DumpedLogEntry;
import herddb.client.ClientConfiguration;
import herddb.client.ClientSideMetadataProvider;
import herddb.client.ClientSideMetadataProviderException;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.core.AbstractTableManager.TableCheckpoint;
import herddb.core.stats.TableManagerStats;
import herddb.core.stats.TableSpaceManagerStats;
import herddb.core.system.SysclientsTableManager;
import herddb.core.system.SyscolumnsTableManager;
import herddb.core.system.SysconfigTableManager;
import herddb.core.system.SysdualTableManager;
import herddb.core.system.SysforeignkeysTableManager;
import herddb.core.system.SysindexcolumnsTableManager;
import herddb.core.system.SysindexesTableManager;
import herddb.core.system.SyslogstatusManager;
import herddb.core.system.SysnodesTableManager;
import herddb.core.system.SysstatementsTableManager;
import herddb.core.system.SystablesTableManager;
import herddb.core.system.SystablespacereplicastateTableManager;
import herddb.core.system.SystablespacesTableManager;
import herddb.core.system.SystablestatsTableManager;
import herddb.core.system.SystransactionsTableManager;
import herddb.data.consistency.TableChecksum;
import herddb.data.consistency.TableDataChecksum;
import herddb.index.MemoryHashIndexManager;
import herddb.index.brin.BRINIndexManager;
import herddb.jmx.JMXUtils;
import herddb.log.CommitLog;
import herddb.log.CommitLogListener;
import herddb.log.CommitLogResult;
import herddb.log.FullRecoveryNeededException;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.log.RestoredFromSnapshot;
import herddb.log.RestoredFromSnapshotException;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.DDLException;
import herddb.model.DDLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ForeignKeyDef;
import herddb.model.Index;
import herddb.model.IndexAlreadyExistsException;
import herddb.model.IndexDoesNotExistException;
import herddb.model.NodeMetadata;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableAlreadyExistsException;
import herddb.model.TableAwareStatement;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.Transaction;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.AlterTableStatement;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DropIndexStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.SQLPlannedOperationStatement;
import herddb.model.commands.ScanStatement;
import herddb.network.Channel;
import herddb.network.ServerHostData;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.server.ServerConfiguration;
import herddb.sql.TranslatedQuery;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.utils.Bytes;
import herddb.utils.Futures;
import herddb.utils.KeyValue;
import herddb.utils.SystemProperties;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Manages a TableSet in memory
 *
 * @author enrico.olivelli
 */
public class TableSpaceManager {
    private static final boolean ENABLE_PENDING_TRANSACTION_CHECK = SystemProperties.getBooleanSystemProperty("herddb.tablespace.checkpendingtransactions", true);

    private static final Logger LOGGER = Logger.getLogger(TableSpaceManager.class.getName());

    /**
     * How long the acknowledgement of the message that reports a failed dump is waited for. Nothing depends on that
     * acknowledgement, this only bounds how long the message is remembered for.
     */
    private static final long DUMP_FAILED_TIMEOUT = 60000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    final StatsLogger tablespaceStasLogger;
    final OpStatsLogger checkpointTimeStats;

    private final MetadataStorageManager metadataStorageManager;
    private final DataStorageManager dataStorageManager;
    private final CommitLog log;
    private final String tableSpaceName;
    private final String tableSpaceUUID;
    private final String nodeId;
    private final ConcurrentHashMap<String, AbstractTableManager> tables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AbstractIndexManager> indexes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Map<String, AbstractIndexManager>> indexesByTable = new ConcurrentHashMap<>();
    private final StampedLock generalLock = new StampedLock();
    private final AtomicLong newTransactionId = new AtomicLong();
    private final DBManager dbmanager;
    private volatile FollowerThread followerThread;
    private final ExecutorService callbacksExecutor;
    private final boolean virtual;

    private volatile boolean recoveryInProgress;
    private volatile boolean leader;
    private volatile boolean closed;
    private volatile boolean failed;
    private LogSequenceNumber actualLogSequenceNumber;

    /**
     * Position of the marker that opened the restore from a snapshot that is running on this tablespace, or
     * {@code null} when no restore is open. Every marker that changes it goes through
     * {@link #applyRestoredFromSnapshot}, whether the marker is being replayed at boot, applied by a follower tailing
     * the leader or written by the restore itself. The other writes of the field read no marker at all and belong to
     * the lifecycle of a boot: {@link #replayLog} clears it before every pass over the log, and
     * {@link #writeRestoredFromSnapshotMarker} sets it by hand when the marker did reach the log but could not be
     * applied.
     * <p>
     * A restore writes tables, records and indexes straight into the storage and nothing to the log, so the marker
     * that opens it is the only thing that tells a node booting after a crash that the content it holds is
     * incomplete. While this field is set the content of the tablespace is a fragment of that snapshot and no
     * checkpoint may persist it: a checkpoint would both write that incomplete content and move the position the
     * tablespace is aligned to past the marker, so the next boot would start above the marker, never meet it, and
     * declare the tablespace healthy while it holds half a snapshot. On a leader the same checkpoint also drops the
     * ledgers up to that position, so the marker can be gone for good.
     * </p>
     */
    private volatile LogSequenceNumber restoreFromSnapshot;

    /**
     * Whether the restore that is open on this tablespace is the one this node is serving, as opposed to one it only
     * learned about by reading a marker somebody else wrote.
     * <p>
     * This is local knowledge and it is kept locally: the marker on the log says that a restore happened, and nothing
     * about who is driving it. The one thing that depends on the answer is how this node gives up on a restore that
     * has stopped making progress, because a node that is driving one can say that it was abandoned while a node that
     * is merely watching can only say that it has waited long enough.
     * </p>
     */
    private volatile boolean restoreDrivenByThisNode;

    /**
     * Position of the restore marker met while replaying the log during this boot, or {@code null} if there was none.
     * <p>
     * It is not cleared between the passes over the log a single boot makes, because what it records is a property of
     * the local content of this node and not of one pass: the content the tablespace holds here stops below a point
     * where the whole content was replaced, and reading the log again from a higher position does not put it back.
     * Only downloading the content from the leader does, which is why that is the one thing that clears it.
     * </p>
     */
    private volatile LogSequenceNumber restoreMarkerMetWhileBooting;

    /**
     * Position the snapshot currently being restored was taken at, on the system it comes from. It is the most recent
     * of the positions of the dumped tables, and it is only known while a restore is running.
     */
    private volatile LogSequenceNumber restoreSourceLogSequenceNumber = LogSequenceNumber.START_OF_TIME;

    /**
     * When the restore that is open last did anything at all. Every request a restore served here is made of goes
     * through {@link #restoreInProgress()}, so on the node that is running the restore this is its age rather than
     * its duration: a restore of a large snapshot keeps it fresh for as long as it takes, and only a restore nobody
     * is driving any more lets it go stale. On a node that is only watching somebody else's restore there is nothing
     * to keep it fresh, and it stays at the moment the marker that opened the restore was read.
     * <p>
     * It is set wherever {@link #restoreFromSnapshot} is set, so it always describes the restore that is open now and
     * never a restore that is already over.
     * </p>
     */
    private volatile long restoreLastActivity;

    /**
     * How long a restore may make no progress at all before this node concludes that nobody is driving it any more.
     */
    private final long restoreMaxInactivityTime;

    // only for tests
    private Runnable afterTableCheckPointAction;

    public Runnable getAfterTableCheckPointAction() {
        return afterTableCheckPointAction;
    }

    public void setAfterTableCheckPointAction(Runnable afterTableCheckPointAction) {
        this.afterTableCheckPointAction = afterTableCheckPointAction;
    }

    public String getTableSpaceName() {
        return tableSpaceName;
    }

    public String getTableSpaceUUID() {
        return tableSpaceUUID;
    }

    public TableSpaceManager(String nodeId, String tableSpaceName,
                             String tableSpaceUUID,
                             int expectedReplicaCount,
                             MetadataStorageManager metadataStorageManager,
                             DataStorageManager dataStorageManager,
                             CommitLog log, DBManager manager, boolean virtual) {
        this.nodeId = nodeId;
        this.dbmanager = manager;
        this.callbacksExecutor = dbmanager.getCallbacksExecutor();
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.log = log;
        this.tableSpaceName = tableSpaceName;
        this.tableSpaceUUID = tableSpaceUUID;
        this.virtual = virtual;
        this.tablespaceStasLogger = this.dbmanager.getStatsLogger().scope(this.tableSpaceName);
        this.checkpointTimeStats = this.tablespaceStasLogger.getOpStatsLogger("checkpointTime");
        this.restoreMaxInactivityTime = dbmanager.getServerConfiguration().getLong(
                ServerConfiguration.PROPERTY_RESTORE_MAX_INACTIVITY_TIME,
                ServerConfiguration.PROPERTY_RESTORE_MAX_INACTIVITY_TIME_DEFAULT);
        this.dataStorageManager.tableSpaceMetadataUpdated(tableSpaceUUID, expectedReplicaCount);
    }

    private void bootSystemTables() {
        if (virtual) {
            registerSystemTableManager(new SysconfigTableManager(this));
            registerSystemTableManager(new SysclientsTableManager(this));
        } else {
            registerSystemTableManager(new SystablesTableManager(this));
            registerSystemTableManager(new SystablestatsTableManager(this));
            registerSystemTableManager(new SysindexesTableManager(this));
            registerSystemTableManager(new SysindexcolumnsTableManager(this));
            registerSystemTableManager(new SyscolumnsTableManager(this));
            registerSystemTableManager(new SystransactionsTableManager(this));
            registerSystemTableManager(new SyslogstatusManager(this));
            registerSystemTableManager(new SysdualTableManager(this));
            registerSystemTableManager(new SysforeignkeysTableManager(this));
        }
        registerSystemTableManager(new SystablespacesTableManager(this));
        registerSystemTableManager(new SystablespacereplicastateTableManager(this));
        registerSystemTableManager(new SysnodesTableManager(this));
        registerSystemTableManager(new SysstatementsTableManager(this));

    }

    private void registerSystemTableManager(AbstractTableManager tableManager) {
        tables.put(tableManager.getTable().name, tableManager);
    }

    void start() throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException, DDLException {

        TableSpace tableSpaceInfo = metadataStorageManager.describeTableSpace(tableSpaceName);

        bootSystemTables();
        if (virtual) {
            startAsLeader(1);
        } else {
            dataStorageManager.initTablespace(tableSpaceUUID);
            recover(tableSpaceInfo);

            LOGGER.log(Level.INFO, " after recovery of tableSpace {0}, actualLogSequenceNumber:{1}", new Object[]{tableSpaceName, actualLogSequenceNumber});

            tableSpaceInfo = metadataStorageManager.describeTableSpace(tableSpaceName);
            if (tableSpaceInfo.leaderId.equals(nodeId)) {
                startAsLeader(tableSpaceInfo.expectedReplicaCount);
            } else {
                startAsFollower();
            }
        }
    }

    void recover(TableSpace tableSpaceInfo) throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException {
        if (recoveryInProgress) {
            throw new HerdDBInternalException("Cannot run recovery twice");
        }
        recoveryInProgress = true;
        try {
            LogSequenceNumber logSequenceNumber = dataStorageManager.getLastcheckpointSequenceNumber(tableSpaceUUID);
            actualLogSequenceNumber = logSequenceNumber;
            LOGGER.log(Level.INFO, "{0} recover {1}, logSequenceNumber from DataStorage: {2}", new Object[]{nodeId, tableSpaceName, logSequenceNumber});
            List<Table> tablesAtBoot = dataStorageManager.loadTables(logSequenceNumber, tableSpaceUUID);
            List<Index> indexesAtBoot = dataStorageManager.loadIndexes(logSequenceNumber, tableSpaceUUID);
            String tableNames = tablesAtBoot.stream().map(t -> {
                return t.name;
            }).collect(Collectors.joining(","));

            String indexNames = indexesAtBoot.stream().map(t -> {
                return t.name + " on table " + t.table;
            }).collect(Collectors.joining(","));

            if (!tableNames.isEmpty()) {
                LOGGER.log(Level.INFO, "{0} {1} tablesAtBoot: {2}, indexesAtBoot: {3}", new Object[]{nodeId, tableSpaceName, tableNames, indexNames});
            }

            for (Table table : tablesAtBoot) {
                TableManager tableManager = bootTable(table, 0, null, false);
                for (Index index : indexesAtBoot) {
                    if (index.table.equals(table.name)) {
                        bootIndex(index, tableManager, false, 0, false, false);
                    }
                }
            }
            dataStorageManager.loadTransactions(logSequenceNumber, tableSpaceUUID, t -> {
                transactions.put(t.transactionId, t);
                LOGGER.log(Level.FINER, "{0} {1} tx {2} at boot lsn {3}", new Object[]{nodeId, tableSpaceName, t.transactionId, t.lastSequenceNumber});
                try {
                    if (t.newTables != null) {
                        for (Table table : t.newTables.values()) {
                            if (!tables.containsKey(table.name)) {
                                bootTable(table, t.transactionId, null, false);
                            }
                        }
                    }
                    if (t.newIndexes != null) {
                        for (Index index : t.newIndexes.values()) {
                            if (!indexes.containsKey(index.name)) {
                                AbstractTableManager tableManager = tables.get(index.table);
                                bootIndex(index, tableManager, false, t.transactionId, false, false);
                            }
                        }
                    }
                } catch (Exception err) {
                    LOGGER.log(Level.SEVERE, "error while booting tmp tables " + err, err);
                    throw new RuntimeException(err);
                }
            });

            boolean thisNodeIsTheLeader = tableSpaceInfo != null && nodeId.equals(tableSpaceInfo.leaderId);
            if (LogSequenceNumber.START_OF_TIME.equals(logSequenceNumber)
                    && dbmanager.getServerConfiguration().getBoolean(ServerConfiguration.PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT, ServerConfiguration.PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT_DEFAULT)) {
                LOGGER.log(Level.SEVERE, nodeId + " full recovery of data is forced (" + ServerConfiguration.PROPERTY_BOOT_FORCE_DOWNLOAD_SNAPSHOT + "=true) for tableSpace " + tableSpaceName);
                downloadTableSpaceData();
                replayLog(actualLogSequenceNumber, false, thisNodeIsTheLeader);
            } else {
                try {
                    replayLog(logSequenceNumber, false, thisNodeIsTheLeader);
                } catch (FullRecoveryNeededException fullRecoveryNeeded) {
                    // a restore marker met by the leader never gets here: replayLog refuses to boot the tablespace
                    // instead, because the leader is the one node that has nowhere to download the data from
                    LOGGER.log(Level.SEVERE, nodeId + " full recovery of data is needed for tableSpace " + tableSpaceName, fullRecoveryNeeded);
                    try {
                        downloadTableSpaceData();
                        replayLog(actualLogSequenceNumber, false, thisNodeIsTheLeader);
                    } catch (FullRecoveryNeededException stillNotEnough) {
                        // the data downloaded from the leader still does not allow us to replay the log:
                        // downloading it again would not change anything, there is nothing else to try
                        stillNotEnough.addSuppressed(fullRecoveryNeeded);
                        throw new DataStorageManagerException("Tablespace " + tableSpaceName + " cannot be booted on node "
                                + nodeId + ": the data downloaded from the leader is not enough to replay the log", stillNotEnough);
                    } catch (DataStorageManagerException | LogNotAvailableException | MetadataStorageManagerException failure) {
                        // keep track of why the download was attempted at all, the failure of the download alone
                        // does not tell that the local data of this node cannot be used to boot the tablespace
                        failure.addSuppressed(fullRecoveryNeeded);
                        throw failure;
                    }
                }
            }
        } finally {
            recoveryInProgress = false;
        }
        // the checkpoint that closes the recovery is outside it, and it has to be: a checkpoint taken while the
        // recovery is still marked as running is skipped
        if (!LogSequenceNumber.START_OF_TIME.equals(actualLogSequenceNumber)) {
            LOGGER.log(Level.INFO, "Recovery finished for {0} seqNum {1}", new Object[]{tableSpaceName, actualLogSequenceNumber});
            checkpoint(false, false, false);
        }

    }

    /**
     * Replays the log on top of the current content of the tablespace and then makes sure that this node can serve
     * the content the log describes.
     *
     * @param from the position the local data of the tablespace is aligned to
     * @param fencing whether the log has to be fenced while it is read, so that a previous leader that is still
     * running cannot write to it any more. This is the split-brain guard of the promotion to leader and it is never
     * enabled on an ordinary boot
     * @param thisNodeIsTheLeader whether this node is the one that leads the tablespace, or is about to. It cannot be
     * read from {@link #isLeader()} here, because the whole recovery runs before the tablespace manager declares
     * itself leader or follower
     */
    private void replayLog(
            LogSequenceNumber from, boolean fencing, boolean thisNodeIsTheLeader
    ) throws DataStorageManagerException, LogNotAvailableException {
        restoreFromSnapshot = null;
        RestoredFromSnapshotException restoreMetOnThisPass = null;
        try {
            log.recovery(from, new ApplyEntryOnRecovery(), fencing);
        } catch (RestoredFromSnapshotException restore) {
            // the log says the content of the tablespace was replaced, and the local data of this node is below the
            // marker: no checkpoint of the replaced content was ever taken here
            restoreMetOnThisPass = restore;
        }
        if (thisNodeIsTheLeader && restoreMarkerMetWhileBooting != null) {
            throw restoreOfATableSpaceThisNodeDoesNotHold(restoreMetOnThisPass);
        }
        if (restoreMetOnThisPass != null) {
            throw restoreMetOnThisPass;
        }
    }

    /**
     * Refuses to boot a tablespace this node leads while the content of that tablespace is not here.
     * <p>
     * A restore marker met while replaying the log always means the same thing, whichever of the two markers it is
     * and whoever wrote it: the log does not describe the content of the tablespace, and the local data of this node
     * stops below the point where that content was replaced. Whatever the restore produced can only be downloaded
     * from the node that leads the tablespace, because the data of a restore comes from a client one request at a
     * time and never travels through the log. That is exactly what a replica does, and it is the one thing a leader
     * cannot do: a leader has nobody to download from.
     * </p>
     * <p>
     * So the tablespace does not boot here, and nothing else is touched: the content is intact wherever it is, and
     * this node simply is not the node that holds it. Booting it empty would be worse than refusing, because an
     * empty tablespace is indistinguishable from one that was just created and an application would build on a
     * foundation that was supposed to hold the restored data. Both ways out are named in the message, because they
     * are commands and the node has to stay alive to receive them.
     * </p>
     */
    private TableSpaceCannotBeLedHereException restoreOfATableSpaceThisNodeDoesNotHold(Throwable cause) {
        return new TableSpaceCannotBeLedHereException("Tablespace " + tableSpaceName + " cannot be booted on node "
                + nodeId + ": the log holds the marker of a restore from a snapshot at " + restoreMarkerMetWhileBooting
                + " and the local data of this node stops below it, so this node holds nothing of the content the"
                + " restore produced. It cannot download it either, because it is the leader of the tablespace and a"
                + " leader has nobody to download from. Either give the leadership to a node that holds the content,"
                + " with ALTER TABLESPACE '" + tableSpaceName + "','leader:<node>', or, if no node holds it any more,"
                + " throw the tablespace away with DROP TABLESPACE '" + tableSpaceName + "' and run the restore"
                + " again", tableSpaceName, cause);
    }

    void recoverForLeadership() throws DataStorageManagerException, LogNotAvailableException {
        if (recoveryInProgress) {
            throw new HerdDBInternalException("Cannot run recovery twice");
        }
        recoveryInProgress = true;
        try {
            actualLogSequenceNumber = log.getLastSequenceNumber();
            LOGGER.log(Level.INFO, "recovering tablespace {0} log from sequence number {1}, with fencing", new Object[]{tableSpaceName, actualLogSequenceNumber});
            replayLog(actualLogSequenceNumber, true, true);
            LOGGER.log(Level.INFO, "Recovery (with fencing) finished for {0}", tableSpaceName);
        } finally {
            recoveryInProgress = false;
        }
    }

    void apply(CommitLogResult position, LogEntry entry, boolean recovery) throws DataStorageManagerException, DDLException {
        apply(position, entry, recovery, false);
    }

    /**
     * @param recovery whether the entry is being replayed rather than produced by something happening now. Table
     * managers use it to skip what they already hold
     * @param fromRestoredDump whether the entry comes from the tail of the log a restore is streaming into this
     * tablespace, instead of from the log of this tablespace. Both are replays, so both pass {@code recovery}, and
     * they are told apart here because what an entry that cannot be applied means, and what can be done about it,
     * is not the same on the two paths
     */
    private void apply(
            CommitLogResult position, LogEntry entry, boolean recovery, boolean fromRestoredDump
    ) throws DataStorageManagerException, DDLException {
        if (!position.deferred || position.sync) {
            // this will wait for the write to be acknowledged by the log
            // it can throw LogNotAvailableException
            this.actualLogSequenceNumber = position.getLogSequenceNumber();
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(Level.FINEST, "apply {0} {1}", new Object[]{position.getLogSequenceNumber(), entry});
            }
        } else {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(Level.FINEST, "apply {0} {1}", new Object[]{position, entry});
            }
        }
        switch (entry.type) {
            case LogEntryType.NOOP: {
                // NOOP
            }
            break;
            case LogEntryType.BEGINTRANSACTION: {
                long id = entry.transactionId;
                Transaction transaction = new Transaction(id, tableSpaceName, position);
                transactions.put(id, transaction);
            }
            break;
            case LogEntryType.ROLLBACKTRANSACTION: {
                long id = entry.transactionId;
                Transaction transaction = transactions.get(id);
                if (transaction == null) {
                    throw new DataStorageManagerException("invalid transaction id " + id + ", only " + transactions.keySet());
                }
                List<AbstractIndexManager> indexManagers = new ArrayList<>(indexes.values());
                for (AbstractIndexManager indexManager : indexManagers) {
                    if (indexManager.getCreatedInTransaction() == 0 || indexManager.getCreatedInTransaction() == id) {
                        indexManager.onTransactionRollback(transaction);
                    }
                }
                List<AbstractTableManager> managers = new ArrayList<>(tables.values());
                for (AbstractTableManager manager : managers) {
                    if (manager.getCreatedInTransaction() == 0 || manager.getCreatedInTransaction() == id) {
                        Table table = manager.getTable();
                        if (transaction.isNewTable(table.name)) {
                            LOGGER.log(Level.INFO, "rollback CREATE TABLE " + table.tablespace + "." + table.name);
                            disposeTable(manager);
                            Map<String, AbstractIndexManager> indexes = indexesByTable.remove(manager.getTable().name);
                            if (indexes != null) {
                                for (AbstractIndexManager indexManager : indexes.values()) {
                                    disposeIndexManager(indexManager);
                                }
                            }
                        } else {
                            manager.onTransactionRollback(transaction);
                        }
                    }
                }
                transactions.remove(transaction.transactionId);
            }
            break;
            case LogEntryType.COMMITTRANSACTION: {
                long id = entry.transactionId;
                Transaction transaction = transactions.get(id);
                if (transaction == null) {
                    throw new DataStorageManagerException("invalid transaction id " + id);
                }
                LogSequenceNumber commit = position.getLogSequenceNumber();
                transaction.sync(commit);
                List<AbstractTableManager> managers = new ArrayList<>(tables.values());
                for (AbstractTableManager manager : managers) {
                    if (manager.getCreatedInTransaction() == 0 || manager.getCreatedInTransaction() == id) {
                        manager.onTransactionCommit(transaction, recovery);
                    }
                }
                List<AbstractIndexManager> indexManagers = new ArrayList<>(indexes.values());
                for (AbstractIndexManager indexManager : indexManagers) {
                    if (indexManager.getCreatedInTransaction() == 0 || indexManager.getCreatedInTransaction() == id) {
                        indexManager.onTransactionCommit(transaction, recovery);
                    }
                }
                if ((transaction.droppedTables != null && !transaction.droppedTables.isEmpty()) || (transaction.droppedIndexes != null && !transaction.droppedIndexes.isEmpty())) {

                    if (transaction.droppedTables != null) {
                        for (String dropped : transaction.droppedTables) {
                            for (AbstractTableManager manager : managers) {
                                if (manager.getTable().name.equals(dropped)) {
                                    disposeTable(manager);
                                }
                            }
                        }
                    }
                    if (transaction.droppedIndexes != null) {
                        for (String dropped : transaction.droppedIndexes) {
                            for (AbstractIndexManager manager : indexManagers) {
                                if (manager.getIndex().name.equals(dropped)) {
                                    disposeIndexManager(manager);
                                }
                            }
                        }
                    }

                }
                if ((transaction.newTables != null && !transaction.newTables.isEmpty())
                        || (transaction.droppedTables != null && !transaction.droppedTables.isEmpty())
                        || (transaction.newIndexes != null && !transaction.newIndexes.isEmpty())
                        || (transaction.droppedIndexes != null && !transaction.droppedIndexes.isEmpty())) {
                    writeTablesOnDataStorageManager(position, false);
                    dbmanager.getPlanner().clearCache();
                }
                transactions.remove(transaction.transactionId);
            }
            break;
            case LogEntryType.CREATE_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerNewTable(table, position);
                }

                bootTable(table, entry.transactionId, null, true);
                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position, false);
                }
            }
            break;
            case LogEntryType.CREATE_INDEX: {
                Index index = Index.deserialize(entry.value.to_array());
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerNewIndex(index, position);
                }
                AbstractTableManager tableManager = tables.get(index.table);
                if (tableManager == null) {
                    throw new RuntimeException("table " + index.table + " does not exists");
                }
                bootIndex(index, tableManager, true, entry.transactionId, true, false);
                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position, false);
                }
            }
            break;
            case LogEntryType.DROP_TABLE: {
                String tableName = entry.tableName;
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerDropTable(tableName, position);
                } else {
                    AbstractTableManager manager = tables.get(tableName);
                    if (manager != null) {
                        disposeTable(manager);
                        Map<String, AbstractIndexManager> indexes = indexesByTable.get(tableName);
                        if (indexes != null && !indexes.isEmpty()) {
                            LOGGER.log(Level.SEVERE, "It looks like we are dropping a table " + tableName + " with these indexes " + indexes);
                        }
                    }
                }

                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position, false);
                }
            }
            break;
            case LogEntryType.DROP_INDEX: {
                String indexName = entry.value.to_string();
                if (entry.transactionId > 0) {
                    long id = entry.transactionId;
                    Transaction transaction = transactions.get(id);
                    transaction.registerDropIndex(indexName, position);
                } else {
                    AbstractIndexManager manager = indexes.get(indexName);
                    if (manager != null) {
                        disposeIndexManager(manager);
                    }
                }

                if (entry.transactionId <= 0) {
                    writeTablesOnDataStorageManager(position, false);
                    dbmanager.getPlanner().clearCache();
                }
            }
            break;
            case LogEntryType.ALTER_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                alterTable(table, null);
                writeTablesOnDataStorageManager(position, false);
            }
            break;
            case LogEntryType.TABLE_CONSISTENCY_CHECK: {
                /*
                    In recovery mode, we need to skip the consistency check.
                    The tablespace may not be avaible yet and therefore calcite will not able to performed the select query.
                */
                if (recovery) {
                   LOGGER.log(Level.INFO, "skip {0} consistency check LogEntry {1}", new Object[]{tableSpaceName, entry});
                   break;
                }
                try {
                    TableChecksum check = MAPPER.readValue(entry.value.to_array(), TableChecksum.class);
                    String tableSpace = check.getTableSpaceName();
                    String query = check.getQuery();
                    String tableName = entry.tableName;
                    //In the entry type = 14, the follower will have to run the query on the transaction log
                    if (!isLeader()) {
                        AbstractTableManager tablemanager = this.getTableManager(tableName);
                        DBManager manager = this.getDbmanager();

                        if (tablemanager == null || tablemanager.getCreatedInTransaction() > 0) {
                            throw new TableDoesNotExistException(String.format("Table %s does not exist.", tablemanager));
                        }
                        /*
                            scan = true
                            allowCache = false
                            returnValues = false
                            maxRows = -1
                        */
                        TranslatedQuery translated = manager.getPlanner().translate(tableSpace, query, Collections.emptyList(), true, false, false, -1);
                        TableChecksum scanResult = TableDataChecksum.createChecksum(manager, translated, this, tableSpace, tableName);
                        long followerDigest = scanResult.getDigest();
                        long leaderDigest = check.getDigest();
                        long leaderNumRecords = check.getNumRecords();
                        long followerNumRecords = scanResult.getNumRecords();
                        //the necessary condition to pass the check is to have exactly the same digest and the number of records processed
                        if (followerDigest == leaderDigest && leaderNumRecords == followerNumRecords) {
                            LOGGER.log(Level.INFO, "Data consistency check PASS for table {0}  tablespace {1} with  Checksum {2}", new Object[]{tableName, tableSpace, followerDigest});
                        } else {
                            LOGGER.log(Level.SEVERE, "Data consistency check FAILED for table {0} in tablespace {1} with Checksum {2}", new Object[]{tableName, tableSpace, followerDigest});
                        }
                    } else {
                        long digest = check.getDigest();
                        LOGGER.log(Level.INFO, "Created checksum {0}  for table {1} in tablespace {2} on node {3}", new Object[]{digest, entry.tableName, tableSpace, this.getDbmanager().getNodeId()});
                    }
                } catch (IOException | DataScannerException ex) {
                    LOGGER.log(Level.SEVERE, "Error during table consistency check ", ex);
                }
            }
            break;
            case LogEntryType.RESTORED_FROM_SNAPSHOT: {
                applyRestoredFromSnapshot(position, entry, recovery);
            }
            break;
            default:
                // other entry types are not important for the tablespacemanager
                break;
        }

        if (entry.tableName != null
                && entry.type != LogEntryType.CREATE_TABLE
                && entry.type != LogEntryType.CREATE_INDEX
                && entry.type != LogEntryType.ALTER_TABLE
                && entry.type != LogEntryType.DROP_TABLE
                && entry.type != LogEntryType.TABLE_CONSISTENCY_CHECK) {
            AbstractTableManager tableManager = tables.get(entry.tableName);
            if (tableManager == null) {
                // An entry that changes a table this node does not have. Which entries can be in that position, and
                // what can be done about it, depends entirely on where the entry came from, so the three cases are
                // reported as three different things. What they have in common is that none of them may go on and
                // dereference the missing table manager: a NullPointerException here reaches no handler that knows
                // what it is about, and on the restore path it reaches no handler at all, leaving the client of the
                // restore waiting for a reply until its own socket timeout.
                String message = "Tablespace " + tableSpaceName + ": log entry of type " + entry.type
                        + " at " + describePosition(position) + " refers to table " + entry.tableName
                        + ", which does not exist on this node";
                if (fromRestoredDump) {
                    // The entry comes from the log the dump carries, and the table it names is not among the tables
                    // the dump carries either. Nothing about the local content of this node is wrong and there is
                    // nothing to download: the dump itself does not describe a state that can be rebuilt. A table
                    // created inside a transaction that was still open when the dump was taken is the ordinary way
                    // of producing one, because such a table is not part of a checkpoint and is never streamed,
                    // while the transaction that created it is.
                    throw new DataStorageManagerException(message + ". The entry is part of the dump being"
                            + " restored, and the dump does not carry that table: it is the dump that is"
                            + " incomplete, and restoring it again changes nothing. Take a new backup of the"
                            + " tablespace and restore that one");
                }
                if (recovery) {
                    // The log of this tablespace does not contain the creation of the table: a tablespace created
                    // by restoring a backup boots its tables directly from the dump, without writing any
                    // CREATE_TABLE entry, so a node replaying that log from the beginning cannot rebuild them. The
                    // local log alone is not enough to boot.
                    throw new FullRecoveryNeededException(message
                            + ", a full download of the data of the tablespace from the leader is needed");
                }
                // Outside recovery there is no caller able to fall back to a full download: the follower
                // thread and the local write path can only fail. Report the problem instead of throwing a
                // NullPointerException; the tablespace manager is then marked as failed and booted again,
                // and it is that new boot, going through recovery, that asks for the full download.
                throw new DataStorageManagerException(message);
            }
            tableManager.apply(position, entry, recovery);
        }

    }

    /**
     * Names the position of an entry for a message, without ever waiting for the log to say where it wrote it.
     * Asking a deferred write for its position blocks the writer until the log acknowledges it and throws a
     * {@link LogNotAvailableException} when that acknowledgement fails, so building a message would both stall the
     * write path and replace the error the caller is trying to report with an unrelated one.
     */
    private static String describePosition(CommitLogResult position) {
        if (position.deferred && !position.sync) {
            return "a position the log has not acknowledged yet";
        }
        return String.valueOf(position.getLogSequenceNumber());
    }

    /**
     * Handles the marker that tells that the content of this tablespace has been replaced by a snapshot. The restore
     * writes the tables, the records and the indexes straight into the storage of the leader, so the log holds no
     * trace of them: from the point of view of anybody replaying that log the tablespace is materialised out of
     * nothing, and the only way to get the data is to download it.
     * <p>
     * This is the only place that derives {@link #restoreFromSnapshot} from a marker, and it runs on every path a
     * marker can arrive from: the log being replayed at boot, a follower tailing the leader, and the restore itself,
     * which goes through {@link #apply} like any other writer. Reading the marker here rather than at the call sites
     * is what makes it impossible for the marker to be durable while the state that goes with it is not. The field
     * is written elsewhere too, but never out of a marker: see its declaration for the other writes.
     * </p>
     * <p>
     * The position of the marker is read here and the state of the tablespace is derived from it, so the marker has to
     * be written synchronously and every path that produces one does write it that way. The alternative is not a
     * position that arrives later, it is no position at all: asking a deferred write where it wrote blocks until the
     * log acknowledges it and throws when that acknowledgement fails, which is why {@link #apply} guards that same
     * call. A marker without a position would leave this tablespace unable to say where the restore began.
     * </p>
     */
    private void applyRestoredFromSnapshot(CommitLogResult position, LogEntry entry, boolean recovery) {
        RestoredFromSnapshot restore = RestoredFromSnapshot.deserialize(entry.value.to_array());
        if (position.deferred && !position.sync) {
            throw new HerdDBInternalException("Tablespace " + tableSpaceName + ": " + restore + " was written to the"
                    + " log without waiting for the log to say where, at " + describePosition(position)
                    + ". The markers of a restore have to be logged synchronously, the position of the marker is the"
                    + " state of the restore");
        }
        LogSequenceNumber markerPosition = position.getLogSequenceNumber();
        boolean started = restore.getPhase() == RestoredFromSnapshot.Phase.STARTED;
        if (started) {
            // the clock the give-up rule reads is set here, together with the state it describes, so that it always
            // belongs to the restore that is open now. On the node that is running the restore it is about to be
            // refreshed by every request the restore is made of; on a node that is only watching, this is the
            // moment it learned that the restore exists and nothing is ever going to move it again
            restoreLastActivity = System.currentTimeMillis();
            restoreFromSnapshot = markerPosition;
        } else {
            restoreFromSnapshot = null;
            restoreDrivenByThisNode = false;
        }
        if (recovery) {
            // Both markers say the same thing to a node replaying the log: the content of the tablespace was replaced
            // above the point the local data of this node stops at, and the log does not describe it. The whole
            // content has to come from the leader, and it is remembered here because a later pass over the log,
            // starting from a higher position, would not meet the marker again and would find nothing wrong.
            restoreMarkerMetWhileBooting = markerPosition;
            if (started) {
                // the restore writes nothing to the log between the two markers, so there is nothing left to replay
                // and nothing to skip: this node boots with the content it holds and downloads the new one as soon
                // as the marker that closes the restore reaches it
                LOGGER.log(Level.WARNING, "Tablespace {0} at {1}: {2}. The log ends inside the restore: the content"
                        + " of the tablespace is being replaced and this node does not hold the result yet",
                        new Object[]{tableSpaceName, markerPosition, restore});
                return;
            }
            throw new RestoredFromSnapshotException("Tablespace " + tableSpaceName + " at " + markerPosition + ": "
                    + restore + ". The log does not describe the content of the tablespace up to that point, so the"
                    + " whole content of the tablespace has to be downloaded from the leader", markerPosition);
        }
        if (started || isLeader()) {
            // Either this node is the one running the restore and it is writing the marker for the other nodes, or it
            // is a follower that has just been told that the content of the tablespace is about to be replaced. The
            // follower has nothing to do yet: the restore writes nothing to the log until it is over, and it is the
            // marker that closes it that says the new content exists and can be downloaded. All that matters until
            // then is that no checkpoint records the content this node holds as the state of the tablespace, and
            // recording the open restore above has already taken care of that.
            LOGGER.log(Level.INFO, "Tablespace {0} at {1}: {2}",
                    new Object[]{tableSpaceName, markerPosition, restore});
            return;
        }
        // A follower tailing the log of the leader, and the restore is over. The data this node holds is obsolete and
        // there is nothing here that can download the new one: the follower thread only knows how to apply log
        // entries, and going on would mean serving the pre-restore content until we happen to meet a change of a
        // table that the restore created and that this node does not have.
        // Marking the tablespace manager as failed is the way out the follower thread already takes on any other
        // error: the activator takes this manager out of service and boots a new one, and it is that boot, going
        // through recovery, that meets the marker again and downloads the whole content of the tablespace from the
        // leader. A failed tablespace manager takes no checkpoint, so the obsolete content this node still holds
        // cannot be recorded as the state of the tablespace in the meantime.
        LOGGER.log(Level.WARNING, "Tablespace {0} at {1}: {2}. The content of the tablespace has been replaced"
                + " on the leader, the data this node holds is obsolete: this tablespace is taken out of"
                + " service and booted again, so that the whole content of the tablespace is downloaded"
                + " from the leader", new Object[]{tableSpaceName, markerPosition, restore});
        setFailed();
    }

    private void disposeTable(AbstractTableManager manager) throws DataStorageManagerException {
        manager.dropTableData();
        manager.close();
        tables.remove(manager.getTable().name);
    }

    private void disposeIndexManager(AbstractIndexManager indexManager) throws DataStorageManagerException {
        indexManager.dropIndexData();
        indexManager.close();
        indexes.remove(indexManager.getIndex().name);
        Map<String, AbstractIndexManager> indexesForTable =
                indexesByTable.get(indexManager.getIndex().table);
        if (indexesForTable != null) {
            indexesForTable.remove(indexManager.getIndex().name);
        }
    }

    private Collection<PostCheckpointAction> writeTablesOnDataStorageManager(CommitLogResult writeLog, boolean prepareActions) throws DataStorageManagerException,
            LogNotAvailableException {
        LogSequenceNumber logSequenceNumber = writeLog.getLogSequenceNumber();
        List<Table> tablelist = new ArrayList<>();
        List<Index> indexlist = new ArrayList<>();
        for (AbstractTableManager tableManager : tables.values()) {
            if (!tableManager.isSystemTable()) {
                tablelist.add(tableManager.getTable());
            }
        }
        for (AbstractIndexManager indexManager : indexes.values()) {
            indexlist.add(indexManager.getIndex());
        }
        return dataStorageManager.writeTables(tableSpaceUUID, logSequenceNumber, tablelist, indexlist, prepareActions);
    }

    public DataScanner scan(
            ScanStatement statement, StatementEvaluationContext context,
            TransactionContext transactionContext, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        boolean rollbackOnError = false;
        if (transactionContext.transactionId == TransactionContext.AUTOTRANSACTION_ID
                && (lockRequired || forWrite || context.isForceAcquireWriteLock() || context.isForceRetainReadLock())) {
            try {
                // sync on beginTransaction
                StatementExecutionResult newTransaction = Futures.result(beginTransactionAsync(context, true));
                transactionContext = new TransactionContext(newTransaction.transactionId);
                rollbackOnError = true;
            } catch (Exception err) {
                if (err.getCause() instanceof HerdDBInternalException) {
                    throw (HerdDBInternalException) err.getCause();
                } else {
                    throw new StatementExecutionException(err.getCause());
                }
            }
        }
        Transaction transaction = transactions.get(transactionContext.transactionId);
        if (transactionContext.transactionId > 0 && transaction == null) {
            throw new StatementExecutionException("transaction " + transactionContext.transactionId + " does not exist on tablespace " + tableSpaceName);
        }
        if (transaction != null && !transaction.tableSpace.equals(tableSpaceName)) {
            throw new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName);
        }
        if (transaction != null) {
            transaction.touch();
        }
        try {
            String table = statement.getTable();
            AbstractTableManager tableManager = tables.get(table);
            if (tableManager == null) {
                throw new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName);
            }
            if (tableManager.getCreatedInTransaction() > 0) {
                if (transaction == null || transaction.transactionId != tableManager.getCreatedInTransaction()) {
                    throw new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName + ". created temporary in transaction " + tableManager.getCreatedInTransaction());
                }
            }
            return tableManager.scan(statement, context, transaction, lockRequired, forWrite);
        } catch (StatementExecutionException error) {
            if (rollbackOnError) {
                LOGGER.log(Level.FINE, tableSpaceName + " forcing rollback of implicit tx " + transactionContext.transactionId, error);
                try {
                    rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, transactionContext.transactionId), context).get();
                } catch (ExecutionException err) {
                    throw new StatementExecutionException(err.getCause());
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    error.addSuppressed(ex);
                }
            }
            throw error;
        }
    }

    private void downloadTableSpaceData() throws MetadataStorageManagerException, DataStorageManagerException, LogNotAvailableException {
        TableSpace tableSpaceData = metadataStorageManager.describeTableSpace(tableSpaceName);
        String leaderId = tableSpaceData.leaderId;
        if (this.nodeId.equals(leaderId)) {
            if (restoreMarkerMetWhileBooting != null) {
                // The leadership moved onto this node while it was replaying the log, which takes as long as the log
                // is: the boot began as a replica, which can download what it is missing, and reached this point as
                // the leader, which cannot. The conclusion is the one every node reaching a restore marker it cannot
                // act on draws, and it has to be reported as that one and not as a download that could not be made.
                throw restoreOfATableSpaceThisNodeDoesNotHold(null);
            }
            throw new DataStorageManagerException("cannot download data of tableSpace " + tableSpaceName
                    + " from myself: this node is the leader of the tablespace, so there is no other node to take the data from");
        }
        Optional<NodeMetadata> leaderAddress = metadataStorageManager.listNodes().stream().filter(n -> n.nodeId.equals(leaderId)).findAny();
        if (!leaderAddress.isPresent()) {
            throw new DataStorageManagerException("cannot download data of tableSpace " + tableSpaceName + " from leader " + leaderId + ", no metadata found");
        }

        // ensure we do not have any data on disk and in memory

        actualLogSequenceNumber = LogSequenceNumber.START_OF_TIME;
        newTransactionId.set(0);
        LOGGER.log(Level.INFO, "tablespace " + tableSpaceName + " at downloadTableSpaceData " + tables + ", " + indexes + ", " + transactions);
        Iterator<AbstractTableManager> tableManagers = tables.values().iterator();
        while (tableManagers.hasNext()) {
            AbstractTableManager manager = tableManagers.next();
            if (manager.isSystemTable()) {
                // System tables hold no data and they are not part of the dump we are about to download:
                // they are created once, by bootSystemTables(), and they must survive this reset. Dropping
                // them here would leave this tablespace without SYSTABLES, SYSCOLUMNS, ... on this node
                // until the whole process is restarted.
                continue;
            }
            // this is like a truncate table, and it releases all pages
            // and all indexes
            manager.dropTableData();
            manager.close();
            tableManagers.remove();
        }

        // this map should be empty
        for (AbstractIndexManager manager : indexes.values()) {
            manager.dropIndexData();
            manager.close();
        }
        indexes.clear();
        // the very same index managers are indexed by table name as well, and that is the map read by
        // getIndexesOnTable(): leaving it behind would expose closed index managers to the write path
        indexesByTable.clear();
        transactions.clear();

        dataStorageManager.eraseTablespaceData(tableSpaceUUID);

        NodeMetadata nodeData = leaderAddress.get();
        ClientConfiguration clientConfiguration = new ClientConfiguration(dbmanager.getTmpDirectory());
        clientConfiguration.set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, dbmanager.getServerToServerUsername());
        clientConfiguration.set(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, dbmanager.getServerToServerPassword());
        // always use network, we want to run tests with this case
        clientConfiguration.set(ClientConfiguration.PROPERTY_CLIENT_CONNECT_LOCALVM_SERVER, false);
        try (HDBClient client = new HDBClient(clientConfiguration)) {
            client.setClientSideMetadataProvider(new ClientSideMetadataProvider() {
                @Override
                public String getTableSpaceLeader(String tableSpace) throws ClientSideMetadataProviderException {
                    return leaderId;
                }

                @Override
                public ServerHostData getServerHostData(String nodeId) throws ClientSideMetadataProviderException {
                    return new ServerHostData(nodeData.host, nodeData.port, "?", nodeData.ssl, Collections.emptyMap());
                }
            });
            try (HDBConnection con = client.openConnection()) {
                ReplicaFullTableDataDumpReceiver receiver = new ReplicaFullTableDataDumpReceiver(this);
                int fetchSize = 10000;
                con.dumpTableSpace(tableSpaceName, receiver, fetchSize, false);
                receiver.getLatch().get(1, TimeUnit.HOURS);
                this.actualLogSequenceNumber = receiver.logSequenceNumber;
                // The content of this tablespace is now the content the leader holds, taken at the position above.
                // Whatever the log said below that position, a restore marker included, is about a content that is
                // not here any more and must not keep this node from serving the tablespace.
                this.restoreMarkerMetWhileBooting = null;
                LOGGER.log(Level.INFO, tableSpaceName + " After download local actualLogSequenceNumber is " + actualLogSequenceNumber);

            } catch (ClientSideMetadataProviderException | HDBException | InterruptedException | ExecutionException | TimeoutException internalError) {
                LOGGER.log(Level.SEVERE, tableSpaceName + " error downloading snapshot", internalError);
                throw new DataStorageManagerException(internalError);
            }

        }

    }

    public MetadataStorageManager getMetadataStorageManager() {
        return metadataStorageManager;
    }

    public List<Table> getAllVisibleTables(Transaction t) {
        return tables
                .values()
                .stream()
                .filter(s -> s.getCreatedInTransaction() == 0 || (t != null && t.transactionId == s.getCreatedInTransaction()))
                .map(AbstractTableManager::getTable)
                .collect(Collectors.toList());
    }

    public List<Table> getAllCommittedTables() {
        // No LOCK is necessary, since tables is a concurrent map and this function is only for
        // system monitoring
        return tables.values().stream().filter(s -> s.getCreatedInTransaction() == 0).map(AbstractTableManager::getTable).collect(Collectors.toList());

    }

    public List<Table> getAllTablesForPlanner() {
        // No LOCK is necessary, since tables is a concurrent map
        return tables.values().stream().map(AbstractTableManager::getTable).collect(Collectors.toList());

    }

    private void releaseWriteLock(long lockStamp, Object description) {
        generalLock.unlockWrite(lockStamp);
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "{0} ts {2} relwlock {1}", new Object[]{tableSpaceName, description, lockStamp});
        }
//        LOGGER.log(Level.SEVERE, "RELEASE TS WRITELOCK for " + description + " -> " + lockStamp + " " + generalLock);
    }

    public Map<String, AbstractIndexManager> getIndexesOnTable(String name) {
        Map<String, AbstractIndexManager> result = indexesByTable.get(name);
        if (result == null || result.isEmpty()) {
            return null;
        }
        return result;
    }

    boolean isTransactionRunningOnTable(String name) {
        return transactions
                .values()
                .stream()
                .anyMatch((t) -> (t.isOnTable(name)));
    }

    long handleLocalMemoryUsage() {
        long result = 0;
        for (AbstractTableManager tableManager : tables.values()) {
            TableManagerStats stats = tableManager.getStats();
            result += stats.getBuffersUsedMemory();
            result += stats.getKeysUsedMemory();
            result += stats.getDirtyUsedMemory();
        }
        return result;
    }

    void metadataUpdated(TableSpace tableSpace) {
        if (!tableSpace.uuid.equals(this.tableSpaceUUID)) {
            throw new IllegalArgumentException();
        }
        log.metadataUpdated(tableSpace.expectedReplicaCount);
        dataStorageManager.tableSpaceMetadataUpdated(tableSpace.uuid, tableSpace.expectedReplicaCount);
    }

    AbstractTableManager getTableManagerByUUID(String uuid) {
        //TODO: make this method more efficient
        for (AbstractTableManager manager : tables.values()) {
            if (manager.getTable().uuid.equals(uuid)) {
                return manager;
            }
        }
        throw new HerdDBInternalException("Cannot find tablemanager for " + uuid);
    }

    public Table[] collectChildrenTables(Table parentTable) {
        List<Table> list = new ArrayList<>();
        for (AbstractTableManager manager : tables.values()) {
            Table table = manager.getTable();
            if (table.isChildOfTable(parentTable.uuid)) {
                list.add(table);
            }
        }
        // selft reference
        if (parentTable.isChildOfTable(parentTable.uuid)) {
            list.add(parentTable);
        }
        return list.isEmpty() ? null : list.toArray(new Table[0]);
    }

    void rebuildForeignKeyReferences(Table table) {
        for (AbstractTableManager manager : tables.values()) {
            manager.rebuildForeignKeyReferences(table);
        }
    }

    private static class CheckpointFuture extends CompletableFuture {

        private final String tableName;

        public CheckpointFuture(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 31 * hash + Objects.hashCode(this.tableName);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final CheckpointFuture other = (CheckpointFuture) obj;
            return Objects.equals(this.tableName, other.tableName);
        }

    }

    void processAbandonedTransactions() {
        if (!leader) {
            return;
        }
        long now = System.currentTimeMillis();
        long timeout = dbmanager.getAbandonedTransactionsTimeout();
        if (timeout <= 0) {
            return;
        }
        long abandonedTransactionTimeout = now - timeout;
        for (Transaction t : transactions.values()) {
            if (t.isAbandoned(abandonedTransactionTimeout)) {
                LOGGER.log(Level.SEVERE, "forcing rollback of abandoned transaction {0},"
                                + " created locally at {1},"
                                + " last activity locally at {2}",
                        new Object[]{t.transactionId,
                                new java.sql.Timestamp(t.localCreationTimestamp),
                                new java.sql.Timestamp(t.lastActivityTs)});
                try {
                    if (!validateTransactionBeforeTxCommand(t.transactionId, false /* no wait */)) {
                        // Continue to check next transaction
                        continue;
                    }
                } catch (StatementExecutionException e) {
                    LOGGER.log(Level.SEVERE, "Failed to validate transaction {0}: {1}",
                            new Object[] { t.transactionId, e.getMessage() });
                    // Continue to check next transaction
                    continue;
                } catch (RuntimeException e) {
                    LOGGER.log(Level.SEVERE, "Failed to validate transaction {0}", new Object[] { t.transactionId, e });
                    // Continue to check next transaction
                    continue;
                }
                long lockStamp = acquireReadLock("forceRollback" + t.transactionId);
                try {
                    forceTransactionRollback(t.transactionId);
                } finally {
                    releaseReadLock(lockStamp, "forceRollback" + t.transactionId);
                }
            }
        }
    }

    public void restoreRawDumpedEntryLogs(List<DumpedLogEntry> entries) throws DataStorageManagerException, DDLException, EOFException {
        long lockStamp = acquireWriteLock("restoreRawDumpedEntryLogs");
        try {
            for (DumpedLogEntry ld : entries) {
                // these entries are a replay, like the ones of a boot, but they come from the dump and not from the
                // log of this tablespace: a dump that does not describe its own content cannot be answered with a
                // download, so the two are told apart
                apply(new CommitLogResult(ld.logSequenceNumber, false, false),
                        LogEntry.deserialize(ld.entryData), true, true);
            }
        } finally {
            releaseWriteLock(lockStamp, "restoreRawDumpedEntryLogs");
        }
    }

    /**
     * Declares on the commit log that the content of this tablespace is about to be replaced by a snapshot. The
     * restore streams the data straight into the storage, without writing anything to the log, so this marker and the
     * one written by {@link #restoreFinished()} are the only trace of it that a replica can read.
     *
     * @throws TableSpaceRestoreRefusedException if this tablespace holds tables of its own, in which case nothing at
     * all is written: see {@link #checkTableSpaceCanBeRestoredInto}
     */
    public void beginRestore() throws DataStorageManagerException, LogNotAvailableException {
        long lockStamp = acquireWriteLock("beginRestore");
        try {
            checkTableSpaceCanBeRestoredInto();
            restoreSourceLogSequenceNumber = LogSequenceNumber.START_OF_TIME;
            // opening the restore is the first thing it does, and it is what tells a restore this node is serving
            // from one it is only watching the leader run
            restoreDrivenByThisNode = true;
            restoreInProgress();
            // writing the marker is what puts this tablespace into "being restored": the state is set while the
            // entry is applied, so the marker cannot become durable without it
            writeRestoredFromSnapshotMarker(RestoredFromSnapshot.Phase.STARTED);
        } finally {
            releaseWriteLock(lockStamp, "beginRestore");
        }
    }

    /**
     * Refuses a restore aimed at a tablespace that holds tables of its own, before a single byte is written anywhere.
     * <p>
     * A restore replaces the whole content of a tablespace, and everything that is done with the markers it leaves on
     * the log rests on the tablespace having had no content before it: a marker met while replaying the log is read as
     * "the log does not describe what this tablespace holds", and the whole content is downloaded from the leader
     * again. That reading is true of the restore that {@link herddb.backup.BackupUtils} drives, which creates the
     * tablespace itself, and it is only true because of that {@code CREATE TABLESPACE}. Nothing forces a caller to go
     * through it: {@code HDBConnection.restoreTableSpace} is public API and streams into whatever tablespace it is
     * pointed at. A restore aimed at an existing, populated tablespace would open a restore on it, fail on the first
     * table it tried to create, and leave behind a log whose marker now claims that the data of that tablespace never
     * went through it, so that a node holding that data would throw it away and download it again from a leader that
     * does not have it either.
     * </p>
     * <p>
     * So the invariant is enforced here rather than assumed. The state that is asked for is the tables, and not the
     * position the local data is aligned to: the tables are what a tablespace holds, they are already in memory, and
     * the question is answered under the lock the restore is about to write under. The last checkpoint would answer a
     * different question and answer it wrongly in both directions, because a tablespace that has been written to and
     * has not checkpointed yet is still aligned to {@code START_OF_TIME} while holding every row it was given, and a
     * tablespace that is genuinely empty is above {@code START_OF_TIME} as soon as one periodic checkpoint has run.
     * </p>
     */
    private void checkTableSpaceCanBeRestoredInto() {
        List<String> existingTables = new ArrayList<>();
        for (AbstractTableManager tableManager : tables.values()) {
            if (!tableManager.isSystemTable()) {
                existingTables.add(tableManager.getTable().name);
            }
        }
        if (existingTables.isEmpty()) {
            return;
        }
        Collections.sort(existingTables);
        throw new TableSpaceRestoreRefusedException("Tablespace " + tableSpaceName + " on node " + nodeId
                + " already holds tables of its own " + existingTables + ", so it cannot be restored into: a restore"
                + " replaces the whole content of a tablespace and the tablespace it replaces has to be empty."
                + " Restore into a tablespace of its own, which is what a restore driven by BackupUtils creates for"
                + " itself, or drop this one first");
    }

    /**
     * Gives up on a restore nobody is driving any more, typically because the connection that was running it is gone.
     * <p>
     * This is called for every tablespace the connection had opened a restore on and not closed, which is exactly
     * the set of restores that did not run to the end, so it checks nothing else. In particular it does not ask
     * whether the marker that closes the restore was written: a last step that wrote that marker and then failed to
     * take the checkpoint that makes the restored content usable leaves the tablespace in the very state this method
     * is here to report, with a log that claims the restore is complete and a storage that knows nothing about it.
     * </p>
     * <p>
     * The inhibition of the checkpoints is deliberately <b>not</b> lifted: the tablespace holds a fragment of a
     * snapshot, and persisting it would move the checkpoint position above the marker that opened the restore, which
     * is exactly what has to be avoided. The tablespace manager is marked as failed instead, so that the activator
     * takes it out of service and boots it again: that boot replays the log from the last checkpoint and meets the
     * marker, which is what keeps the fragment from ever being served as the content of the tablespace. Nothing stays
     * inhibited behind: the state lives and dies with this tablespace manager, which is being thrown away.
     * </p>
     *
     * @param reason what made us conclude that the restore will never be completed, for the logs
     */
    public void abortRestore(String reason) {
        LOGGER.log(Level.SEVERE, "Restore of tablespace {0} on node {1} has been abandoned: {2}."
                + " The tablespace holds only a fragment of the snapshot and it is taken out of service: it does not"
                + " serve that fragment to anybody, and the restore can be run again",
                new Object[]{tableSpaceName, nodeId, reason});
        restoreDrivenByThisNode = false;
        setFailed();
    }

    public void beginRestoreTable(byte[] tableDef, LogSequenceNumber dumpLogSequenceNumber) {
        Table table = Table.deserialize(tableDef);
        long lockStamp = acquireWriteLock("beginRestoreTable " + table.name);
        try {
            if (tables.containsKey(table.name)) {
                throw new TableAlreadyExistsException(table.name);
            }
            if (dumpLogSequenceNumber != null && dumpLogSequenceNumber.after(restoreSourceLogSequenceNumber)) {
                // keep the most recent position among the dumped tables, it is the best description of
                // the snapshot this tablespace is being restored from
                restoreSourceLogSequenceNumber = dumpLogSequenceNumber;
            }
            bootTable(table, 0, dumpLogSequenceNumber, true);
        } finally {
            releaseWriteLock(lockStamp, "beginRestoreTable " + table.name);
        }
    }

    /**
     * Writes one of the two markers of a restore on the log and applies it, which is what records on this tablespace
     * manager that a restore is running or is over. Applying the marker is not a separate decision taken here, it is
     * the same code every other reader of the log goes through, so the marker and the state cannot disagree.
     */
    private void writeRestoredFromSnapshotMarker(RestoredFromSnapshot.Phase phase) throws DataStorageManagerException, LogNotAvailableException {
        RestoredFromSnapshot restore = new RestoredFromSnapshot(phase, tableSpaceName, tableSpaceUUID,
                restoreSourceLogSequenceNumber);
        LogEntry entry = LogEntryFactory.restoredFromSnapshot(restore);
        CommitLogResult pos = log.log(entry, true);
        try {
            apply(pos, entry, false);
        } catch (RuntimeException failure) {
            if (phase == RestoredFromSnapshot.Phase.STARTED) {
                // The marker may or may not be on the log. Most of the time it is not, and this is the write itself
                // failing; but a write that reached the storage and whose acknowledgement was lost on the way back
                // fails in exactly the same way, and from here the two are indistinguishable. The one that would hurt
                // is the second: the marker is on the log, every reader of that log is going to meet it, and this
                // tablespace manager would be the only one that does not know. So the restore is recorded as open,
                // which is what inhibits the checkpoints, and a restore recorded here for a marker that never made it
                // to the log costs this tablespace the boot it is about to be given anyway.
                restoreLastActivity = System.currentTimeMillis();
                restoreFromSnapshot = log.getLastSequenceNumber();
            }
            throw failure;
        }
    }

    public void restoreTableFinished(String table, List<Index> indexes) {
        // A step of a restore names a table the client chose, and the previous steps may well have failed: the
        // request that was supposed to create this table can have been answered with an error and the client can
        // have carried on anyway. Reporting it is what lets the connection peer answer the request the client is
        // waiting for; a NullPointerException or a ClassCastException escaping from here reaches no handler at all
        // and leaves the client waiting until its own socket timeout.
        AbstractTableManager manager = tables.get(table);
        if (!(manager instanceof TableManager)) {
            throw new StatementExecutionException("table " + table + " of tablespace " + tableSpaceName
                    + " is not being restored on node " + nodeId);
        }
        TableManager tableManager = (TableManager) manager;
        tableManager.restoreFinished();

        for (Index index : indexes) {
            bootIndex(index, tableManager, false, 0, true, true);
        }
    }

    public void restoreRawDumpedTransactions(List<Transaction> entries) {
        for (Transaction ld : entries) {
            LOGGER.log(Level.INFO, "restore transaction " + ld);
            transactions.put(ld.transactionId, ld);
        }
    }

    void dumpTableSpace(String dumpId, Channel channel, int fetchSize, boolean includeLog) throws DataStorageManagerException, LogNotAvailableException {

        LOGGER.log(Level.INFO, "dumpTableSpace dumpId:{0} channel {1} fetchSize:{2}, includeLog:{3}", new Object[]{dumpId, channel, fetchSize, includeLog});

        List<DumpedLogEntry> txlogentries = new CopyOnWriteArrayList<>();
        CommitLogListener logDumpReceiver = new CommitLogListener() {
            @Override
            public void logEntry(LogSequenceNumber logPos, LogEntry data) {
                // we are going to capture all the changes to the tablespace during the dump, in order to replay
                // eventually 'missed' changes during the dump
                txlogentries.add(new DumpedLogEntry(logPos, data.serialize()));
                //LOGGER.log(Level.SEVERE, "dumping entry " + logPos + ", " + data + " nentries: " + txlogentries.size());
            }
        };

        // Everything below runs while the tablespace is locked and, when the log is part of the dump, while a
        // listener is attached to the commit log. Both have to be given back on every way out of this method, the
        // ones that are not the happy path included: a lock stamp that is never released blocks every later write,
        // DDL, checkpoint and follower of this tablespace for as long as the process lives, and a listener that is
        // never detached keeps a copy of every entry that goes through the log.
        long lockStamp = acquireWriteLock(null);
        boolean downgradedToReadLock = false;
        try {
            if (includeLog) {
                log.attachCommitLogListener(logDumpReceiver);
            }
            try {
                TableSpaceCheckpoint checkpoint = checkpoint(true /* compact records*/, true, true /* already locked */);
                LOGGER.log(Level.INFO, "Created checkpoint at {}", checkpoint);
                if (checkpoint == null) {
                    // a checkpoint is skipped rather than taken for a few legitimate reasons, a restore that is
                    // replacing the content of this very tablespace being one of them
                    throw new DataStorageManagerException("failed to create a checkpoint, check logs for the reason");
                }
                try {
                    /* Downgrade lock */
                    long readLockStamp = generalLock.tryConvertToReadLock(lockStamp);
                    if (readLockStamp == 0) {
                        // the write lock is still held and the stamp is still the one we have to release
                        throw new DataStorageManagerException("unable to downgrade lock");
                    }
                    lockStamp = readLockStamp;
                    downgradedToReadLock = true;
                    sendDump(dumpId, channel, fetchSize, includeLog, checkpoint, txlogentries);
                } finally {
                    unPinCheckpointOfDumpedTables(checkpoint);
                }
            } finally {
                if (includeLog) {
                    log.removeCommitLogListener(logDumpReceiver);
                }
            }
        } finally {
            if (downgradedToReadLock) {
                releaseReadLock(lockStamp, "senddump");
            } else {
                releaseWriteLock(lockStamp, "senddump");
            }
        }

    }

    /**
     * Streams the content of a checkpoint of this tablespace to a client. It runs with the tablespace locked for read
     * and, when the log is part of the dump, with a listener attached to the commit log by the caller.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE")
    private void sendDump(
            String dumpId, Channel channel, int fetchSize, boolean includeLog,
            TableSpaceCheckpoint checkpoint, List<DumpedLogEntry> txlogentries
    ) throws DataStorageManagerException, LogNotAvailableException {
        try {
            final int timeout = 60000;
            LogSequenceNumber checkpointSequenceNumber = checkpoint.sequenceNumber;

            long id = channel.generateRequestId();
            LOGGER.log(Level.INFO, "start sending dump, dumpId: {0} to client {1}", new Object[]{dumpId, channel});
            try (Pdu response_to_start = channel.sendMessageWithPduReply(id, PduCodec.TablespaceDumpData.write(
                    id, tableSpaceName, dumpId, "start", null, stats.getTablesize(), checkpointSequenceNumber.ledgerId, checkpointSequenceNumber.offset, null, null), timeout)) {
                if (response_to_start.type != Pdu.TYPE_ACK) {
                    LOGGER.log(Level.SEVERE, "error response at start command");
                    return;
                }
            }

            if (includeLog) {
                List<Transaction> transactionsSnapshot = new ArrayList<>();
                dataStorageManager.loadTransactions(checkpointSequenceNumber, tableSpaceUUID, transactionsSnapshot::add);
                List<Transaction> batch = new ArrayList<>();
                for (Transaction t : transactionsSnapshot) {
                    batch.add(t);
                    if (batch.size() == 10) {
                        sendTransactionsDump(batch, channel, dumpId, timeout);
                    }
                }
                sendTransactionsDump(batch, channel, dumpId, timeout);
            }

            for (Entry<String, LogSequenceNumber> entry : checkpoint.tablesCheckpoints.entrySet()) {
                final AbstractTableManager tableManager = tables.get(entry.getKey());
                final LogSequenceNumber sequenceNumber = entry.getValue();
                if (tableManager.isSystemTable()) {
                    continue;
                }
                try {
                    LOGGER.log(Level.INFO, "Sending table checkpoint for {} took at sequence number {}", new Object[]{tableManager.getTable().name, sequenceNumber});
                    FullTableScanConsumer sink = new SingleTableDumper(tableSpaceName, tableManager, channel, dumpId, timeout, fetchSize);
                    tableManager.dump(sequenceNumber, sink);
                } catch (DataStorageManagerException err) {
                    LOGGER.log(Level.SEVERE, "error sending dump id " + dumpId, err);
                    sendDumpFailed(tableSpaceName, dumpId, channel, err);
                    return;
                }
            }

            if (!txlogentries.isEmpty()) {
                txlogentries.sort(Comparator.naturalOrder());
                sendDumpedCommitLog(txlogentries, channel, dumpId, timeout);
            }

            LogSequenceNumber finishLogSequenceNumber = log.getLastSequenceNumber();
            channel.sendOneWayMessage(PduCodec.TablespaceDumpData.write(
                    id, tableSpaceName, dumpId, "finish", null, 0,
                    finishLogSequenceNumber.ledgerId, finishLogSequenceNumber.offset,
                    null, null), (Throwable error) -> {
                        if (error != null) {
                            LOGGER.log(Level.SEVERE, "Cannot send last dump msg for " + dumpId, error);
                        } else {
                            LOGGER.log(Level.INFO, "Sent last dump msg for " + dumpId);
                        }
            });
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException("interrupted while sending dump id " + dumpId, error);
        } catch (TimeoutException error) {
            // The receiver of a dump is told nothing by a stream that simply stops: it goes on waiting for the next
            // chunk of a dump nobody is sending any more. The caller answers for that, this only has to report.
            throw new DataStorageManagerException("timed out while sending dump id " + dumpId, error);
        }
    }

    /**
     * Tells the receiver of a dump that the dump will not be completed, so that it stops waiting for the rest of it.
     * The stream of a dump only ever goes one way and the request that started it was acknowledged long before, so
     * this message is the only thing that can reach the receiver.
     * <p>
     * The answer to it is waited for asynchronously and dropped. Nothing here has any use for it, the caller is on
     * its way out of a dump that failed and has no time to give to this; but a receiver does answer this message, and
     * a reply nobody is expecting is a reply nobody releases either.
     * </p>
     * <p>
     * A channel that is already gone is checked for first, and it is the commonest case of all: the receiver having
     * disappeared is the ordinary reason a dump dies. The check saves building a message for a peer that is known
     * not to be there, and puts the reason in the log, where a send that simply failed would leave it to be guessed.
     * It cannot do more than that: the channel can go away between the check and the call, so sending has to be
     * safe on its own in any case.
     * </p>
     */
    static void sendDumpFailed(String tableSpaceName, String dumpId, Channel channel, Throwable error) {
        if (!channel.isValid()) {
            LOGGER.log(Level.INFO, "Not telling the receiver of dump {0} of tablespace {1} that the dump failed: the"
                    + " channel is gone, which is very likely why it failed", new Object[]{dumpId, tableSpaceName});
            return;
        }
        long id = channel.generateRequestId();
        channel.sendRequestWithAsyncReply(id, PduCodec.TablespaceDumpData.writeError(
                id, tableSpaceName, dumpId, String.valueOf(error)), DUMP_FAILED_TIMEOUT,
                (Pdu reply, Throwable sendFailure) -> {
                    if (sendFailure != null) {
                        LOGGER.log(Level.SEVERE, "Cannot tell the receiver of dump " + dumpId + " that the dump"
                                + " failed, it is left waiting for data that is not coming", sendFailure);
                        return;
                    }
                    reply.close();
                });
    }

    /**
     * Releases the pins the checkpoint taken for a dump put on the files of the dumped tables. The dump asks for a
     * pinned checkpoint so that nothing deletes those files while they are being sent, so the pins have to be
     * released whatever happens to the dump.
     */
    private void unPinCheckpointOfDumpedTables(TableSpaceCheckpoint checkpoint) {
        for (Entry<String, LogSequenceNumber> entry : checkpoint.tablesCheckpoints.entrySet()) {
            String tableName = entry.getKey();
            AbstractTableManager tableManager = tables.get(tableName);
            String tableUUID = tableManager.getTable().uuid;
            LogSequenceNumber seqNumber = entry.getValue();
            LOGGER.log(Level.INFO, "unPinTableCheckpoint {0}.{1} ({2}) {3}", new Object[]{tableSpaceUUID, tableName, tableUUID, seqNumber});
            dataStorageManager.unPinTableCheckpoint(tableSpaceUUID, tableUUID, seqNumber);
        }
    }

    private void sendTransactionsDump(List<Transaction> batch, Channel channel, String dumpId, final int timeout) throws TimeoutException, InterruptedException {
        if (batch.isEmpty()) {
            return;
        }
        List<KeyValue> encodedTransactions = batch
                .stream()
                .map(tr -> {
                    return new KeyValue(Bytes.from_long(tr.transactionId), Bytes.from_array(tr.serialize()));
                })
                .collect(Collectors.toList());
        long id = channel.generateRequestId();
        try (Pdu response_to_transactionsData = channel.sendMessageWithPduReply(id, PduCodec.TablespaceDumpData.write(
                id, tableSpaceName, dumpId, "transactions", null, 0,
                0, 0,
                null, encodedTransactions), timeout)) {
            if (response_to_transactionsData.type != Pdu.TYPE_ACK) {
                LOGGER.log(Level.SEVERE, "error response at transactionsData command");
            }
        }
        batch.clear();
    }

    private void sendDumpedCommitLog(List<DumpedLogEntry> txlogentries, Channel channel, String dumpId, final int timeout) throws TimeoutException, InterruptedException {
        List<KeyValue> batch = new ArrayList<>();
        for (DumpedLogEntry e : txlogentries) {
            batch.add(new KeyValue(Bytes.from_array(e.logSequenceNumber.serialize()),
                    Bytes.from_array(e.entryData)));
        }
        long id = channel.generateRequestId();
        try (Pdu response_to_txlog = channel.sendMessageWithPduReply(id, PduCodec.TablespaceDumpData.write(
                id, tableSpaceName, dumpId, "txlog", null, 0,
                0, 0,
                null, batch), timeout)) {

            if (response_to_txlog.type != Pdu.TYPE_ACK) {
                LOGGER.log(Level.SEVERE, "error response at txlog command");
            }
        }

    }

    public void restoreFinished() throws DataStorageManagerException, LogNotAvailableException {
        LOGGER.log(Level.INFO, "restore finished of tableSpace " + tableSpaceName + ". requesting checkpoint");
        transactions.clear();
        // The marker goes on the log before the checkpoint: the checkpoint is what makes the restored data usable, so
        // a log that ends between the two describes a restore that did not complete. Writing the marker is also what
        // lifts the inhibition of the checkpoints, so the checkpoint below, the one that makes the restored content
        // usable, is the first one that can run since the restore began.
        long lockStamp = acquireWriteLock("restoreFinished");
        try {
            writeRestoredFromSnapshotMarker(RestoredFromSnapshot.Phase.FINISHED);
        } finally {
            releaseWriteLock(lockStamp, "restoreFinished");
        }
        restoreSourceLogSequenceNumber = LogSequenceNumber.START_OF_TIME;
        if (checkpoint(false, false, false) == null) {
            // The checkpoint is what makes the restored content usable: it is the only thing that writes the tables
            // and the records the restore streamed into memory, and it is what moves the position the tablespace is
            // aligned to above the marker that was just written. A checkpoint that did not run leaves a log that
            // claims the restore is complete and a storage that knows nothing about it, which is exactly the state
            // the next boot refuses. The client must not be told that the restore succeeded.
            throw new DataStorageManagerException("Restore of tablespace " + tableSpaceName + " on node " + nodeId
                    + " declared itself complete on the log, but the checkpoint that persists the restored content"
                    + " was skipped, so nothing of the restored data reached the storage");
        }
    }

    public boolean isVirtual() {
        return virtual;
    }

    private class FollowerThread implements Runnable {

        private volatile CountDownLatch running = new CountDownLatch(1);

        @Override
        public String toString() {
            return "FollowerThread{" + tableSpaceName + '}';
        }

        @Override
        public void run() {
            try (CommitLog.FollowerContext context = log.startFollowing(actualLogSequenceNumber)) {
                // isFailed() is part of the question, in the loop as much as in the acceptor: an entry can take the
                // tablespace manager out of service without throwing anything, the marker that says the leader has
                // replaced the content of the tablespace being the one that does. Everything that comes after it
                // describes a content this node does not hold, and when the restored tablespace has the same schema
                // as the one it replaced every table name still resolves, so those entries apply cleanly on top of
                // obsolete rows and whoever reads this replica sees a mix of the two. Leaving the loop out of the
                // check would only postpone that by one round: the acceptor stops the batch it is in and the loop
                // immediately asks for the next one.
                while (!isLeader() && !closed && !isFailed()) {
                    long readLock = acquireReadLock("follow");
                    try {
                        log.followTheLeader(actualLogSequenceNumber, (LogSequenceNumber num, LogEntry u) -> {
                            if (isLeader() || closed || isFailed()) {
                                // asked before applying and not only afterwards: the answer this acceptor gives
                                // stops the log from handing over more entries, but a log that has already read a
                                // batch of them may well deliver the rest of that batch anyway. Refusing to apply
                                // is the only thing that holds in every case
                                return false;
                            }
                            try {
                                apply(new CommitLogResult(num, false, true), u, false);
                            } catch (Throwable t) {
                                throw new RuntimeException(t);
                            }
                            return !isLeader() && !closed && !isFailed();
                        }, context);
                    } finally {
                        releaseReadLock(readLock, "follow");
                    }
                }
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "follower error " + tableSpaceName, t);
                setFailed();
            } finally {
                running.countDown();
            }
        }

        void waitForStop() throws InterruptedException {
            LOGGER.log(Level.INFO, "Waiting for FollowerThread of {0} to stop", tableSpaceName);
            running.await();
            LOGGER.log(Level.INFO, "FollowerThread of {0} stopped", tableSpaceName);
        }
    }

    void setFailed() {
        failed = true;
    }

    public boolean isFailed() {
        if (virtual) {
            return false;
        }
        return failed || log.isFailed();
    }

    private void startAsFollower() throws DataStorageManagerException, LogNotAvailableException {
        if (dbmanager.getMode().equals(ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER)) {
            // in diskless cluster mode there is no need to really 'follow' the leader
        } else {
            followerThread = new FollowerThread();
            dbmanager.submit(followerThread);
        }
    }

    private void startAsLeader(int expectedReplicaCount) throws DataStorageManagerException, DDLException, LogNotAvailableException {
        if (virtual) {

        } else {

            LOGGER.log(Level.INFO, "startAsLeader {0} tablespace {1}", new Object[]{nodeId, tableSpaceName});
            recoverForLeadership();

            // every pending transaction MUST be rollback back
            List<Long> pending_transactions = new ArrayList<>(this.transactions.keySet());
            log.startWriting(expectedReplicaCount);
            LOGGER.log(Level.INFO, "startAsLeader {0} tablespace {1} log, there were {2} pending transactions to be rolledback", new Object[]{nodeId, tableSpaceName, pending_transactions.size()});
            for (long tx : pending_transactions) {
                forceTransactionRollback(tx);
            }
        }
        leader = true;
    }

    private void forceTransactionRollback(long tx) throws LogNotAvailableException, DataStorageManagerException, DDLException {
        LOGGER.log(Level.FINER, "rolling back transaction {0}", tx);
        LogEntry rollback = LogEntryFactory.rollbackTransaction(tx);
        // let followers see the rollback on the log
        CommitLogResult pos = log.log(rollback, true);
        apply(pos, rollback, false);
    }

    private final ConcurrentHashMap<Long, Transaction> transactions = new ConcurrentHashMap<>();

    public StatementExecutionResult executeStatement(Statement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        CompletableFuture<StatementExecutionResult> res = executeStatementAsync(statement, context, transactionContext);
        try {
            return res.get();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new StatementExecutionException(err);
        } catch (ExecutionException err) {
            Throwable cause = err.getCause();
            if (cause instanceof StatementExecutionException) {
                throw (StatementExecutionException) cause;
            } else {
                throw new StatementExecutionException(cause);
            }
        } catch (Throwable t) {
            throw new StatementExecutionException(t);
        }
    }

    public CompletableFuture<StatementExecutionResult> executeStatementAsync(
            Statement statement, StatementEvaluationContext context,
            TransactionContext transactionContext
    ) throws StatementExecutionException {

        if (transactionContext.transactionId == TransactionContext.AUTOTRANSACTION_ID
                && statement.supportsTransactionAutoCreate() // Do not autostart transaction on alter table statements
        ) {
            AtomicLong capturedTx = new AtomicLong();
            boolean wasHoldingTableSpaceLock = context.getTableSpaceLock() != 0;
            CompletableFuture<StatementExecutionResult> newTransaction = beginTransactionAsync(context, false);
            CompletableFuture<StatementExecutionResult> finalResult = newTransaction
                    .thenCompose((StatementExecutionResult begineTransactionResult) -> {
                        TransactionContext newtransactionContext = new TransactionContext(begineTransactionResult.transactionId);
                        capturedTx.set(newtransactionContext.transactionId);
                        return executeStatementAsyncInternal(statement, context, newtransactionContext, true);
                    });
            finalResult.whenComplete((xx, error) -> {
                if (!wasHoldingTableSpaceLock) {
                    releaseReadLock(context.getTableSpaceLock(), "begin implicit transaction");
                }
                long txId = capturedTx.get();
                if (error != null && txId > 0) {
                    LOGGER.log(Level.FINE, tableSpaceName + " force rollback of implicit transaction " + txId, error);
                    try {
                        rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, txId), context)
                                .get(); // block until rollback is complete
                    } catch (InterruptedException ex) {
                        LOGGER.log(Level.SEVERE, tableSpaceName + " Cannot rollback implicit tx " + txId, ex);
                        Thread.currentThread().interrupt();
                        error.addSuppressed(ex);
                    } catch (ExecutionException ex) {
                        LOGGER.log(Level.SEVERE, tableSpaceName + " Cannot rollback implicit tx " + txId, ex.getCause());
                        error.addSuppressed(ex.getCause());
                    } catch (Throwable t) {
                        LOGGER.log(Level.SEVERE, tableSpaceName + " Cannot rollback  implicittx " + txId, t);
                        error.addSuppressed(t);
                    }
                }
            });
            return finalResult;
        } else {
            return executeStatementAsyncInternal(statement, context, transactionContext, false);
        }
    }

    private CompletableFuture<StatementExecutionResult> executeStatementAsyncInternal(
            Statement statement, StatementEvaluationContext context,
            TransactionContext transactionContext, boolean rollbackOnError
    ) throws StatementExecutionException {
        Transaction transaction = transactions.get(transactionContext.transactionId);
        if (transaction != null
                && !transaction.tableSpace.equals(tableSpaceName)) {
            return Futures.exception(
                    new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName));
        }
        if (transactionContext.transactionId > 0
                && transaction == null) {
            return Futures.exception(
                    new StatementExecutionException("transaction " + transactionContext.transactionId + " not found on tablespace " + tableSpaceName));
        }
        boolean isTransactionCommand = statement instanceof CommitTransactionStatement
                || statement instanceof RollbackTransactionStatement
                || statement instanceof AlterTableStatement; // AlterTable implictly commits the transaction
        if (transaction != null) {
            transaction.touch();
            if (!isTransactionCommand) {
                transaction.increaseRefcount();
            }
        }
        CompletableFuture<StatementExecutionResult> res;
        try {
            if (statement instanceof TableAwareStatement) {
                res = executeTableAwareStatement(statement, transaction, context);
            } else if (statement instanceof SQLPlannedOperationStatement) {
                res = executePlannedOperationStatement(statement, transactionContext, context);
            } else if (statement instanceof BeginTransactionStatement) {
                if (transaction != null) {
                    res = Futures.exception(new StatementExecutionException("transaction already started"));
                } else {
                    res = beginTransactionAsync(context, true);
                }
            } else if (statement instanceof CommitTransactionStatement) {
                res = commitTransaction((CommitTransactionStatement) statement, context);
            } else if (statement instanceof RollbackTransactionStatement) {
                res = rollbackTransaction((RollbackTransactionStatement) statement, context);
            } else if (statement instanceof CreateTableStatement) {
                res = CompletableFuture.completedFuture(createTable((CreateTableStatement) statement, transaction, context));
            } else if (statement instanceof CreateIndexStatement) {
                res = CompletableFuture.completedFuture(createIndex((CreateIndexStatement) statement, transaction, context));
            } else if (statement instanceof DropTableStatement) {
                res = CompletableFuture.completedFuture(dropTable((DropTableStatement) statement, transaction, context));
            } else if (statement instanceof DropIndexStatement) {
                res = CompletableFuture.completedFuture(dropIndex((DropIndexStatement) statement, transaction, context));
            } else if (statement instanceof AlterTableStatement) {
                res = CompletableFuture.completedFuture(alterTable((AlterTableStatement) statement, transactionContext, context));
            } else {
                res = Futures.exception(new StatementExecutionException("unsupported statement " + statement)
                        .fillInStackTrace());
            }
        } catch (StatementExecutionException error) {
            res = Futures.exception(error);
        }
        if (transaction != null && !isTransactionCommand) {
            res = res.whenComplete((a, b) -> {
                transaction.decreaseRefCount();
            });
        }
        if (rollbackOnError) {
            long txId = transactionContext.transactionId;
            if (txId > 0) {
                res = res.whenComplete((xx, error) -> {
                    if (error != null) {
                        LOGGER.log(Level.FINE, tableSpaceName + " force rollback of implicit transaction " + txId, error);
                        try {
                            rollbackTransaction(new RollbackTransactionStatement(tableSpaceName, txId), context)
                                    .get(); // block until operation completes
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            error.addSuppressed(ex);
                        } catch (ExecutionException ex) {
                            error.addSuppressed(ex.getCause());
                        }
                        throw new HerdDBInternalException(error);
                    }
                });
            }
        }
        return res;
    }

    private CompletableFuture<StatementExecutionResult> executePlannedOperationStatement(
            Statement statement,
            TransactionContext transactionContext, StatementEvaluationContext context
    ) {
        long lockStamp = context.getTableSpaceLock();
        boolean lockAcquired = false;
        if (lockStamp == 0) {
            lockStamp = acquireReadLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }

        SQLPlannedOperationStatement planned = (SQLPlannedOperationStatement) statement;
        CompletableFuture<StatementExecutionResult> res;
        try {
            res = planned.getRootOp().executeAsync(this, transactionContext, context, false, false);
        } catch (HerdDBInternalException err) {
            // ensure we are able to release locks correctly
            LOGGER.log(Level.SEVERE, "Internal error", err);
            res = Futures.exception(err);
        }
//        res.whenComplete((ee, err) -> {
//            LOGGER.log(Level.SEVERE, "COMPLETED " + statement + ": " + ee, err);
//        });
        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .thenApply(s -> {
                        context.setTableSpaceLock(0);
                        return s;
                    });
        }
        return res;
    }

    private CompletableFuture<StatementExecutionResult> executeTableAwareStatement(Statement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        long lockStamp = context.getTableSpaceLock();
        boolean lockAcquired = false;
        if (lockStamp == 0) {
            lockStamp = acquireReadLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        TableAwareStatement st = (TableAwareStatement) statement;
        String table = st.getTable();
        AbstractTableManager manager = tables.get(table);
        CompletableFuture<StatementExecutionResult> res;
        if (manager == null) {
            res = Futures.exception(new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName));
        } else if (manager.getCreatedInTransaction() > 0
                && (transaction == null || transaction.transactionId != manager.getCreatedInTransaction())) {
            res = Futures.exception(new TableDoesNotExistException("no table " + table + " in tablespace " + tableSpaceName + ". created temporary in transaction " + manager.getCreatedInTransaction()));
        } else {
            res = manager.executeStatementAsync(statement, transaction, context);
        }
        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .whenComplete((s, err) -> {
                        context.setTableSpaceLock(0);
                    });
        }
        return res;

    }

    private long acquireReadLock(Object statement) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "{0} rlock {1}", new Object[]{tableSpaceName, statement});
        }
        long lockStamp = generalLock.readLock();
//        LOGGER.log(Level.SEVERE, "ACQUIRED READLOCK for " + statement + ", " + generalLock);
        return lockStamp;
    }

    private long acquireWriteLock(Object statement) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "{0} wlock {1}", new Object[]{tableSpaceName, statement});
        }
//        LOGGER.log(Level.SEVERE, "ACQUIRINGTS WRITELOCK for " + statement + ", " + generalLock);

        long lockStamp = generalLock.writeLock();
//        LOGGER.log(Level.SEVERE, "ACQUIRED WRITELOCK for " + statement + " -> " + lockStamp + ", " + generalLock);
        return lockStamp;
    }

    private StatementExecutionResult alterTable(AlterTableStatement statement, TransactionContext transactionContext, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            if (transactionContext.transactionId > 0) {
                Transaction transaction = transactions.get(transactionContext.transactionId);
                if (transactionContext.transactionId > 0 && transaction == null) {
                    throw new StatementExecutionException("transaction " + transactionContext.transactionId + " does not exist on tablespace " + tableSpaceName);
                }
                if (transaction != null && !transaction.tableSpace.equals(tableSpaceName)) {
                    throw new StatementExecutionException("transaction " + transaction.transactionId + " is for tablespace " + transaction.tableSpace + ", not for " + tableSpaceName);
                }
                LOGGER.log(Level.INFO, "Implicitly committing transaction " + transactionContext.transactionId + " due to an ALTER TABLE statement in tablespace " + tableSpaceName);
                try {
                    commitTransaction(new CommitTransactionStatement(tableSpaceName, transactionContext.transactionId), context).join();
                } catch (CompletionException err) {
                    throw new StatementExecutionException(err);
                }
                transactionContext = TransactionContext.NO_TRANSACTION;
            }
            AbstractTableManager tableManager = tables.get(statement.getTable());
            if (tableManager == null) {
                throw new TableDoesNotExistException("no table " + statement.getTable() + " in tablespace " + tableSpaceName + ","
                        + " only " + tables.keySet());
            }

            Table oldTable = tableManager.getTable();
            Table[] childrenTables = collectChildrenTables(oldTable);
            if (childrenTables != null) {
                for (Table child : childrenTables) {
                    for (String col : statement.getDropColumns()) {
                        for (ForeignKeyDef fk : child.foreignKeys) {
                            if (fk.parentTableId.equals(oldTable.uuid)) {
                                if (Stream
                                        .of(fk.parentTableColumns)
                                        .anyMatch(c -> c.equalsIgnoreCase(col))) {
                                    throw new StatementExecutionException(
                                            "Cannot drop column " + oldTable.name + "." + col + " because of foreign key constraint " + fk.name + " on table " + child.name);
                                }
                            }
                        }
                    }
                }
            }

            Table newTable;
            try {
                newTable = tableManager.getTable().applyAlterTable(statement);
            } catch (IllegalArgumentException error) {
                throw new StatementExecutionException(error);
            }
            validateAlterTable(newTable, context);
            LogEntry entry = LogEntryFactory.alterTable(newTable, null);
            try {
                CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                apply(pos, entry, false);
            } catch (Exception err) {
                throw new StatementExecutionException(err);
            }
            // Here transactionId is always 0, because transaction is implicitly committed
            return new DDLStatementExecutionResult(transactionContext.transactionId);
        } finally {
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }

    }

    private StatementExecutionResult createTable(CreateTableStatement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            if (tables.containsKey(statement.getTableDefinition().name)) {
                if (statement.isIfExistsClause()) {
                    return new DDLStatementExecutionResult(
                            transaction != null ? transaction.transactionId : 0);
                }
                throw new TableAlreadyExistsException(statement.getTableDefinition().name);
            }
            for (Index additionalIndex : statement.getAdditionalIndexes()) {
                AbstractIndexManager exists = indexes.get(additionalIndex.name);
                if (exists != null) {
                    LOGGER.log(Level.INFO, "Error while creating index " + additionalIndex.name + ", there is already an index " + exists.getIndex().name + " on table " + exists.getIndex().table);
                    throw new IndexAlreadyExistsException(additionalIndex.name);
                }
            }
            Table table = statement.getTableDefinition();
            // validate foreign keys
            if (table.foreignKeys != null) {
                for (ForeignKeyDef def: table.foreignKeys) {
                    AbstractTableManager parentTableManager = null;
                    for (AbstractTableManager ab : tables.values()) {
                        if (ab.getTable().uuid.equals(def.parentTableId)) {
                            parentTableManager = ab;
                            break;
                        }
                    }
                    if (parentTableManager == null) {
                        throw new StatementExecutionException("Table " + def.parentTableId + " does not exist in tablespace " + tableSpaceName);
                    }
                    Table parentTable = parentTableManager.getTable();
                    int i = 0;
                    for (String col : def.columns) {
                        Column column = table.getColumn(col);
                        Column parentColumn = parentTable.getColumn(def.parentTableColumns[i]);
                        if (column == null) {
                            throw new StatementExecutionException("Cannot find column " + col);
                        }
                        if (parentColumn == null) {
                            throw new StatementExecutionException("Cannot find column " + def.parentTableColumns[i]);
                        }
                        if (!ColumnTypes.sameRawDataType(column.type, parentColumn.type)) {
                            throw new StatementExecutionException("Column " + table.name + "." + column.name + " is not the same tyepe of column " + parentTable.name + "." + parentColumn.name);
                        }
                        i++;
                    }
                }
            }
            LogEntry entry = LogEntryFactory.createTable(statement.getTableDefinition(), transaction);
            CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);

            for (Index additionalIndex : statement.getAdditionalIndexes()) {
                LogEntry index_entry = LogEntryFactory.createIndex(additionalIndex, transaction);
                CommitLogResult index_pos = log.log(index_entry, index_entry.transactionId <= 0);
                apply(index_pos, index_entry, false);
            }

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException | LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }
    }

    private StatementExecutionResult createIndex(CreateIndexStatement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            AbstractIndexManager exists = indexes.get(statement.getIndexDefinition().name);
            if (exists != null) {
                LOGGER.log(Level.INFO, "Error while creating index " + statement.getIndexDefinition().name
                        + ", there is already an index " + exists.getIndex().name + " on table " + exists.getIndex().table);
                throw new IndexAlreadyExistsException(statement.getIndexDefinition().name);
            }
            LogEntry entry = LogEntryFactory.createIndex(statement.getIndexDefinition(), transaction);
            CommitLogResult pos;
            try {
                pos = log.log(entry, entry.transactionId <= 0);
            } catch (LogNotAvailableException ex) {
                throw new StatementExecutionException(ex);
            }

            apply(pos, entry, false);

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }
    }

    private StatementExecutionResult dropTable(DropTableStatement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            String tableNameUpperCase = statement.getTable().toUpperCase();
            String tableNameNormalized = tables.keySet()
                    .stream()
                    .filter(t -> t.toUpperCase().equals(tableNameUpperCase))
                    .findFirst()
                    .orElse(statement.getTable());
            AbstractTableManager tableManager = tables.get(tableNameNormalized);
            if (tableManager == null) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction != null ? transaction.transactionId : 0);
                }
                throw new TableDoesNotExistException("table does not exist " + tableNameNormalized + " on tableSpace " + statement.getTableSpace());
            }
            if (transaction != null && transaction.isTableDropped(tableNameNormalized)) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction.transactionId);
                }
                throw new TableDoesNotExistException("table does not exist " + tableNameNormalized + " on tableSpace " + statement.getTableSpace());
            }
            Table table = tableManager.getTable();
            Table[] childrenTables = collectChildrenTables(table);
            if (childrenTables != null) {
                String errorMsg = "Cannot drop table " + table.tablespace + "." + table.name
                        + " because it has children tables: "
                        + Stream.of(childrenTables).map(t -> t.name).collect(Collectors.joining(","));
                throw new StatementExecutionException(errorMsg);
            }
            Map<String, AbstractIndexManager> indexesOnTable = indexesByTable.get(tableNameNormalized);
            if (indexesOnTable != null) {
                for (String index : new ArrayList<>(indexesOnTable.keySet())) {
                    LogEntry entry = LogEntryFactory.dropIndex(index, transaction);
                    CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
                    apply(pos, entry, false);
                }
            }

            LogEntry entry = LogEntryFactory.dropTable(tableNameNormalized, transaction);
            CommitLogResult pos = log.log(entry, entry.transactionId <= 0);
            apply(pos, entry, false);

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException | LogNotAvailableException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }
    }

    private StatementExecutionResult dropIndex(DropIndexStatement statement, Transaction transaction, StatementEvaluationContext context) throws StatementExecutionException {
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireWriteLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            if (!indexes.containsKey(statement.getIndexName())) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction != null ? transaction.transactionId : 0);
                }
                throw new IndexDoesNotExistException("index " + statement.getIndexName() + " does not exist " + statement.getIndexName() + " on tableSpace " + statement.getTableSpace());
            }
            if (transaction != null && transaction.isIndexDropped(statement.getIndexName())) {
                if (statement.isIfExists()) {
                    return new DDLStatementExecutionResult(transaction.transactionId);
                }
                throw new IndexDoesNotExistException("index does not exist " + statement.getIndexName() + " on tableSpace " + statement.getTableSpace());
            }
            LogEntry entry = LogEntryFactory.dropIndex(statement.getIndexName(), transaction);
            CommitLogResult pos;
            try {
                pos = log.log(entry, entry.transactionId <= 0);
            } catch (LogNotAvailableException ex) {
                throw new StatementExecutionException(ex);
            }

            apply(pos, entry, false);

            return new DDLStatementExecutionResult(entry.transactionId);
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException(err);
        } finally {
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), statement);
                context.setTableSpaceLock(0);
            }
        }
    }

    TableManager bootTable(Table table, long transaction, LogSequenceNumber dumpLogSequenceNumber, boolean freshNew) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        if (!freshNew) {
            LOGGER.log(Level.INFO, "bootTable {0} {1}.{2}", new Object[]{nodeId, tableSpaceName, table.name});
        }
        AbstractTableManager prevTableManager = tables.remove(table.name);
        if (prevTableManager != null) {
            if (dumpLogSequenceNumber != null) {
                // restoring a table already booted in a previous life
                LOGGER.log(Level.INFO, "bootTable {0} {1}.{2} already exists on this tablespace. It will be truncated", new Object[]{nodeId, tableSpaceName, table.name});
                prevTableManager.dropTableData();
            } else {
                LOGGER.log(Level.INFO, "bootTable {0} {1}.{2} already exists on this tablespace", new Object[]{nodeId, tableSpaceName, table.name});
                throw new DataStorageManagerException("Table " + table.name + " already present in tableSpace " + tableSpaceName);
            }
        }
        TableManager tableManager = new TableManager(
                table, log, dbmanager.getMemoryManager(), dataStorageManager, this, tableSpaceUUID, transaction);
        if (dbmanager.getServerConfiguration().getBoolean(
                ServerConfiguration.PROPERTY_JMX_ENABLE, ServerConfiguration.PROPERTY_JMX_ENABLE_DEFAULT)) {
            JMXUtils.registerTableManagerStatsMXBean(tableSpaceName, table.name, tableManager.getStats());
        }

        if (dumpLogSequenceNumber != null) {
            tableManager.prepareForRestore(dumpLogSequenceNumber);
        }
        tables.put(table.name, tableManager);
        tableManager.start(freshNew);
        if (!freshNew) {
            LOGGER.log(Level.INFO, "bootTable {0} {1}.{2} time {3} ms", new Object[]{nodeId, tableSpaceName, table.name, (System.currentTimeMillis() - _start) + ""});
        }
        dbmanager.getPlanner().clearCache();
        return tableManager;
    }

    AbstractIndexManager bootIndex(Index index, AbstractTableManager tableManager, boolean created, long transaction, boolean rebuild, boolean restore) throws DataStorageManagerException {
        long _start = System.currentTimeMillis();
        if (!created) {
            LOGGER.log(Level.INFO, "bootIndex {0} {1}.{2}.{3} uuid {4} - {5}",
                new Object[] { nodeId, tableSpaceName, index.table, index.name, index.uuid, index.type });
        }
        AbstractIndexManager prevIndexManager = indexes.remove(index.name);
        if (prevIndexManager != null) {
            if (restore) {
                // restoring an index already booted in a previous life
                LOGGER.log(Level.INFO,
                        "bootIndex {0} {1}.{2}.{3} uuid {4} - {5} already exists on this tablespace. It will be truncated",
                        new Object[] { nodeId, tableSpaceName, index.table, index.name, index.uuid, index.type });
                prevIndexManager.dropIndexData();
            } else {
                LOGGER.log(Level.INFO, "bootIndex {0} {1}.{2}.{3} uuid {4} - {5}",
                        new Object[] { nodeId, tableSpaceName, index.table, index.name, index.uuid, index.type });
                if (indexes.containsKey(index.name)) {
                    throw new DataStorageManagerException(
                            "Index" + index.name + " already present in tableSpace " + tableSpaceName);
                }
            }
        }
        final int writeLockTimeout = dbmanager.getServerConfiguration().getInt(
                ServerConfiguration.PROPERTY_WRITELOCK_TIMEOUT,
                ServerConfiguration.PROPERTY_WRITELOCK_TIMEOUT_DEFAULT
        );
        final int readLockTimeout = dbmanager.getServerConfiguration().getInt(
                ServerConfiguration.PROPERTY_READLOCK_TIMEOUT,
                ServerConfiguration.PROPERTY_READLOCK_TIMEOUT_DEFAULT
        );
        AbstractIndexManager indexManager;
        switch (index.type) {
            case Index.TYPE_HASH:
                indexManager = new MemoryHashIndexManager(index, tableManager, log, dataStorageManager, this, tableSpaceUUID, transaction,
                        writeLockTimeout, readLockTimeout);
                break;
            case Index.TYPE_BRIN:
                indexManager = new BRINIndexManager(index, dbmanager.getMemoryManager(), tableManager, log, dataStorageManager, this, tableSpaceUUID, transaction,
                        writeLockTimeout, readLockTimeout);
                break;
            default:
                throw new DataStorageManagerException("invalid NON-UNIQUE index type " + index.type);
        }
        indexes.put(index.name, indexManager);

        Map<String, AbstractIndexManager> newMap = new HashMap<>(); // this must be mutable (see DROP INDEX)
        newMap.put(index.name, indexManager);

        indexesByTable.merge(index.table, newMap, (a, b) -> {
            Map<String, AbstractIndexManager> map = new HashMap<>(a);
            map.putAll(b);
            return map;
        });
        indexManager.start(tableManager.getBootSequenceNumber());
        if (!created) {
            long _stop = System.currentTimeMillis();
            LOGGER.log(Level.INFO, "bootIndex {0} {1}.{2} time {3} ms", new Object[]{nodeId, tableSpaceName, index.name, (_stop - _start) + ""});
        }
        if (rebuild) {
            indexManager.rebuild();
        }
        dbmanager.getPlanner().clearCache();
        return indexManager;
    }

    private void validateAlterTable(Table table, StatementEvaluationContext context) {
        AbstractTableManager tableManager = null;
        String oldTableName = null;
        for (AbstractTableManager tm : tables.values()) {
            if (tm.getTable().uuid.equals(table.uuid)) {
                tableManager = tm;
                oldTableName = tm.getTable().name;
            }
        }
        if (tableManager == null || oldTableName == null) {
            throw new TableDoesNotExistException("Cannot find table " + table.name + " with uuid " + table.uuid);
        }
        tableManager.validateAlterTable(table, context);
    }

    private AbstractTableManager alterTable(Table table, Transaction transaction) throws DDLException {
        LOGGER.log(Level.INFO, "alterTable {0} {1}.{2} uuid {3}", new Object[]{nodeId, tableSpaceName, table.name,
                table.uuid});
        AbstractTableManager tableManager = null;
        String oldTableName = null;
        for (AbstractTableManager tm : tables.values()) {
            if (tm.getTable().uuid.equals(table.uuid)) {
                tableManager = tm;
                oldTableName = tm.getTable().name;
            }
        }
        if (tableManager == null || oldTableName == null) {
            throw new TableDoesNotExistException("Cannot find table " + table.name + " with uuid " + table.uuid);
        }
        tableManager.tableAltered(table, transaction);
        if (!oldTableName.equalsIgnoreCase(table.name)) {
            tables.remove(oldTableName);
            tables.put(table.name, tableManager);
            Map<String, AbstractIndexManager> removed = indexesByTable.remove(oldTableName);
            if (removed != null && !removed.isEmpty()) {
                indexesByTable.put(table.name, removed);
            }
        }
        rebuildForeignKeyReferences(table);
        return tableManager;
    }

    public void close() throws LogNotAvailableException {
        boolean useJmx = dbmanager.getServerConfiguration().getBoolean(ServerConfiguration.PROPERTY_JMX_ENABLE, ServerConfiguration.PROPERTY_JMX_ENABLE_DEFAULT);
        closed = true;

        if (followerThread != null) {
            try {
                followerThread.waitForStop();
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.SEVERE, "Cannot wait for FollowerThread to stop", err);
            }
        }
        if (!virtual) {
            long lockStamp = acquireWriteLock("closeTablespace");
            try {
                for (Map.Entry<String, AbstractTableManager> table : tables.entrySet()) {
                    if (useJmx) {
                        JMXUtils.unregisterTableManagerStatsMXBean(tableSpaceName, table.getKey());
                    }
                    table.getValue().close();
                }
                for (AbstractIndexManager index : indexes.values()) {
                    index.close();
                }
                log.close();
            } finally {
                releaseWriteLock(lockStamp, "closeTablespace");
            }
        }
        if (useJmx) {
            JMXUtils.unregisterTableSpaceManagerStatsMXBean(tableSpaceName);
        }
    }

    public boolean isClosed() {
        return closed;
    }

    private static class TableSpaceCheckpoint {

        private final LogSequenceNumber sequenceNumber;
        private final Map<String, LogSequenceNumber> tablesCheckpoints;

        public TableSpaceCheckpoint(
                LogSequenceNumber sequenceNumber,
                Map<String, LogSequenceNumber> tablesCheckpoints
        ) {
            super();
            this.sequenceNumber = sequenceNumber;
            this.tablesCheckpoints = tablesCheckpoints;
        }
    }

    //this method return a tableCheckSum object contain scan values (record numbers , table digest,digestType, next autoincrement value, table name, tablespacename, query used for table scan )
    public TableChecksum createAndWriteTableCheksum(TableSpaceManager tableSpaceManager, String tableSpaceName, String tableName, StatementEvaluationContext context) throws IOException, DataScannerException {
        CommitLogResult pos;
        boolean lockAcquired = false;
        if (context == null) {
           context = StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT();
        }
        long lockStamp = context.getTableSpaceLock();
        LOGGER.log(Level.INFO, "Create and write table {0} checksum in tablespace " , new Object[]{tableName, tableSpaceName});
        if (lockStamp == 0) {
            lockStamp = acquireWriteLock("checkDataConsistency_" + tableName);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        try {
            AbstractTableManager tablemanager = tableSpaceManager.getTableManager(tableName);
            if (tableSpaceManager == null) {
                throw new TableSpaceDoesNotExistException(String.format("Tablespace %s does not exist.", tableSpaceName));
            }
            if (tablemanager == null || tablemanager.getCreatedInTransaction() > 0) {
                throw new TableDoesNotExistException(String.format("Table %s does not exist.", tablemanager));
            }
            TableChecksum scanResult = TableDataChecksum.createChecksum(tableSpaceManager.getDbmanager(), null, tableSpaceManager, tableSpaceName, tableName);
            byte[] serialize = MAPPER.writeValueAsBytes(scanResult);

            Bytes value = Bytes.from_array(serialize);
            LogEntry entry = LogEntryFactory.dataConsistency(tableName, value);
            pos = log.log(entry, false);
            apply(pos, entry, false);
            return scanResult;

        } finally {
            if (lockAcquired) {
                releaseWriteLock(context.getTableSpaceLock(), "checkDataConsistency");
                context.setTableSpaceLock(0);
            }
        }
    }

    /**
     * Says why a checkpoint was not taken while a restore from a snapshot is running, and gives up on a restore that
     * has stopped making progress.
     * <p>
     * The tablespace holds a fragment of that snapshot. Writing it to the storage would persist that fragment and
     * would move the position the tablespace is aligned to above the marker that opened the restore, so a node
     * booting after a crash would replay the log from after the marker, would never meet it, and would declare the
     * tablespace healthy while it holds half a snapshot. On a leader the same checkpoint also drops the ledgers up to
     * that position, so the marker can be gone for good.
     * </p>
     * <p>
     * The checkpoint is skipped, not deferred: {@link #restoreFinished()} takes the checkpoint that makes the
     * restored content usable as soon as the restore is over, and until then the activator comes back at every
     * checkpoint period anyway.
     * </p>
     * <p>
     * That is also why the way out of a restore that is standing still lives here. A client that dies is noticed
     * when its connection closes, but a client that keeps the connection open and simply stops, a connection pool or a
     * long lived application, leaves the restore open for good: no checkpoint of this tablespace ever runs again, its
     * commit log is never trimmed and its ledgers grow without bound. This is the one thing that comes back to the
     * question at a known period, so it is the one that can ask how long the restore has been standing still.
     * </p>
     * <p>
     * A node that is only watching somebody else's restore is in the same trap for a reason of its own. It has no
     * request of the restore to refresh the clock with, so what it measures is how long ago it read the marker, and
     * what it is waiting for is the marker that closes the restore. That marker never comes when the restore is
     * abandoned on the node that is running it: nothing is written to the log to say so. A node that keeps its
     * restore open without ever rebooting therefore leaves every replica of that tablespace unable to checkpoint, for
     * good. So the watcher gives up as well, and the two give-ups are different: see {@link #abortRestore} and
     * {@link #stopWatchingTheRestoreOfAnotherNode}.
     * </p>
     *
     * @return {@code true} if the restore was given up on, which takes the tablespace manager out of service
     */
    private boolean skipCheckpointBecauseOfRestore(LogSequenceNumber openRestore) {
        // Whether this node is driving the restore is local knowledge and it is read from local state: the marker on
        // the log says that a restore is open and nothing about who is running it, and it could not say anything
        // useful either, because a node reading it cannot tell a restore that is still going on from one that
        // completed elsewhere.
        boolean runHere = restoreDrivenByThisNode;
        long standingStillFor = System.currentTimeMillis() - restoreLastActivity;
        if (restoreMaxInactivityTime > 0 && standingStillFor > restoreMaxInactivityTime) {
            String reason = "it has not made any progress for " + standingStillFor + " ms, more than the "
                    + restoreMaxInactivityTime + " ms allowed by "
                    + ServerConfiguration.PROPERTY_RESTORE_MAX_INACTIVITY_TIME;
            if (runHere) {
                abortRestore(reason);
            } else {
                stopWatchingTheRestoreOfAnotherNode(openRestore, reason);
            }
            return true;
        }
        // a restore that is being driven is an ordinary state of a tablespace and says nothing worth a warning; one
        // that has been standing still long enough to be noticed is on its way to being given up on
        Level level = restoreMaxInactivityTime > 0 && standingStillFor > restoreMaxInactivityTime / 2
                ? Level.WARNING : Level.INFO;
        if (runHere) {
            LOGGER.log(level, "Checkpoint for tablespace {0} skipped. The restore from a snapshot opened at {1} is"
                    + " still running, the content of the tablespace is not complete yet. The restore last made"
                    + " progress {2} ms ago", new Object[]{tableSpaceName, openRestore, standingStillFor});
        } else {
            LOGGER.log(level, "Checkpoint for tablespace {0} skipped. The restore from a snapshot opened at {1} has"
                    + " not been closed yet, so the content this node holds is not the content of the tablespace any"
                    + " more. This node read the marker that opened it {2} ms ago",
                    new Object[]{tableSpaceName, openRestore, standingStillFor});
        }
        return false;
    }

    /**
     * Stops waiting for a restore another node opened and never closed, and takes the tablespace out of service so
     * that it is booted again.
     * <p>
     * This is not the conclusion {@link #abortRestore} draws, and it must not be: this node does not know whether that
     * restore is still going on somewhere, and the boot that follows changes nothing outside this node. What the boot
     * does is read the log again from the same place, which is the only thing that can end the wait: if the restore
     * was closed in the meantime the marker is there and the content is downloaded, and if the restore really is
     * still open this node goes back to waiting for another period. A wait that is bounded, logged and re-evaluated
     * is the point, against a tablespace that silently stops checkpointing for the rest of the life of the process.
     * </p>
     * <p>
     * Nothing is lost when the wait ends too early, which is why this is reported one level below
     * {@link #abortRestore}. A watching node has no request of the restore to keep its clock fresh, so what it
     * measures is not how long the restore has been standing still but how long ago it read the marker: a restore
     * that legitimately takes longer than the allowance makes every node watching it boot the tablespace once more,
     * which costs a tablespace that holds nothing a boot and a pass over the tail of its log.
     * </p>
     */
    private void stopWatchingTheRestoreOfAnotherNode(LogSequenceNumber openRestore, String reason) {
        LOGGER.log(Level.WARNING, "Tablespace {0} on node {1} has been waiting for the restore from a snapshot opened"
                + " at {2} to be closed: {3}. This node holds none of the content that restore is producing and it"
                + " cannot checkpoint while it waits, so the tablespace is taken out of service and booted again,"
                + " which reads the log from the same place and asks the question again",
                new Object[]{tableSpaceName, nodeId, openRestore, reason});
        setFailed();
    }

    /**
     * Records that the restore this node is serving is being driven. Every request a restore is made of goes through
     * here, so that a restore of a large snapshot, which takes as long as it takes, is told from one that nobody is
     * driving any more.
     */
    public void restoreInProgress() {
        restoreLastActivity = System.currentTimeMillis();
    }

    // visible for testing
    public TableSpaceCheckpoint checkpoint(boolean full, boolean pin, boolean alreadLocked) throws DataStorageManagerException, LogNotAvailableException {
        if (virtual) {
            return null;
        }

        if (recoveryInProgress) {
            LOGGER.log(Level.INFO, "Checkpoint for tablespace {0} skipped. Recovery is still in progress", tableSpaceName);
            return null;
        }

        if (isFailed()) {
            // A failed tablespace manager is on its way out of service: the activator is about to stop it and boot a
            // new one. Whatever it holds in memory is by definition not to be trusted, and a checkpoint would record
            // it as the state the tablespace is aligned to, which is exactly what the new boot is supposed to fix.
            // A manager whose log is what failed is failed in the same way and for the same reasons, which is the
            // question isFailed() answers and the raw field does not: its last sequence number is stale, and on a
            // leader the end of a checkpoint asks that very log to drop the ledgers below it.
            LOGGER.log(Level.INFO, "Checkpoint for tablespace {0} skipped. The tablespace manager is failed and it is"
                    + " going to be booted again", tableSpaceName);
            return null;
        }

        LogSequenceNumber openRestore = restoreFromSnapshot;
        if (openRestore != null) {
            skipCheckpointBecauseOfRestore(openRestore);
            return null;
        }

        long _start = System.currentTimeMillis();
        LogSequenceNumber logSequenceNumber = null;
        LogSequenceNumber _logSequenceNumber = null;
        Map<String, LogSequenceNumber> checkpointsTableNameSequenceNumber = new HashMap<>();

        try {
            List<PostCheckpointAction> actions = new ArrayList<>();

            long lockStamp = 0;
            if (!alreadLocked) {
                lockStamp = acquireWriteLock("checkpoint");
            }
            try {
                openRestore = restoreFromSnapshot;
                if (openRestore != null) {
                    // Asked again now that the write lock is held. beginRestore() writes the marker under this very
                    // lock, so a restore that started between the check above and this point would be checkpointed
                    // exactly like the one that check exists to prevent: the position read below is the position of
                    // the marker itself.
                    skipCheckpointBecauseOfRestore(openRestore);
                    return null;
                }
                logSequenceNumber = log.getLastSequenceNumber();

                if (logSequenceNumber.isStartOfTime()) {
                    LOGGER.log(Level.INFO, "{0} checkpoint {1} at {2}. skipped (no write ever issued to log)", new Object[]{nodeId, tableSpaceName, logSequenceNumber});
                    return new TableSpaceCheckpoint(logSequenceNumber, checkpointsTableNameSequenceNumber);
                }
                LOGGER.log(Level.INFO, "{0} checkpoint start {1} at {2}", new Object[]{nodeId, tableSpaceName, logSequenceNumber});
                if (actualLogSequenceNumber == null) {
                    throw new DataStorageManagerException("actualLogSequenceNumber cannot be null");
                }
                // TODO: transactions checkpoint is not atomic
                Collection<Transaction> currentTransactions = new ArrayList<>(transactions.values());
                for (Transaction t : currentTransactions) {
                    LogSequenceNumber txLsn = t.lastSequenceNumber;
                    if (txLsn != null && txLsn.after(logSequenceNumber)) {
                        LOGGER.log(Level.SEVERE, "Found transaction {0} with LSN {1} in the future", new Object[]{t.transactionId, txLsn});
                    }
                }
                actions.addAll(dataStorageManager.writeTransactionsAtCheckpoint(tableSpaceUUID, logSequenceNumber, currentTransactions));
                actions.addAll(writeTablesOnDataStorageManager(new CommitLogResult(logSequenceNumber, false, true), true));

                // we checkpoint all data to disk and save the actual log sequence number
                for (AbstractTableManager tableManager : tables.values()) {
                    // each TableManager will save its own checkpoint sequence number (on TableStatus) and upon recovery will replay only actions with log position after the actual table-local checkpoint
                    // remember that the checkpoint for a table can last "minutes" and we do not want to stop the world

                    if (!tableManager.isSystemTable()) {
                        TableCheckpoint checkpoint = full ? tableManager.fullCheckpoint(pin) : tableManager.checkpoint(pin);

                        if (checkpoint != null) {
                            LOGGER.log(Level.INFO, "checkpoint done for table {0}.{1} (pin: {2})", new Object[]{tableSpaceName, tableManager.getTable().name, pin});
                            actions.addAll(checkpoint.actions);
                            checkpointsTableNameSequenceNumber.put(checkpoint.tableName, checkpoint.sequenceNumber);
                            if (afterTableCheckPointAction != null) {
                                afterTableCheckPointAction.run();
                            }
                        }
                    }
                }

                // we are sure that all data as been flushed. upon recovery we will replay the log starting from this position
                actions.addAll(dataStorageManager.writeCheckpointSequenceNumber(tableSpaceUUID, logSequenceNumber));

                /* Indexes checkpoint is handled by TableManagers */
                if (leader) {
                    log.dropOldLedgers(logSequenceNumber);
                }

                _logSequenceNumber = log.getLastSequenceNumber();
            } finally {
                if (!alreadLocked) {
                    releaseWriteLock(lockStamp, "checkpoint");
                }
            }

            for (PostCheckpointAction action : actions) {
                try {
                    action.run();
                } catch (Exception error) {
                    LOGGER.log(Level.SEVERE, "postcheckpoint error:" + error, error);
                }
            }
            return new TableSpaceCheckpoint(logSequenceNumber, checkpointsTableNameSequenceNumber);
        } finally {
            long _stop = System.currentTimeMillis();
            LOGGER.log(Level.INFO, "{0} checkpoint finish {1} started ad {2}, finished at {3}, total time {4} ms",
                    new Object[]{nodeId, tableSpaceName, logSequenceNumber, _logSequenceNumber, Long.toString(_stop - _start)});
            checkpointTimeStats.registerSuccessfulEvent(_stop, TimeUnit.MILLISECONDS);
        }
    }

    private CompletableFuture<StatementExecutionResult> beginTransactionAsync(StatementEvaluationContext context, boolean releaseLock) throws StatementExecutionException {

        long id = newTransactionId.incrementAndGet();

        LogEntry entry = LogEntryFactory.beginTransaction(id);
        CommitLogResult pos;
        boolean lockAcquired = false;
        if (context.getTableSpaceLock() == 0) {
            long lockStamp = acquireReadLock("begin transaction");
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }

        pos = log.log(entry, false);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.thenApplyAsync((lsn) -> {
            apply(pos, entry, false);
            return new TransactionResult(id, TransactionResult.OutcomeType.BEGIN);
        }, callbacksExecutor);
        if (lockAcquired && releaseLock) {
            releaseReadLock(res, context.getTableSpaceLock(), "begin transaction");
        }
        return res;

    }

    private CompletableFuture<StatementExecutionResult> rollbackTransaction(RollbackTransactionStatement statement, StatementEvaluationContext context) throws StatementExecutionException {
        long txId = statement.getTransactionId();
        validateTransactionBeforeTxCommand(txId);
        LogEntry entry = LogEntryFactory.rollbackTransaction(txId);
        long lockStamp = context.getTableSpaceLock();
        boolean lockAcquired = false;
        if (lockStamp == 0) {
            lockStamp = acquireReadLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }

        CommitLogResult pos = log.log(entry, true);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.thenApplyAsync((lsn) -> {
            apply(pos, entry, false);
            return new TransactionResult(txId, TransactionResult.OutcomeType.ROLLBACK);
        }, callbacksExecutor);

        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .thenApply(s -> {
                        context.setTableSpaceLock(0);
                        return s;
                    });
        }
        return res;
    }

    private CompletableFuture<StatementExecutionResult> commitTransaction(CommitTransactionStatement statement, StatementEvaluationContext context) throws StatementExecutionException {
        long txId = statement.getTransactionId();

        validateTransactionBeforeTxCommand(txId);
        LogEntry entry = LogEntryFactory.commitTransaction(txId);
        long lockStamp = context.getTableSpaceLock();
        boolean lockAcquired = false;
        if (lockStamp == 0) {
            lockStamp = acquireReadLock(statement);
            context.setTableSpaceLock(lockStamp);
            lockAcquired = true;
        }
        CommitLogResult pos = log.log(entry, true);
        CompletableFuture<StatementExecutionResult> res = pos.logSequenceNumber.handleAsync((lsn, error) -> {
            if (error == null) {
                apply(pos, entry, false);
                return new TransactionResult(txId, TransactionResult.OutcomeType.COMMIT);
            } else {
                // if the log is not able to write the commit
                // apply a dummy "rollback", we are no more going to accept commands
                // in the scope of this transaction
                LogEntry rollback = LogEntryFactory.rollbackTransaction(txId);
                apply(new CommitLogResult(LogSequenceNumber.START_OF_TIME, false, false), rollback, false);
                throw new CompletionException(error);
            }
        }, callbacksExecutor);
        if (lockAcquired) {
            res = releaseReadLock(res, lockStamp, statement)
                    .thenApply(s -> {
                        context.setTableSpaceLock(0);
                        return s;
                    });
        }
        return res;
    }

    private void validateTransactionBeforeTxCommand(long txId) throws StatementExecutionException {
        validateTransactionBeforeTxCommand(txId, true);
    }

    private boolean validateTransactionBeforeTxCommand(long txId, boolean wait) throws StatementExecutionException {
        Transaction tc = transactions.get(txId);
        if (tc == null) {
            throw new StatementExecutionException("no such transaction " + txId + " in tablespace " + tableSpaceName);
        }
        while (tc.hasPendingActivities() && !closed) {
            LOGGER.log(Level.INFO, "Transaction {0} ({1}) has {2} pending activities",
                    new Object[]{txId, tableSpaceName, tc.getRefCount()});
            if (!ENABLE_PENDING_TRANSACTION_CHECK) {
                return true;
            }
            if (!wait) {
                return false;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new StatementExecutionException("Error while waiting for pending activities of transaction " + txId + " in " + tableSpaceName,
                        ex);
            }
        }
        if (closed) {
            throw new StatementExecutionException("tablespace closed during commit of transaction " + txId + " in tablespace " + tableSpaceName);
        }
        return true;
    }

    private CompletableFuture<StatementExecutionResult> releaseReadLock(
            CompletableFuture<StatementExecutionResult> promise, long lockStamp, Object description
    ) {
        return promise.whenComplete((r, error) -> {
            releaseReadLock(lockStamp, description);
        });
    }

    private void releaseReadLock(long lockStamp, Object description) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "{0} ts {2} relrlock {1}", new Object[]{tableSpaceName, description, lockStamp});
        }
//        LOGGER.log(Level.SEVERE, "RELEASED READLOCK for " + description + ", " + generalLock);
        generalLock.unlockRead(lockStamp);
    }

    public boolean isLeader() {
        return leader;
    }

    public Transaction getTransaction(long transactionId) {
        if (transactionId <= 0) {
            return null;
        }
        return transactions.get(transactionId);
    }

    public AbstractTableManager getTableManager(String tableName) {
        return tables.get(tableName);
    }

    public Collection<Long> getOpenTransactions() {
        return new HashSet<>(this.transactions.keySet());
    }

    public List<Transaction> getTransactions() {
        return new ArrayList<>(this.transactions.values());
    }

    private class ApplyEntryOnRecovery implements BiConsumer<LogSequenceNumber, LogEntry> {

        public ApplyEntryOnRecovery() {
        }

        @Override
        public void accept(LogSequenceNumber t, LogEntry u) {
            if (dbmanager.isStopped()) {
                throw new RuntimeException("System was requested to stop, aborting recovery at " + t);
            }
            try {
                apply(new CommitLogResult(t, false, true), u, true);
            } catch (DDLException | DataStorageManagerException err) {
                throw new RuntimeException(err);
            }
        }
    }

    public DBManager getDbmanager() {
        return dbmanager;
    }

    private final TableSpaceManagerStats stats = new TableSpaceManagerStats() {

        @Override
        public int getLoadedpages() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToInt(TableManagerStats::getLoadedpages)
                    .sum();

        }

        @Override
        public long getLoadedPagesCount() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToLong(TableManagerStats::getLoadedPagesCount)
                    .sum();
        }

        @Override
        public long getUnloadedPagesCount() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToLong(TableManagerStats::getUnloadedPagesCount)
                    .sum();
        }

        @Override
        public long getTablesize() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToLong(TableManagerStats::getTablesize)
                    .sum();
        }

        @Override
        public int getDirtypages() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToInt(TableManagerStats::getDirtypages)
                    .sum();
        }

        @Override
        public int getDirtyrecords() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToInt(TableManagerStats::getDirtyrecords)
                    .sum();
        }

        @Override
        public long getDirtyUsedMemory() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToLong(TableManagerStats::getDirtyUsedMemory)
                    .sum();
        }

        @Override
        public long getMaxLogicalPageSize() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToLong(TableManagerStats::getMaxLogicalPageSize)
                    .findFirst()
                    .orElse(0);
        }

        @Override
        public long getBuffersUsedMemory() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToLong(TableManagerStats::getBuffersUsedMemory)
                    .sum();
        }

        @Override
        public long getKeysUsedMemory() {
            return tables.values()
                    .stream()
                    .map(AbstractTableManager::getStats)
                    .mapToLong(TableManagerStats::getKeysUsedMemory)
                    .sum();
        }
    };

    public TableSpaceManagerStats getStats() {
        return stats;
    }

    public CommitLog getLog() {
        return log;
    }

    public ExecutorService getCallbacksExecutor() {
        return callbacksExecutor;
    }

    @Override
    public String toString() {
        return "TableSpaceManager [nodeId=" + nodeId
                + ", tableSpaceName=" + tableSpaceName
                + ", tableSpaceUUID=" + tableSpaceUUID + "]";
    }
}
