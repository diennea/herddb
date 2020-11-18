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
import com.google.common.util.concurrent.MoreExecutors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.client.ClientConfiguration;
import herddb.core.stats.ConnectionsInfoProvider;
import herddb.file.FileMetadataStorageManager;
import herddb.jmx.DBManagerStatsMXBean;
import herddb.jmx.JMXUtils;
import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.MetadataChangeListener;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.DDLException;
import herddb.model.DDLStatement;
import herddb.model.DDLStatementExecutionResult;
import herddb.model.DMLStatement;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataConsistencyStatementResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ExecutionPlan;
import herddb.model.GetResult;
import herddb.model.NodeMetadata;
import herddb.model.NotLeaderException;
import herddb.model.ScanResult;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TableSpaceReplicaState;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.DropTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.TableConsistencyCheckStatement;
import herddb.model.commands.TableSpaceConsistencyCheckStatement;
import herddb.network.Channel;
import herddb.network.ServerHostData;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.server.ServerConfiguration;
import herddb.server.ServerSidePreparedStatementCache;
import herddb.sql.AbstractSQLPlanner;
import herddb.sql.CalcitePlanner;
import herddb.sql.JSQLParserPlanner;
import herddb.sql.NullSQLPlanner;
import herddb.sql.PlansCache;
import herddb.sql.TranslatedQuery;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.DefaultJVMHalt;
import herddb.utils.Futures;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General Manager of the local instance of HerdDB
 *
 * @author enrico.olivelli
 */
public class DBManager implements AutoCloseable, MetadataChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBManager.class.getName());
    private final Map<String, TableSpaceManager> tablesSpaces = new ConcurrentHashMap<>();
    private final MetadataStorageManager metadataStorageManager;
    private final DataStorageManager dataStorageManager;
    private final CommitLogManager commitLogManager;
    private final String nodeId;
    private final String virtualTableSpaceId;
    private final ReentrantReadWriteLock generalLock = new ReentrantReadWriteLock();
    private final Thread activator;
    private final Activator activatorJ;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ExecutorService callbacksExecutor;
    private final AbstractSQLPlanner planner;
    private final ServerSidePreparedStatementCache preparedStatementsCache;
    private final Path tmpDirectory;
    private final RecordSetFactory recordSetFactory;
    private MemoryManager memoryManager;
    private final ServerHostData hostData;
    private String serverToServerUsername = ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT;
    private String serverToServerPassword = ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT;
    private boolean errorIfNotLeader = true;
    private final ServerConfiguration serverConfiguration;
    private final String mode;
    private ConnectionsInfoProvider connectionsInfoProvider;
    private long checkpointPeriod;
    private long abandonedTransactionsTimeout;
    private final StatsLogger mainStatsLogger;

    private long maxMemoryReference = ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE_DEFAULT;
    private long maxLogicalPageSize = ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE_DEFAULT;
    private long maxDataUsedMemory = ServerConfiguration.PROPERTY_MAX_DATA_MEMORY_DEFAULT;
    private long maxPKUsedMemory = ServerConfiguration.PROPERTY_MAX_PK_MEMORY_DEFAULT;

    private boolean clearAtBoot = false;
    private boolean haltOnTableSpaceBootError = ServerConfiguration.PROPERTY_HALT_ON_TABLESPACE_BOOT_ERROR_DEAULT;
    private Runnable haltProcedure = DefaultJVMHalt.INSTANCE;
    private final AtomicLong lastCheckPointTs = new AtomicLong(System.currentTimeMillis());

    private final RunningStatementsStats runningStatements;
    private final ExecutorService followersThreadPool;

    public DBManager(
            String nodeId, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager,
            CommitLogManager commitLogManager, Path tmpDirectory, herddb.network.ServerHostData hostData
    ) {
        this(nodeId, metadataStorageManager, dataStorageManager, commitLogManager, tmpDirectory, hostData, new ServerConfiguration(),
                null);
    }

    public DBManager(
            String nodeId, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager,
            CommitLogManager commitLogManager, Path tmpDirectory, herddb.network.ServerHostData hostData, ServerConfiguration configuration,
            StatsLogger statsLogger
    ) {
        this.serverConfiguration = configuration;
        this.mode = serverConfiguration.getString(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
        this.tmpDirectory = tmpDirectory;
        int asyncWorkerThreads = configuration.getInt(ServerConfiguration.PROPERTY_ASYNC_WORKER_THREADS,
                ServerConfiguration.PROPERTY_ASYNC_WORKER_THREADS_DEFAULT);
        if (asyncWorkerThreads <= 0) {
            // use this only for tests on in HerdDB Collections framework
            // without a threadpool every statement will be executed
            // on any system thread (Netty, BookKeeper...).
            this.callbacksExecutor = MoreExecutors.newDirectExecutorService();
        } else {
            this.callbacksExecutor = Executors.newFixedThreadPool(asyncWorkerThreads, new ThreadFactory() {
                private final AtomicLong count = new AtomicLong();

                @Override
                public Thread newThread(final Runnable r) {
                    final String marker = hostData == null ? "local" : hostData.getHost() + ":" + hostData.getPort();
                    return new FastThreadLocalThread(r, "db-dmlcall-" + marker + "-" + count.incrementAndGet());
                }
            });
        }
        // todo: make it configurable, cached have some pitfalls under load
        this.followersThreadPool = Executors.newCachedThreadPool((Runnable r) -> new FastThreadLocalThread(
                r, "herddb-worker-" + (hostData == null ? "local" : hostData.getHost() + ":" + hostData.getPort()) + "-" + r));
        this.recordSetFactory = dataStorageManager.createRecordSetFactory();
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.commitLogManager = commitLogManager;
        if (statsLogger == null) {
            this.mainStatsLogger = NullStatsLogger.INSTANCE;
        } else {
            this.mainStatsLogger = statsLogger;
        }
        this.runningStatements = new RunningStatementsStats(this.mainStatsLogger);
        this.nodeId = nodeId;
        this.virtualTableSpaceId = makeVirtualTableSpaceManagerId(nodeId);
        this.hostData = hostData != null ? hostData : new ServerHostData("localhost", 7000, "", false, new HashMap<>());
        long planCacheMem = configuration.getLong(ServerConfiguration.PROPERTY_PLANSCACHE_MAXMEMORY,
                ServerConfiguration.PROPERTY_PLANSCACHE_MAXMEMORY_DEFAULT);
        long statementsMem = configuration.getLong(ServerConfiguration.PROPERTY_STATEMENTSCACHE_MAXMEMORY,
                ServerConfiguration.PROPERTY_STATEMENTSCACHE_MAXMEMORY_DEFAULT);
        preparedStatementsCache = new ServerSidePreparedStatementCache(statementsMem);
        String plannerType = serverConfiguration.getString(ServerConfiguration.PROPERTY_PLANNER_TYPE,
                ServerConfiguration.PROPERTY_PLANNER_TYPE_DEFAULT);
        PlansCache plansCache = new PlansCache(planCacheMem);
        switch (plannerType) {
            case ServerConfiguration.PLANNER_TYPE_CALCITE:
                planner = new CalcitePlanner(this, plansCache);
                break;
            case ServerConfiguration.PLANNER_TYPE_JSQLPARSER:
                 // no Calcite fallback, you can drop Calcite dependecy
                planner = new JSQLParserPlanner(this, plansCache, null);
                break;
            case ServerConfiguration.PLANNER_TYPE_AUTO:
                // try with jSQLParser, fallback to Calcite
                planner = new JSQLParserPlanner(this, plansCache,
                        new CalcitePlanner(this, plansCache));
                break;
            case ServerConfiguration.PLANNER_TYPE_NONE:
                planner = new NullSQLPlanner(this);
                break;
            default:
                throw new IllegalArgumentException("invalid planner type " + plannerType);
        }
        this.activatorJ = new Activator();
        this.activator = new Thread(activatorJ, "hdb-" + nodeId + "-activator");
        this.activator.setDaemon(true);

        this.maxMemoryReference = configuration.getLong(
                ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE,
                ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE_DEFAULT);

        this.maxLogicalPageSize = configuration.getLong(
                ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE,
                ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE_DEFAULT);

        this.maxDataUsedMemory = configuration.getLong(
                ServerConfiguration.PROPERTY_MAX_DATA_MEMORY,
                ServerConfiguration.PROPERTY_MAX_DATA_MEMORY_DEFAULT);

        this.maxPKUsedMemory = configuration.getLong(
                ServerConfiguration.PROPERTY_MAX_PK_MEMORY,
                ServerConfiguration.PROPERTY_MAX_PK_MEMORY_DEFAULT);

    }

    public boolean isHaltOnTableSpaceBootError() {
        return haltOnTableSpaceBootError;
    }

    public void setHaltOnTableSpaceBootError(boolean haltOnTableSpaceBootError) {
        this.haltOnTableSpaceBootError = haltOnTableSpaceBootError;
    }

    public Runnable getHaltProcedure() {
        return haltProcedure;
    }

    public void setHaltProcedure(Runnable haltProcedure) {
        this.haltProcedure = haltProcedure;
    }

    public boolean isErrorIfNotLeader() {
        return errorIfNotLeader;
    }

    public void setErrorIfNotLeader(boolean errorIfNotLeader) {
        this.errorIfNotLeader = errorIfNotLeader;
    }

    public MetadataStorageManager getMetadataStorageManager() {
        return metadataStorageManager;
    }

    public DataStorageManager getDataStorageManager() {
        return dataStorageManager;
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    public String getServerToServerUsername() {
        return serverToServerUsername;
    }

    public void setServerToServerUsername(String serverToServerUsername) {
        this.serverToServerUsername = serverToServerUsername;
    }

    public String getServerToServerPassword() {
        return serverToServerPassword;
    }

    public void setServerToServerPassword(String serverToServerPassword) {
        this.serverToServerPassword = serverToServerPassword;
    }

    public ServerConfiguration getServerConfiguration() {
        return serverConfiguration;
    }

    public ConnectionsInfoProvider getConnectionsInfoProvider() {
        return connectionsInfoProvider;
    }

    public void setConnectionsInfoProvider(ConnectionsInfoProvider connectionsInfoProvider) {
        this.connectionsInfoProvider = connectionsInfoProvider;
    }

    public AbstractSQLPlanner getPlanner() {
        return planner;
    }

    public long getMaxMemoryReference() {
        return maxMemoryReference;
    }

    public void setMaxMemoryReference(long maxMemoryReference) {
        this.maxMemoryReference = maxMemoryReference;
    }

    public long getMaxLogicalPageSize() {
        return maxLogicalPageSize;
    }

    public void setMaxLogicalPageSize(long maxLogicalPageSize) {
        this.maxLogicalPageSize = maxLogicalPageSize;
    }

    public long getMaxDataUsedMemory() {
        return maxDataUsedMemory;
    }

    public void setMaxDataUsedMemory(long maxDataUsedMemory) {
        this.maxDataUsedMemory = maxDataUsedMemory;
    }

    public long getMaxPKUsedMemory() {
        return maxPKUsedMemory;
    }

    public void setMaxPKUsedMemory(long maxPKUsedMemory) {
        this.maxPKUsedMemory = maxPKUsedMemory;
    }

    private final DBManagerStatsMXBean stats = new DBManagerStatsMXBean() {

        @Override
        public long getCachedPlans() {
            return planner.getCacheSize();
        }

        @Override
        public long getCachePlansHits() {
            return planner.getCacheHits();
        }

        @Override
        public long getCachePlansMisses() {
            return planner.getCacheMisses();
        }

    };

    /**
     * Initial boot of the system
     *
     * @throws herddb.storage.DataStorageManagerException
     * @throws herddb.log.LogNotAvailableException
     * @throws herddb.metadata.MetadataStorageManagerException
     */
    public void start() throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException {
        LOGGER.debug("Starting DBManager at {}", nodeId);
        if (serverConfiguration.getBoolean(ServerConfiguration.PROPERTY_JMX_ENABLE, ServerConfiguration.PROPERTY_JMX_ENABLE_DEFAULT)) {
            JMXUtils.registerDBManagerStatsMXBean(stats);
        }

        final long maxHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();

        /* If max memory isn't configured or is too high default it to maximum heap */
        if (maxMemoryReference == 0 || maxMemoryReference > maxHeap) {
            maxMemoryReference = maxHeap;
        }
        LOGGER.info(ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE + "= {} bytes", Long.toString(maxMemoryReference));

        /* If max data memory for pages isn't configured or is too high default it to 0.3 maxMemoryReference */
        if (maxDataUsedMemory == 0 || maxDataUsedMemory > maxMemoryReference) {
            maxDataUsedMemory = (long) (0.3F * maxMemoryReference);
        }

        /* If max index memory for pages isn't configured or is too high default it to 0.2 maxMemoryReference */
        if (maxPKUsedMemory == 0 || maxPKUsedMemory > maxMemoryReference) {
            maxPKUsedMemory = (long) (0.2F * maxMemoryReference);
        }

        /* If max used memory is too high lower index and data accordingly */
        if (maxDataUsedMemory + maxPKUsedMemory > maxMemoryReference) {

            long data = (int) ((double) maxDataUsedMemory
                    / ((double) (maxDataUsedMemory + maxPKUsedMemory)) * maxMemoryReference);
            long pk = (int) ((double) maxPKUsedMemory
                    / ((double) (maxDataUsedMemory + maxPKUsedMemory)) * maxMemoryReference);

            maxDataUsedMemory = data;
            maxPKUsedMemory = pk;
        }

        memoryManager = new MemoryManager(maxDataUsedMemory, maxPKUsedMemory, maxLogicalPageSize);

        metadataStorageManager.start();

        if (clearAtBoot) {
            metadataStorageManager.clear();
        }

        metadataStorageManager.setMetadataChangeListener(this);
        NodeMetadata nodeMetadata = NodeMetadata
                .builder()
                .host(hostData.getHost())
                .port(hostData.getPort())
                .ssl(hostData.isSsl())
                .nodeId(nodeId)
                .build();
        if (!mode.equals(ServerConfiguration.PROPERTY_MODE_LOCAL)) {
            LOGGER.info("Registering on metadata storage manager my data: {}", nodeMetadata);
        }
        metadataStorageManager.registerNode(nodeMetadata);

        try {
            TableSpaceManager local_node_virtual_tables_manager = new TableSpaceManager(nodeId, virtualTableSpaceId,
                    virtualTableSpaceId, 1 /* expectedreplicacount*/,
                    metadataStorageManager, dataStorageManager, null, this, true);
            tablesSpaces.put(virtualTableSpaceId, local_node_virtual_tables_manager);
            local_node_virtual_tables_manager.start();
        } catch (DDLException | DataStorageManagerException | LogNotAvailableException | MetadataStorageManagerException error) {
            throw new IllegalStateException("cannot boot local virtual tablespace manager");
        }

        String initialDefaultTableSpaceLeader = nodeId;
        String initialDefaultTableSpaceReplicaNode = initialDefaultTableSpaceLeader;
        long initialDefaultTableSpaceMaxLeaderInactivityTime = 0; // disabled
        int expectedreplicacount = 1;

        // try to be more fault tolerant in case of DISKLESSCLUSTER
        // setting replica = * and maxLeaderInactivityTime = 1 minutes means
        // that if you lose the node with the server you will have at most 1 minute of unavailability (no leader)
        if (ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER.equals(mode)) {
            initialDefaultTableSpaceReplicaNode = TableSpace.ANY_NODE;
            initialDefaultTableSpaceMaxLeaderInactivityTime = 60_000; // 1 minute
            expectedreplicacount = serverConfiguration.getInt(ServerConfiguration.PROPERTY_DEFAULT_REPLICA_COUNT, ServerConfiguration.PROPERTY_DEFAULT_REPLICA_COUNT_DEFAULT);
        } else if (ServerConfiguration.PROPERTY_MODE_CLUSTER.equals(mode)) {
            expectedreplicacount = serverConfiguration.getInt(ServerConfiguration.PROPERTY_DEFAULT_REPLICA_COUNT, ServerConfiguration.PROPERTY_DEFAULT_REPLICA_COUNT_DEFAULT);
        }
        boolean created = metadataStorageManager.ensureDefaultTableSpace(initialDefaultTableSpaceLeader, initialDefaultTableSpaceReplicaNode,
                initialDefaultTableSpaceMaxLeaderInactivityTime, expectedreplicacount);
        if (created && !ServerConfiguration.PROPERTY_MODE_LOCAL.equals(mode)) {
            LOGGER.info("Created default tablespace " + TableSpace.DEFAULT
                    + " with expectedreplicacount={}, leader={}, replica={}, maxleaderinactivitytime={}",
                    new Object[]{expectedreplicacount, initialDefaultTableSpaceLeader,
                        initialDefaultTableSpaceReplicaNode,
                        initialDefaultTableSpaceMaxLeaderInactivityTime});
        }

        commitLogManager.start();

        generalLock.writeLock().lock();
        try {
            dataStorageManager.start();
        } finally {
            generalLock.writeLock().unlock();
        }
        activator.start();

        triggerActivator(ActivatorRunRequest.FULL);
    }

    public boolean waitForTablespace(String tableSpace, int millis) throws InterruptedException {
        return waitForTablespace(tableSpace, millis, true);
    }

    public boolean waitForBootOfLocalTablespaces(int millis) throws InterruptedException, MetadataStorageManagerException {
        List<String> tableSpacesToWaitFor = new ArrayList<>();
        Collection<String> allTableSpaces = metadataStorageManager.listTableSpaces();
        for (String tableSpaceName : allTableSpaces) {
            TableSpace tableSpace = metadataStorageManager.describeTableSpace(tableSpaceName);
            if (tableSpace.leaderId.equals(nodeId)) {
                tableSpacesToWaitFor.add(tableSpaceName);
            }
        }
        LOGGER.info("Waiting (max " + millis + " ms) for boot of local tablespaces: " + tableSpacesToWaitFor);

        for (String tableSpace : tableSpacesToWaitFor) {
            boolean ok = waitForTablespace(tableSpace, millis, true);
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    public boolean waitForTablespace(String tableSpace, int millis, boolean checkLeader) throws InterruptedException {
        long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now <= millis) {
            TableSpaceManager manager = tablesSpaces.get(tableSpace);
            if (manager != null) {
                if (checkLeader && manager.isLeader()) {
                    return true;
                }
                if (!checkLeader) {
                    return true;
                }
            }
            Thread.sleep(100);
        }
        return false;
    }

    public boolean waitForTable(String tableSpace, String table, int millis, boolean checkLeader) throws InterruptedException {
        long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now <= millis) {
            TableSpaceManager manager = tablesSpaces.get(tableSpace);
            if (manager != null) {
                if (checkLeader && manager.isLeader()) {
                    if (manager.getTableManager(table) != null) {
                        return true;
                    }
                }
                if (!checkLeader) {
                    AbstractTableManager tableManager = manager.getTableManager(table);
                    if (tableManager != null && tableManager.isStarted()) {
                        return true;
                    }
                }
            }
            Thread.sleep(100);
        }
        return false;
    }

    private void handleTableSpace(TableSpace tableSpace) throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException, DDLException {
        String tableSpaceName = tableSpace.name;

        TableSpaceManager actual_manager = tablesSpaces.get(tableSpaceName);
        if (actual_manager != null && actual_manager.isFailed()) {
            LOGGER.info("Tablespace {} is in 'Failed' status", new Object[]{tableSpaceName, nodeId});
            return;
        }
        if (actual_manager != null && actual_manager.isLeader() && !tableSpace.leaderId.equals(nodeId)) {
            LOGGER.info("Tablespace {} leader is no more {}, it changed to {}", new Object[]{tableSpaceName, nodeId, tableSpace.leaderId});
            stopTableSpace(tableSpaceName, tableSpace.uuid);
            return;
        }

        if (actual_manager != null && !actual_manager.isLeader() && tableSpace.leaderId.equals(nodeId)) {
            LOGGER.info("Tablespace {} need to switch to leadership on node {}", new Object[]{tableSpaceName, nodeId});
            stopTableSpace(tableSpaceName, tableSpace.uuid);
            return;
        }

        if (tableSpace.isNodeAssignedToTableSpace(nodeId) && !tablesSpaces.containsKey(tableSpaceName)) {
            LOGGER.info("Booting tablespace {} on {}, uuid {}", new Object[]{tableSpaceName, nodeId, tableSpace.uuid});
            long _start = System.currentTimeMillis();
            CommitLog commitLog = commitLogManager.createCommitLog(tableSpace.uuid, tableSpace.name, nodeId);
            TableSpaceManager manager = new TableSpaceManager(nodeId, tableSpaceName, tableSpace.uuid, tableSpace.expectedReplicaCount, metadataStorageManager, dataStorageManager, commitLog, this, false);
            try {
                manager.start();
                LOGGER.info("Boot success tablespace {} on {}, uuid {}, time {} ms leader:{}", new Object[]{tableSpaceName, nodeId, tableSpace.uuid, (System.currentTimeMillis() - _start) + "", manager.isLeader()});
                tablesSpaces.put(tableSpaceName, manager);
                if (serverConfiguration.getBoolean(ServerConfiguration.PROPERTY_JMX_ENABLE, ServerConfiguration.PROPERTY_JMX_ENABLE_DEFAULT)) {
                    JMXUtils.registerTableSpaceManagerStatsMXBean(tableSpaceName, manager.getStats());
                }
            } catch (DataStorageManagerException | LogNotAvailableException | MetadataStorageManagerException | DDLException t) {
                LOGGER.error("Error Booting tablespace {} on {}", new Object[]{tableSpaceName, nodeId});
                LOGGER.error("Error", t);
                tablesSpaces.remove(tableSpaceName);
                try {
                    manager.close();
                } catch (Throwable t2) {
                    LOGGER.error("Other Error", t2);
                    t.addSuppressed(t2);
                }
                throw t;
            }
            return;
        }

        if (tablesSpaces.containsKey(tableSpaceName) && !tableSpace.isNodeAssignedToTableSpace(nodeId)) {
            LOGGER.info("Tablespace {} on {} is not more in replica list {}, uuid {}", new Object[]{tableSpaceName, nodeId, tableSpace.uuid, tableSpace.replicas + ""});
            stopTableSpace(tableSpaceName, tableSpace.uuid);
            return;
        }

        if (!tableSpace.isNodeAssignedToTableSpace("*")
                && tableSpace.replicas.size() < tableSpace.expectedReplicaCount) {
            List<NodeMetadata> nodes = metadataStorageManager.listNodes();
            LOGGER.warn("Tablespace {} is underreplicated expectedReplicaCount={}, replicas={}, nodes={}", new Object[]{tableSpaceName, tableSpace.expectedReplicaCount, tableSpace.replicas, nodes});
            List<String> availableOtherNodes = nodes.stream().map(n -> {
                return n.nodeId;
            }).filter(n -> {
                return !tableSpace.replicas.contains(n);
            }).collect(Collectors.toList());
            Collections.shuffle(availableOtherNodes);
            int countMissing = tableSpace.expectedReplicaCount - tableSpace.replicas.size();
            LOGGER.warn("Tablespace {} is underreplicated expectedReplicaCount={}, replicas={}, missing {}, availableOtherNodes={}", new Object[]{tableSpaceName, tableSpace.expectedReplicaCount, tableSpace.replicas, countMissing, availableOtherNodes});
            if (!availableOtherNodes.isEmpty()) {
                TableSpace.Builder newTableSpaceBuilder =
                        TableSpace
                                .builder()
                                .cloning(tableSpace);
                while (!availableOtherNodes.isEmpty() && countMissing-- > 0) {
                    String node = availableOtherNodes.remove(0);
                    LOGGER.warn("Tablespace {} adding {} node as replica", new Object[]{tableSpaceName, node});
                    newTableSpaceBuilder.replica(node);
                }
                TableSpace newTableSpace = newTableSpaceBuilder.build();
                boolean ok = metadataStorageManager.updateTableSpace(newTableSpace, tableSpace);
                if (!ok) {
                    LOGGER.error("updating tableSpace {} metadata failed, someone else altered metadata", tableSpaceName);
                }
            }
        }

        if (actual_manager != null && !actual_manager.isFailed() && actual_manager.isLeader()) {
            actual_manager.metadataUpdated(tableSpace);
        }

    }

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

    public CompletableFuture<StatementExecutionResult> executeStatementAsync(Statement statement, StatementEvaluationContext context, TransactionContext transactionContext) {
        context.setDefaultTablespace(statement.getTableSpace());
        context.setManager(this);
        context.setTransactionContext(transactionContext);
//        LOGGER.error("executeStatement {}", new Object[]{statement});
        String tableSpace = statement.getTableSpace();
        if (tableSpace == null) {
            return Futures.exception(new StatementExecutionException("invalid null tableSpace"));
        }
        if (statement instanceof CreateTableSpaceStatement) {
            if (transactionContext.transactionId > 0) {
                return Futures.exception(new StatementExecutionException("CREATE TABLESPACE cannot be issued inside a transaction"));
            }
            return CompletableFuture.completedFuture(createTableSpace((CreateTableSpaceStatement) statement));
        }

        if (statement instanceof AlterTableSpaceStatement) {
            if (transactionContext.transactionId > 0) {
                return Futures.exception(new StatementExecutionException("ALTER TABLESPACE cannot be issued inside a transaction"));
            }
            return CompletableFuture.completedFuture(alterTableSpace((AlterTableSpaceStatement) statement));
        }
        if (statement instanceof DropTableSpaceStatement) {
            if (transactionContext.transactionId > 0) {
                return Futures.exception(new StatementExecutionException("DROP TABLESPACE cannot be issued inside a transaction"));
            }
            return CompletableFuture.completedFuture(dropTableSpace((DropTableSpaceStatement) statement));
        }
        if (statement instanceof TableSpaceConsistencyCheckStatement) {
            if (transactionContext.transactionId > 0) {
                return Futures.exception(new StatementExecutionException("TABLESPACECONSISTENCYCHECK cannot be issue inside a transaction"));
            }
            return  CompletableFuture.completedFuture(createTableSpaceCheckSum((TableSpaceConsistencyCheckStatement) statement));
        }
        TableSpaceManager manager = tablesSpaces.get(tableSpace);
        if (manager == null) {
            return Futures.exception(new NotLeaderException("No such tableSpace " + tableSpace + " here (at " + nodeId + "). "
                    + "Maybe the server is starting "));
        }
        if (errorIfNotLeader && !manager.isLeader()) {
            return Futures.exception(new NotLeaderException("node " + nodeId + " is not leader for tableSpace " + tableSpace));
        }
        CompletableFuture<StatementExecutionResult> res = manager.executeStatementAsync(statement, context, transactionContext);
        if (statement instanceof DDLStatement) {
            res.whenComplete((s, err) -> {
                planner.clearCache();
            });
            planner.clearCache();
        }
//        res.whenComplete((s, err) -> {
//            LOGGER.error("completed " + statement + ": " + s, err);
//        });
        return res;
    }

    /**
     * Executes a single lookup
     *
     * @param statement
     * @param context
     * @param transactionContext
     * @return
     * @throws StatementExecutionException
     */
    public GetResult get(GetStatement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        return (GetResult) executeStatement(statement, context, transactionContext);
    }

    public StatementExecutionResult executePlan(ExecutionPlan plan, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        CompletableFuture<StatementExecutionResult> res = executePlanAsync(plan, context, transactionContext);
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

    public CompletableFuture<StatementExecutionResult> executePlanAsync(ExecutionPlan plan, StatementEvaluationContext context, TransactionContext transactionContext) {
        try {
            context.setManager(this);
            plan.validateContext(context);
            if (plan.mainStatement instanceof ScanStatement) {
                DataScanner result = scan((ScanStatement) plan.mainStatement, context, transactionContext);
                // transction can be auto generated during the scan
                transactionContext = new TransactionContext(result.getTransactionId());
                return CompletableFuture
                        .completedFuture(new ScanResult(transactionContext.transactionId, result));

            } else {
                return executeStatementAsync(plan.mainStatement, context, transactionContext);
            }
        } catch (herddb.model.NotLeaderException err) {
            LOGGER.info("not-leader", err);
            return Futures.exception(err);
        } catch (Throwable err) {
            LOGGER.error("uncaught error", err);
            return Futures.exception(err);
        }
    }

    /**
     * Internal method used to execute simple data accesses, like foreign key checks.
     */
    public DataScanner executeSimpleQuery(String tableSpace, String query, List<Object> parameters,
            int maxRows, boolean keepReadLocks, TransactionContext transactionContext, StatementEvaluationContext context) {
        ScanResult scanResult = (ScanResult) executeSimpleStatement(tableSpace, query, parameters, maxRows, keepReadLocks, transactionContext, context);
        return scanResult.dataScanner;
    }

    /**
     * Internal method used to execute simple data accesses, like foreign key cascade actions.
     */
    public StatementExecutionResult executeSimpleStatement(String tableSpace, String query, List<Object> parameters,
            int maxRows, boolean keepReadLocks, TransactionContext transactionContext, StatementEvaluationContext context) {
        TranslatedQuery translatedQuery = planner.translate(tableSpace,
                query, parameters, true, true, false, maxRows);
        translatedQuery.context.setForceRetainReadLock(keepReadLocks);
        return executePlan(translatedQuery.plan, context != null ? context : translatedQuery.context, transactionContext);
    }

    public DataScanner scan(ScanStatement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        context.setDefaultTablespace(statement.getTableSpace());
        context.setManager(this);
        context.setTransactionContext(transactionContext);
        String tableSpace = statement.getTableSpace();
        if (tableSpace == null) {
            throw new StatementExecutionException("invalid null tableSpace");
        }
        TableSpaceManager manager = tablesSpaces.get(tableSpace);
        if (manager == null) {
            throw new NotLeaderException("No such tableSpace " + tableSpace + " here (at " + nodeId + "). "
                    + "Maybe the server is starting ");
        }
        boolean allowExecutionFromFollower = statement.getAllowExecutionFromFollower();
        if (errorIfNotLeader && !manager.isLeader() && !allowExecutionFromFollower) {
            throw new NotLeaderException("node " + nodeId + " is not leader for tableSpace " + tableSpace);
        }
        return manager.scan(statement, context, transactionContext, false, false);
    }

    /**
     * Utility method for DML/DDL statements
     *
     * @param statement
     * @param context
     * @param transactionContext
     * @return
     * @throws herddb.model.StatementExecutionException
     */
    public DMLStatementExecutionResult executeUpdate(DMLStatement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        return (DMLStatementExecutionResult) executeStatement(statement, context, transactionContext);
    }

    private StatementExecutionResult createTableSpace(CreateTableSpaceStatement createTableSpaceStatement) throws StatementExecutionException {
        TableSpace tableSpace;
        try {
            tableSpace = TableSpace
                    .builder()
                    .leader(createTableSpaceStatement.getLeaderId())
                    .name(createTableSpaceStatement.getTableSpace())
                    .replicas(createTableSpaceStatement.getReplicas())
                    .expectedReplicaCount(createTableSpaceStatement.getExpectedReplicaCount())
                    .maxLeaderInactivityTime(createTableSpaceStatement.getMaxleaderinactivitytime())
                    .build();
        } catch (IllegalArgumentException invalid) {
            throw new StatementExecutionException("invalid CREATE TABLESPACE statement: " + invalid.getMessage(), invalid);
        }

        try {
            metadataStorageManager.registerTableSpace(tableSpace);
            triggerActivator(ActivatorRunRequest.FULL);

            if (createTableSpaceStatement.getWaitForTableSpaceTimeout() > 0) {
                boolean okWait = false;
                int poolTime = 100;
                if (metadataStorageManager instanceof MemoryMetadataStorageManager
                        || metadataStorageManager instanceof FileMetadataStorageManager) {
                    poolTime = 5;
                }
                LOGGER.info("waiting for  " + tableSpace.name + ", uuid " + tableSpace.uuid + ", to be up withint " + createTableSpaceStatement.getWaitForTableSpaceTimeout() + " ms");
                final int timeout = createTableSpaceStatement.getWaitForTableSpaceTimeout();
                for (int i = 0; i < timeout; i += poolTime) {
                    List<TableSpaceReplicaState> replicateStates = metadataStorageManager.getTableSpaceReplicaState(tableSpace.uuid);
                    for (TableSpaceReplicaState ts : replicateStates) {
                        LOGGER.info("waiting for  " + tableSpace.name + ", uuid " + tableSpace.uuid + ", to be up, replica state node: " + ts.nodeId + ", state: " + TableSpaceReplicaState.modeToSQLString(ts.mode) + ", ts " + new java.sql.Timestamp(ts.timestamp));
                        if (ts.mode == TableSpaceReplicaState.MODE_LEADER) {
                            okWait = true;
                            break;
                        }
                    }
                    if (okWait) {
                        break;
                    }
                    Thread.sleep(poolTime);
                }
                if (!okWait) {
                    throw new StatementExecutionException("tablespace " + tableSpace.name + ", uuid " + tableSpace.uuid + " has been created but leader " + tableSpace.leaderId + " did not start within " + createTableSpaceStatement.getWaitForTableSpaceTimeout() + " ms");
                }
            }

            return new DDLStatementExecutionResult(TransactionContext.NOTRANSACTION_ID);
        } catch (StatementExecutionException err) {
            throw err;
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }
    }

    @Override
    public void close() throws DataStorageManagerException {
        stopped.set(true);
        setActivatorPauseStatus(false);
        triggerActivator(ActivatorRunRequest.NOOP);
        try {
            activator.join();
        } catch (final InterruptedException ignore) {
            ignore.printStackTrace();
        }
        followersThreadPool.shutdownNow();

        if (serverConfiguration.getBoolean(ServerConfiguration.PROPERTY_JMX_ENABLE, ServerConfiguration.PROPERTY_JMX_ENABLE_DEFAULT)) {
            JMXUtils.unregisterDBManagerStatsMXBean();
        }
        callbacksExecutor.shutdownNow();

        // lastly give a chance to not "leak" even if not critical (ie not keep used instances after close())
        try {
            followersThreadPool.awaitTermination(1, SECONDS);
            callbacksExecutor.awaitTermination(1, SECONDS); // give a chance to not "leak" even if not critical
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void checkpoint() throws DataStorageManagerException, LogNotAvailableException {
        for (TableSpaceManager man : tablesSpaces.values()) {
            man.checkpoint(false, false, false);
        }
    }

    public void triggerActivator(ActivatorRunRequest type) {
        activatorJ.offer(type);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getVirtualTableSpaceId() {
        return virtualTableSpaceId;
    }

    public String getMode() {
        return mode;
    }

    void submit(Runnable runnable) {
        try {
            followersThreadPool.submit(runnable);
        } catch (RejectedExecutionException err) {
            LOGGER.error("rejected " + runnable, err);
        }
    }

    private StatementExecutionResult alterTableSpace(AlterTableSpaceStatement alterTableSpaceStatement) throws StatementExecutionException {
        TableSpace tableSpace;

        try {
            TableSpace previous = metadataStorageManager.describeTableSpace(alterTableSpaceStatement.getTableSpace());
            if (previous == null) {
                throw new TableSpaceDoesNotExistException(alterTableSpaceStatement.getTableSpace());
            }
            try {
                tableSpace = TableSpace.builder()
                        .cloning(previous)
                        .leader(alterTableSpaceStatement.getLeaderId())
                        .name(alterTableSpaceStatement.getTableSpace())
                        .replicas(alterTableSpaceStatement.getReplicas())
                        .expectedReplicaCount(alterTableSpaceStatement.getExpectedReplicaCount())
                        .maxLeaderInactivityTime(alterTableSpaceStatement.getMaxleaderinactivitytime())
                        .build();
            } catch (IllegalArgumentException invalid) {
                throw new StatementExecutionException("invalid ALTER TABLESPACE statement: " + invalid.getMessage(), invalid);
            }
            metadataStorageManager.updateTableSpace(tableSpace, previous);
            triggerActivator(ActivatorRunRequest.FULL);
            return new DDLStatementExecutionResult(TransactionContext.NOTRANSACTION_ID);
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }
    }

    private StatementExecutionResult dropTableSpace(DropTableSpaceStatement dropTableSpaceStatement) throws StatementExecutionException {
        try {
            TableSpace previous = metadataStorageManager.describeTableSpace(dropTableSpaceStatement.getTableSpace());
            if (previous == null) {
                throw new TableSpaceDoesNotExistException(dropTableSpaceStatement.getTableSpace());
            }
            metadataStorageManager.dropTableSpace(dropTableSpaceStatement.getTableSpace(), previous);
            triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
            return new DDLStatementExecutionResult(TransactionContext.NOTRANSACTION_ID);
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }
    }

    public void dumpTableSpace(String tableSpace, String dumpId, Pdu message, Channel channel, int fetchSize, boolean includeLog) {
        TableSpaceManager manager = tablesSpaces.get(tableSpace);
        ByteBuf resp;
        if (manager == null) {
            resp = PduCodec.ErrorResponse.write(message.messageId, "tableSpace " + tableSpace + " not booted here");
            channel.sendReplyMessage(message.messageId, resp);
            return;
        } else {
            resp = PduCodec.AckResponse.write(message.messageId);
            channel.sendReplyMessage(message.messageId, resp);
        }
        try {
            manager.dumpTableSpace(dumpId, channel, fetchSize, includeLog);
        } catch (Exception error) {
            LOGGER.error("error on dump", error);
        }
    }

    public DataConsistencyStatementResult createTableCheckSum(TableConsistencyCheckStatement tableConsistencyCheckStatement, StatementEvaluationContext context) {
        TableSpaceManager manager = tablesSpaces.get(tableConsistencyCheckStatement.getTableSpace());
        String tableName = tableConsistencyCheckStatement.getTable();
        String tableSpaceName = tableConsistencyCheckStatement.getTableSpace();
        if (manager == null) {
            return new DataConsistencyStatementResult(false, "No such tablespace " + tableSpaceName);
        }
        try {
            manager.createAndWriteTableCheksum(manager, tableSpaceName, tableName, context);
        } catch (IOException | DataScannerException ex) {
            LOGGER.error("Error on check of tablespace " + tableSpaceName , ex);
            return new DataConsistencyStatementResult(false, "Error on check of tablespace " + tableSpaceName + ":" + ex);
        }
        return new DataConsistencyStatementResult(true, "Check table consistency for " + tableName + "completed");
    }

    public DataConsistencyStatementResult createTableSpaceCheckSum(TableSpaceConsistencyCheckStatement tableSpaceConsistencyCheckStatement) {
        TableSpaceManager manager = tablesSpaces.get(tableSpaceConsistencyCheckStatement.getTableSpace());
        String tableSpace = tableSpaceConsistencyCheckStatement.getTableSpace();
        List<Table> tables = manager.getAllCommittedTables();
        long _start = System.currentTimeMillis();
        for (Table table : tables) {
            AbstractTableManager tableManager = manager.getTableManager(table.name);
            if (!tableManager.isSystemTable()) {
                try {
                    manager.createAndWriteTableCheksum(manager, tableSpace, tableManager.getTable().name, null);
                } catch (IOException | DataScannerException ex) {
                    LOGGER.error("Error on check of tablespace " + tableSpace , ex);
                    return new DataConsistencyStatementResult(false, "Error on check  of tablespace " + tableSpace + ":" + ex);
                }
            }
        }
        long _stop = System.currentTimeMillis();
        long tableSpace_check_duration = (_stop - _start);
        LOGGER.info("Check tablespace consistency for {} Completed in {} ms", new Object[]{tableSpace, tableSpace_check_duration});
        return new DataConsistencyStatementResult(true, "Check tablespace consistency for " + tableSpace + "completed in " + tableSpace_check_duration);
    }

    private String makeVirtualTableSpaceManagerId(String nodeId) {
        return nodeId.replace(":", "").replace(".", "").toLowerCase();
    }

    // visible for testing
    public boolean isTableSpaceLocallyRecoverable(TableSpace tableSpace) {
        LogSequenceNumber logSequenceNumber = dataStorageManager.getLastcheckpointSequenceNumber(tableSpace.uuid);
        try (CommitLog tmpCommitLog = commitLogManager.createCommitLog(tableSpace.uuid, tableSpace.name, nodeId);) {
            return tmpCommitLog.isRecoveryAvailable(logSequenceNumber);
        }
    }

    private boolean tryBecomeLeaderFor(TableSpace tableSpace) throws DDLException, MetadataStorageManagerException {
        if (!isTableSpaceLocallyRecoverable(tableSpace)) {
            LOGGER.info("local node {} cannot become leader of {} (current is {})."
                    + "Cannot boot tablespace locally (not enough data, last checkpoint + log)",
                    new Object[]{nodeId, tableSpace.name, tableSpace.leaderId});
            return false;
        }
        LOGGER.info("node {}, try to become leader of {} (prev was {})", new Object[]{nodeId, tableSpace.name, tableSpace.leaderId});
        TableSpace.Builder newTableSpaceBuilder =
                TableSpace
                        .builder()
                        .cloning(tableSpace)
                        .leader(nodeId);
        TableSpace newTableSpace = newTableSpaceBuilder.build();
        boolean ok = metadataStorageManager.updateTableSpace(newTableSpace, tableSpace);
        if (!ok) {
            LOGGER.error("node {} updating tableSpace {} try to become leader failed", new Object[]{nodeId, tableSpace.name});
            return false;
        } else {
            LOGGER.error("node {} updating tableSpace {} try to become leader succeeded", new Object[]{nodeId, tableSpace.name});
            return true;
        }
    }

    long handleLocalMemoryUsage() {
        long result = 0;
        for (TableSpaceManager tableSpaceManager : tablesSpaces.values()) {
            result += tableSpaceManager.handleLocalMemoryUsage();
        }
        return result;

    }

    private class Activator implements Runnable {

        private final Lock runLock = new ReentrantLock();
        private final Condition resume = runLock.newCondition();

        private final BlockingQueue<ActivatorRunRequest> activatorQueue = new ArrayBlockingQueue<>(1);
        private volatile boolean activatorPaused = false;

        @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
        public void offer(ActivatorRunRequest type) {
            /*
             * RV_RETURN_VALUE_IGNORED_BAD_PRACTICE: ignore return, activator queue can contain at max one
             * element, every other request will be eventually handled from ActivatorRunRequest.FULL executed
             * periodically from the activator itself when it doesn't have any other work to do
             */
            activatorQueue.offer(type);
        }

        public void resume() {

            runLock.lock();
            try {
                boolean wasPaused = activatorPaused;
                if (!wasPaused) {
                    return;
                } else {
                    activatorPaused = false;
                }
                resume.signalAll();
            } finally {
                runLock.unlock();
            }
        }

        public void pause() {
            runLock.lock();
            try {
                /*
                 * Must be update in lock to avoid racing conditions between pause check
                 * (activatorPaused == true) and resume signal check (resume.awaitUninterruptibly())
                 */
                activatorPaused = true;
            } finally {
                runLock.unlock();
            }
        }

        @Override
        public void run() {
            try {
                try {

                    while (!stopped.get()) {

                        runLock.lock();
                        try {

                            if (activatorPaused) {
                                LOGGER.info("{} activator paused", nodeId);
                                resume.awaitUninterruptibly();
                                LOGGER.info("{} activator resumed", nodeId);
                                continue;
                            }
                        } finally {
                            runLock.unlock();
                        }

                        ActivatorRunRequest type = activatorQueue.poll(1, TimeUnit.SECONDS);
                        if (type == null) {
                            type = ActivatorRunRequest.FULL;
                        }
                        activatorQueue.clear();
                        if (!stopped.get()) {
                            executeActivator(type);
                        }
                    }
                } catch (InterruptedException ee) {
                }

                generalLock.writeLock().lock();
                try {
                    for (Map.Entry<String, TableSpaceManager> manager : tablesSpaces.entrySet()) {
                        try {
                            manager.getValue().close();
                        } catch (Exception err) {
                            LOGGER.error("error during shutdown of manager of tablespace " + manager.getKey(), err);
                        }
                    }
                } finally {
                    generalLock.writeLock().unlock();
                }
                try {
                    dataStorageManager.close();
                } catch (Exception err) {
                    LOGGER.error("error during shutdown", err);
                }
                try {
                    metadataStorageManager.close();
                } catch (Exception err) {
                    LOGGER.error("error during shutdown", err);
                }

                try {
                    commitLogManager.close();
                } catch (Exception err) {
                    LOGGER.error("error during shutdown", err);
                }
                LOGGER.debug("{} activator stopped", nodeId);
            } catch (RuntimeException err) {
                LOGGER.error("fatal activator erro", err);
            }
        }

    }

    public void setActivatorPauseStatus(boolean pause) {
        if (pause) {
            activatorJ.pause();
        } else {
            activatorJ.resume();
        }
    }

    public final boolean isStopped() {
        return stopped.get();
    }

    private void executeActivator(ActivatorRunRequest type) {
        try {
            if (type.enableTableSpacesManagement()) {
                if (manageTableSpaces()) {
                    return;
                }
            }

            boolean checkpointDone = false;
            if (type.enableGlobalCheckPoint()) {
                long now = System.currentTimeMillis();
                if (checkpointPeriod > 0 && now - lastCheckPointTs.get() > checkpointPeriod) {
                    lastCheckPointTs.set(now);
                    try {
                        checkpoint();
                        checkpointDone = true;
                    } catch (DataStorageManagerException | LogNotAvailableException error) {
                        LOGGER.error("checkpoint failed:" + error, error);
                    }
                }
            }
            if (!checkpointDone && type.enableTableCheckPoints()) {
                for (TableSpaceManager man : tablesSpaces.values()) {
                    man.runLocalTableCheckPoints();
                }
            }
            if (!checkpointDone && type.enableAbandonedTransactionsMaintenaince()) {
                for (TableSpaceManager man : tablesSpaces.values()) {
                    man.processAbandonedTransactions();
                }
            }
        } catch (RuntimeException err) {
            LOGGER.error("Fatal error during a system management task", err);
        }

    }

    private boolean manageTableSpaces() {
        Collection<String> actualTablesSpaces;
        try {
            // all lowercase names
            actualTablesSpaces = metadataStorageManager.listTableSpaces();
        } catch (MetadataStorageManagerException error) {
            LOGGER.error("cannot access tablespaces metadata", error);
            return true;
        }
        Map<String, TableSpace> currentTableSpaceMetadata = new HashMap<>();
        generalLock.writeLock().lock();
        try {
            for (String tableSpace : actualTablesSpaces) {
                TableSpace tableSpaceMetadata = metadataStorageManager.describeTableSpace(tableSpace);
                if (tableSpaceMetadata == null) {
                    LOGGER.info("tablespace {} does not exist", tableSpace);
                    continue;
                }
                currentTableSpaceMetadata.put(tableSpaceMetadata.uuid, tableSpaceMetadata);
                try {
                    handleTableSpace(tableSpaceMetadata);
                } catch (Exception err) {
                    LOGGER.error("cannot handle tablespace " + tableSpace, err);
                    if (haltOnTableSpaceBootError && haltProcedure != null) {
                        err.printStackTrace();
                        haltProcedure.run();
                    }
                }
            }
        } catch (MetadataStorageManagerException error) {
            LOGGER.error("cannot access tablespaces metadata", error);
            return true;
        } finally {
            generalLock.writeLock().unlock();
        }
        List<TableSpaceManager> followingActiveTableSpaces = new ArrayList<>();
        Set<String> failedTableSpaces = new HashSet<>();
        for (Map.Entry<String, TableSpaceManager> entry : tablesSpaces.entrySet()) {
            try {
                String tableSpaceUuid = entry.getValue().getTableSpaceUUID();
                if (entry.getValue().isFailed()) {
                    LOGGER.error("tablespace " + tableSpaceUuid + " failed");
                    failedTableSpaces.add(entry.getKey());
                } else if (!entry.getKey().equals(virtualTableSpaceId) && !actualTablesSpaces.contains(entry.getKey().toLowerCase())) {
                    LOGGER.error("tablespace " + tableSpaceUuid + " should not run here");
                    failedTableSpaces.add(entry.getKey());
                } else if (entry.getValue().isLeader()) {
                    metadataStorageManager.updateTableSpaceReplicaState(
                            TableSpaceReplicaState
                                    .builder()
                                    .mode(TableSpaceReplicaState.MODE_LEADER)
                                    .nodeId(nodeId)
                                    .uuid(tableSpaceUuid)
                                    .timestamp(System.currentTimeMillis())
                                    .build()
                    );
                } else {
                    metadataStorageManager.updateTableSpaceReplicaState(
                            TableSpaceReplicaState
                                    .builder()
                                    .mode(TableSpaceReplicaState.MODE_FOLLOWER)
                                    .nodeId(nodeId)
                                    .uuid(tableSpaceUuid)
                                    .timestamp(System.currentTimeMillis())
                                    .build()
                    );
                    followingActiveTableSpaces.add(entry.getValue());
                }
            } catch (MetadataStorageManagerException error) {
                LOGGER.error("cannot access tablespace " + entry.getKey() + " metadata", error);
                return true;
            }
        }
        if (!failedTableSpaces.isEmpty()) {
            generalLock.writeLock().lock();
            try {
                for (String tableSpace : failedTableSpaces) {
                    stopTableSpace(tableSpace, null);
                }
            } catch (MetadataStorageManagerException error) {
                LOGGER.error("cannot access tablespace metadata", error);
                return true;
            } finally {
                generalLock.writeLock().unlock();
            }
        }
        if (!followingActiveTableSpaces.isEmpty()) {
            long now = System.currentTimeMillis();
            try {
                for (TableSpaceManager tableSpaceManager : followingActiveTableSpaces) {
                    String tableSpaceUuid = tableSpaceManager.getTableSpaceUUID();
                    TableSpace tableSpaceInfo = currentTableSpaceMetadata.get(tableSpaceUuid);

                    if (tableSpaceInfo != null
                            && !tableSpaceInfo.leaderId.equals(nodeId)
                            && tableSpaceInfo.maxLeaderInactivityTime > 0
                            && !tableSpaceManager.isFailed()) {
                        List<TableSpaceReplicaState> allReplicas =
                                metadataStorageManager.getTableSpaceReplicaState(tableSpaceUuid);
                        TableSpaceReplicaState leaderState = allReplicas
                                .stream()
                                .filter(t -> t.nodeId.equals(tableSpaceInfo.leaderId))
                                .findAny()
                                .orElse(null);
                        if (leaderState == null) {
                            leaderState = new TableSpaceReplicaState(tableSpaceUuid, tableSpaceInfo.leaderId, tableSpaceInfo.metadataStorageCreationTime, TableSpaceReplicaState.MODE_STOPPED);
                            LOGGER.info("Leader for " + tableSpaceUuid + " should be " + tableSpaceInfo.leaderId + ", but it never sent pings or it disappeared,"
                                    + " considering last activity as tablespace creation time: " + new java.sql.Timestamp(tableSpaceInfo.metadataStorageCreationTime) + " to leave a minimal grace period");
                        }

                        long delta = now - leaderState.timestamp;
                        if (tableSpaceInfo.maxLeaderInactivityTime > delta) {
                            LOGGER.debug("Leader for " + tableSpaceUuid + " is " + tableSpaceInfo.leaderId
                                    + ", last ping " + new java.sql.Timestamp(leaderState.timestamp) + ". leader is healty");
                        } else {
                            LOGGER.error("Leader for " + tableSpaceUuid + " is " + tableSpaceInfo.leaderId
                                    + ", last ping " + new java.sql.Timestamp(leaderState.timestamp) + ". leader is failed.");
                            if (tryBecomeLeaderFor(tableSpaceInfo)) {
                                // only one change at a time
                                break;
                            }
                        }

                    }
                }
            } catch (MetadataStorageManagerException | DDLException error) {
                LOGGER.error("cannot access tablespace metadata", error);
                return true;
            }
        }
        return false;
    }

    private void stopTableSpace(String tableSpace, String uuid) throws MetadataStorageManagerException {
        LOGGER.info("stopTableSpace " + tableSpace + " uuid " + uuid + ", on " + nodeId);
        try {
            tablesSpaces.get(tableSpace).close();
        } catch (LogNotAvailableException err) {
            LOGGER.error("node " + nodeId + " cannot close for reboot tablespace " + tableSpace, err);
        }
        tablesSpaces.remove(tableSpace);
        if (uuid != null) {
            metadataStorageManager.updateTableSpaceReplicaState(
                    TableSpaceReplicaState
                            .builder()
                            .mode(TableSpaceReplicaState.MODE_STOPPED)
                            .nodeId(nodeId)
                            .uuid(uuid)
                            .timestamp(System.currentTimeMillis())
                            .build()
            );
        }
    }

    public Collection<String> getLocalTableSpaces() {
        return new ArrayList<>(tablesSpaces.keySet());
    }

    public TableSpaceManager getTableSpaceManager(String tableSpace) {
        return tablesSpaces.get(tableSpace);
    }

    public Path getTmpDirectory() {
        return tmpDirectory;
    }

    public RecordSetFactory getRecordSetFactory() {
        return recordSetFactory;
    }

    public long getCheckpointPeriod() {
        return checkpointPeriod;
    }

    public void setCheckpointPeriod(long checkpointPeriod) {
        this.checkpointPeriod = checkpointPeriod;
    }

    public long getAbandonedTransactionsTimeout() {
        return abandonedTransactionsTimeout;
    }

    public void setAbandonedTransactionsTimeout(long abandonedTransactionsTimeout) {
        this.abandonedTransactionsTimeout = abandonedTransactionsTimeout;
    }

    public long getLastCheckPointTs() {
        return lastCheckPointTs.get();
    }

    @Override
    public void metadataChanged(String description) {
        LOGGER.info("metadata changed: " + description);
        triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
    }

    public boolean isClearAtBoot() {
        return clearAtBoot;
    }

    public void setClearAtBoot(boolean clearAtBoot) {
        this.clearAtBoot = clearAtBoot;
    }

    public RunningStatementsStats getRunningStatements() {
        return runningStatements;
    }

    public ExecutorService getCallbacksExecutor() {
        return callbacksExecutor;
    }

    public ServerSidePreparedStatementCache getPreparedStatementsCache() {
        return preparedStatementsCache;
    }

    public StatsLogger getStatsLogger() {
        return this.mainStatsLogger;
    }

    // visible
    public boolean isFullSQLSupportEnabled() {
         String plannerType = serverConfiguration.getString(ServerConfiguration.PROPERTY_PLANNER_TYPE,
                ServerConfiguration.PROPERTY_PLANNER_TYPE_DEFAULT);
        return plannerType.equals(ServerConfiguration.PLANNER_TYPE_CALCITE)
                            || plannerType.equals(ServerConfiguration.PLANNER_TYPE_AUTO);
    }
}
