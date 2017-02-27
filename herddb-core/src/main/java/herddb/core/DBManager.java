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

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import herddb.client.ClientConfiguration;
import herddb.core.join.DataScannerJoinExecutor;
import herddb.core.stats.ConnectionsInfoProvider;
import herddb.file.FileMetadataStorageManager;
import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.log.LogNotAvailableException;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.MetadataChangeListener;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Column;
import herddb.model.DDLException;
import herddb.model.DDLStatement;
import herddb.model.DDLStatementExecutionResult;
import herddb.model.DMLStatement;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ExecutionPlan;
import herddb.model.GetResult;
import herddb.model.LimitedDataScanner;
import herddb.model.NodeMetadata;
import herddb.model.NotLeaderException;
import herddb.model.ScanResult;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TableSpace;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TableSpaceReplicaState;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.model.TuplePredicate;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.DropTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.network.Channel;
import herddb.network.Message;
import herddb.network.ServerHostData;
import herddb.server.ServerConfiguration;
import herddb.sql.SQLPlanner;
import herddb.sql.SQLStatementEvaluationContext;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.ChangeThreadName;
import herddb.utils.DefaultJVMHalt;

/**
 * General Manager of the local instance of HerdDB
 *
 * @author enrico.olivelli
 */
public class DBManager implements AutoCloseable, MetadataChangeListener {

    private final static Logger LOGGER = Logger.getLogger(DBManager.class.getName());
    private final Map<String, TableSpaceManager> tablesSpaces = new ConcurrentHashMap<>();
    private final MetadataStorageManager metadataStorageManager;
    private final DataStorageManager dataStorageManager;
    private final CommitLogManager commitLogManager;
    private final String nodeId;
    private final String virtualTableSpaceId;
    private final ReentrantReadWriteLock generalLock = new ReentrantReadWriteLock();
    private final Thread activator;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final BlockingQueue<ActivatorRunRequest> activatorQueue = new LinkedBlockingDeque<>();
    private final SQLPlanner translator;
    private final Path tmpDirectory;
    private final RecordSetFactory recordSetFactory;
    private MemoryManager memoryManager;
    private final ServerHostData hostData;
    private String serverToServerUsername = ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT;
    private String serverToServerPassword = ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT;
    private boolean errorIfNotLeader = true;
    private final ServerConfiguration serverConfiguration;
    private ConnectionsInfoProvider connectionsInfoProvider;
    private long checkpointPeriod;

    private long maxMemoryReference = ServerConfiguration.PROPERTY_MEMORY_LIMIT_REFERENCE_DEFAULT;
    private long maxLogicalPageSize = ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE_DEFAULT;
    private long maxPagesUsedMemory = ServerConfiguration.PROPERTY_MAX_PAGES_MEMORY_DEFAULT;

    private boolean clearAtBoot = false;
    private boolean haltOnTableSpaceBootError = ServerConfiguration.PROPERTY_HALT_ON_TABLESPACEBOOT_ERROR_DEAULT;
    private Runnable haltProcedure = DefaultJVMHalt.INSTANCE;
    private final AtomicLong lastCheckPointTs = new AtomicLong(System.currentTimeMillis());

    private final ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, r + "");
            t.setDaemon(true);
            return t;
        }
    });

    public DBManager(String nodeId, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager,
        CommitLogManager commitLogManager, Path tmpDirectory, herddb.network.ServerHostData hostData) {
        this(nodeId, metadataStorageManager, dataStorageManager, commitLogManager, tmpDirectory, hostData, new ServerConfiguration());
    }

    public DBManager(String nodeId, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager,
        CommitLogManager commitLogManager, Path tmpDirectory, herddb.network.ServerHostData hostData, ServerConfiguration configuration) {
        this.serverConfiguration = configuration;
        this.tmpDirectory = tmpDirectory;
        this.recordSetFactory = dataStorageManager.createRecordSetFactory();
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.commitLogManager = commitLogManager;
        this.nodeId = nodeId;
        this.virtualTableSpaceId = makeVirtualTableSpaceManagerId(nodeId);
        this.hostData = hostData != null ? hostData : new ServerHostData("localhost", 7000, "", false, new HashMap<>());
        this.translator = new SQLPlanner(this, configuration.getLong(ServerConfiguration.PROPERTY_PLANSCACHE_MAXMEMORY,
            ServerConfiguration.PROPERTY_PLANSCACHE_MAXMEMORY_DEFAULT));
        this.activator = new Thread(new Activator(), "hdb-" + nodeId + "-activator");
        this.activator.setDaemon(true);
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

    public SQLPlanner getPlanner() {
        return translator;
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

    public long getMaxPagesUsedMemory() {
        return maxPagesUsedMemory;
    }

    public void setMaxPagesUsedMemory(long maxPagesUsedMemory) {
        this.maxPagesUsedMemory = maxPagesUsedMemory;
    }

    /**
     * Initial boot of the system
     *
     * @throws herddb.storage.DataStorageManagerException
     * @throws herddb.log.LogNotAvailableException
     * @throws herddb.metadata.MetadataStorageManagerException
     */
    public void start() throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException {

        final long maxHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();

        /* If max memory isn't configured or is too high default it to maximum heap */
        if (maxMemoryReference == 0 || maxMemoryReference > maxHeap) {
            maxMemoryReference = maxHeap;
        }

        /* If max memory for pages isn't configured or is too high default it to 0.5 maxMemoryReference */
        if (maxPagesUsedMemory == 0 || maxPagesUsedMemory > maxMemoryReference) {
            maxPagesUsedMemory = (long) (0.5F * maxMemoryReference);
        }

        memoryManager = new MemoryManager(
            maxPagesUsedMemory, maxLogicalPageSize);

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
        LOGGER.log(Level.SEVERE, "Registering on metadata storage manager my data: {0}", nodeMetadata);
        metadataStorageManager.registerNode(nodeMetadata);

        try {
            TableSpaceManager local_node_virtual_tables_manager = new TableSpaceManager(nodeId, virtualTableSpaceId, virtualTableSpaceId, metadataStorageManager, dataStorageManager, null, this, true);
            tablesSpaces.put(virtualTableSpaceId, local_node_virtual_tables_manager);
            local_node_virtual_tables_manager.start();
        } catch (DDLException | DataStorageManagerException | LogNotAvailableException | MetadataStorageManagerException error) {
            throw new IllegalStateException("cannot boot local virtual tablespace manager");
        }

        metadataStorageManager.ensureDefaultTableSpace(nodeId);

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
        LOGGER.log(Level.INFO, "Waiting (max " + millis + " ms) for boot of local tablespaces: " + tableSpacesToWaitFor);

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
                    if (manager.getTableManager(table) != null) {
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
        if (actual_manager != null && actual_manager.isLeader() && !tableSpace.leaderId.equals(nodeId)) {
            LOGGER.log(Level.SEVERE, "Tablespace {0} leader is no more {1}, it changed to {2}", new Object[]{tableSpaceName, nodeId, tableSpace.leaderId});
            stopTableSpace(tableSpaceName, tableSpace.uuid);
        }

        if (actual_manager != null && !actual_manager.isLeader() && tableSpace.leaderId.equals(nodeId)) {
            LOGGER.log(Level.SEVERE, "Tablespace {0} need to switch to leadership on node {1}", new Object[]{tableSpaceName, nodeId});
            stopTableSpace(tableSpaceName, tableSpace.uuid);
        }

        if (tableSpace.replicas.contains(nodeId) && !tablesSpaces.containsKey(tableSpaceName)) {
            LOGGER.log(Level.SEVERE, "Booting tablespace {0} on {1}, uuid {2}", new Object[]{tableSpaceName, nodeId, tableSpace.uuid});
            long _start = System.currentTimeMillis();
            CommitLog commitLog = commitLogManager.createCommitLog(tableSpace.uuid);
            TableSpaceManager manager = new TableSpaceManager(nodeId, tableSpaceName, tableSpace.uuid, metadataStorageManager, dataStorageManager, commitLog, this, false);
            try {
                manager.start();
                LOGGER.log(Level.SEVERE, "Boot success tablespace {0} on {1}, uuid {2}, time {3} ms", new Object[]{tableSpaceName, nodeId, tableSpace.uuid, (System.currentTimeMillis() - _start) + ""});
                tablesSpaces.put(tableSpaceName, manager);
            } catch (DataStorageManagerException | LogNotAvailableException | MetadataStorageManagerException | DDLException t) {
                LOGGER.log(Level.SEVERE, "Error Booting tablespace {0} on {1}", new Object[]{tableSpaceName, nodeId});
                LOGGER.log(Level.SEVERE, "Error", t);
                try {
                    manager.close();
                } catch (Throwable t2) {
                    LOGGER.log(Level.SEVERE, "Other Error", t2);
                }
                throw t;
            }
            return;
        }

        if (tablesSpaces.containsKey(tableSpaceName) && !tableSpace.replicas.contains(nodeId)) {
            LOGGER.log(Level.SEVERE, "Tablespace {0} on {1} is not more in replica list {3}, uuid {2}", new Object[]{tableSpaceName, nodeId, tableSpace.uuid, tableSpace.replicas + ""});
            stopTableSpace(tableSpaceName, tableSpace.uuid);
            return;
        }

        if (tableSpace.replicas.size() < tableSpace.expectedReplicaCount) {
            List<NodeMetadata> nodes = metadataStorageManager.listNodes();
            LOGGER.log(Level.SEVERE, "Tablespace {0} is underreplicated expectedReplicaCount={1}, replicas={2}, nodes={3}", new Object[]{tableSpaceName, tableSpace.expectedReplicaCount, tableSpace.replicas, nodes});
            List<String> availableOtherNodes = nodes.stream().map(n -> {
                return n.nodeId;
            }).filter(n -> {
                return !tableSpace.replicas.contains(n);
            }).collect(Collectors.toList());
            Collections.shuffle(availableOtherNodes);
            LOGGER.log(Level.SEVERE, "Tablespace {0} is underreplicated expectedReplicaCount={1}, replicas={2}, availableOtherNodes={3}", new Object[]{tableSpaceName, tableSpace.expectedReplicaCount, tableSpace.replicas, availableOtherNodes});
            if (!availableOtherNodes.isEmpty()) {
                int countMissing = tableSpace.expectedReplicaCount - tableSpace.replicas.size();
                TableSpace.Builder newTableSpaceBuilder
                    = TableSpace
                        .builder()
                        .cloning(tableSpace);
                while (!availableOtherNodes.isEmpty() && countMissing > 0) {
                    String node = availableOtherNodes.remove(0);
                    newTableSpaceBuilder.replica(node);
                }
                TableSpace newTableSpace = newTableSpaceBuilder.build();
                boolean ok = metadataStorageManager.updateTableSpace(newTableSpace, tableSpace);
                if (!ok) {
                    LOGGER.log(Level.SEVERE, "updating tableSpace " + tableSpaceName + " metadata failed");
                }
            }
        }

    }

    public StatementExecutionResult executeStatement(Statement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        context.setDefaultTablespace(statement.getTableSpace());
        context.setManager(this);
        context.setTransactionContext(transactionContext);
        //LOGGER.log(Level.SEVERE, "executeStatement {0}", new Object[]{statement});
        String tableSpace = statement.getTableSpace();
        if (tableSpace == null) {
            throw new StatementExecutionException("invalid tableSpace " + tableSpace);
        }
        try {
            if (statement instanceof CreateTableSpaceStatement) {
                if (transactionContext.transactionId > 0) {
                    throw new StatementExecutionException("CREATE TABLESPACE cannot be issued inside a transaction");
                }
                return createTableSpace((CreateTableSpaceStatement) statement);
            }

            if (statement instanceof AlterTableSpaceStatement) {
                if (transactionContext.transactionId > 0) {
                    throw new StatementExecutionException("ALTER TABLESPACE cannot be issued inside a transaction");
                }
                return alterTableSpace((AlterTableSpaceStatement) statement);
            }
            if (statement instanceof DropTableSpaceStatement) {
                if (transactionContext.transactionId > 0) {
                    throw new StatementExecutionException("DROP TABLESPACE cannot be issued inside a transaction");
                }
                return dropTableSpace((DropTableSpaceStatement) statement);
            }

            TableSpaceManager manager = tablesSpaces.get(tableSpace);
            if (manager == null) {
                throw new StatementExecutionException("No such tableSpace " + tableSpace + " here. "
                    + "Maybe the server is starting ");
            }
            if (errorIfNotLeader && !manager.isLeader()) {
                throw new NotLeaderException("node " + nodeId + " is not leader for tableSpace " + tableSpace);
            }
            return manager.executeStatement(statement, context, transactionContext);
        } finally {
            if (statement instanceof DDLStatement) {
                translator.clearCache();
            }
        }
    }

    /**
     * Executes a single lookup
     *
     * @param statement
     * @return
     * @throws StatementExecutionException
     */
    public GetResult get(GetStatement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        return (GetResult) executeStatement(statement, context, transactionContext);
    }

    public StatementExecutionResult executePlan(ExecutionPlan plan, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        try (ChangeThreadName changeThreadName = new ChangeThreadName("executePlan " + plan)) {
            if (plan.mainStatement instanceof ScanStatement) {
                DataScanner result = scan((ScanStatement) plan.mainStatement, context, transactionContext);
                // transction can be auto generated during the scan
                transactionContext = new TransactionContext(result.transactionId);
                return executeDataScannerPlan(plan, result, context, transactionContext);
            } else if (plan.dataSource != null) {
                try {
                    ScanResult data = (ScanResult) executePlan(plan.dataSource, context, transactionContext);
                    int insertCount = 0;
                    try {
                        // transction can be auto generated during the scan
                        transactionContext = new TransactionContext(data.transactionId);
                        while (data.dataScanner.hasNext()) {
                            Tuple tuple = data.dataScanner.next();
                            SQLStatementEvaluationContext tmp_context = new SQLStatementEvaluationContext("--", Arrays.asList(tuple.getValues()));
                            DMLStatementExecutionResult res = (DMLStatementExecutionResult) executeStatement(plan.mainStatement, tmp_context, transactionContext);
                            insertCount += res.getUpdateCount();
                        }
                    } finally {
                        data.dataScanner.close();
                    }
                    return new DMLStatementExecutionResult(transactionContext.transactionId, insertCount);
                } catch (DataScannerException err) {
                    throw new StatementExecutionException(err);
                }
            } else if (plan.joinStatements != null) {
                List<DataScanner> scanResults = new ArrayList<>();
                for (ScanStatement statement : plan.joinStatements) {
                    DataScanner result = scan(statement, context, transactionContext);
                    // transction can be auto generated during the scan
                    transactionContext = new TransactionContext(result.transactionId);
                    scanResults.add(result);
                }
                return executeJoinedScansPlan(scanResults, context, transactionContext,
                    plan);

            } else if (plan.insertStatements != null) {
                int insertCount = 0;
                for (InsertStatement insert : plan.insertStatements) {
                    DMLStatementExecutionResult res = (DMLStatementExecutionResult) executeStatement(insert, context, transactionContext);
                    // transction can be auto generated during the loop
                    transactionContext = new TransactionContext(res.transactionId);
                    insertCount += res.getUpdateCount();
                }
                return new DMLStatementExecutionResult(transactionContext.transactionId, insertCount);
            } else {
                return executeStatement(plan.mainStatement, context, transactionContext);
            }
        }
    }

    private StatementExecutionResult executeDataScannerPlan(ExecutionPlan plan, DataScanner result,
        StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        ScanResult scanResult;
        if (plan.mainAggregator != null) {
            scanResult = new ScanResult(transactionContext.transactionId, plan.mainAggregator.aggregate(result, context));
        } else {
            scanResult = new ScanResult(transactionContext.transactionId, result);
        }
        if (plan.comparator != null) {
            // SORT is to be applied before limits
            MaterializedRecordSet sortedSet = recordSetFactory.createRecordSet(
                scanResult.dataScanner.getFieldNames(),
                scanResult.dataScanner.getSchema());
            try {
                scanResult.dataScanner.forEach(sortedSet::add);
                sortedSet.writeFinished();
                sortedSet.sort(plan.comparator);
                scanResult.dataScanner.close();
                scanResult = new ScanResult(transactionContext.transactionId, new SimpleDataScanner(transactionContext.transactionId, sortedSet));
            } catch (DataScannerException err) {
                throw new StatementExecutionException(err);
            }
        }
        if (plan.limits != null) {
            try {
                return new ScanResult(transactionContext.transactionId,
                    new LimitedDataScanner(scanResult.dataScanner, plan.limits, context));
            } catch (DataScannerException limitError) {
                throw new StatementExecutionException(limitError);
            }
        } else {
            return scanResult;
        }
    }

    private StatementExecutionResult executeJoinedScansPlan(List<DataScanner> scanResults,
        StatementEvaluationContext context,
        TransactionContext transactionContext,
        ExecutionPlan plan) throws StatementExecutionException {
        try {
            List<Column> composedSchema = new ArrayList<>();
            for (DataScanner ds : scanResults) {
                composedSchema.addAll(Arrays.asList(ds.getSchema()));
            }
            Column[] finalSchema = new Column[composedSchema.size()];            
            composedSchema.toArray(finalSchema);
            String[] finalSchemaFieldNames = Column.buildFieldNamesList(finalSchema);
            MaterializedRecordSet finalResultSet = recordSetFactory.createRecordSet(finalSchemaFieldNames, finalSchema);

            DataScannerJoinExecutor joinExecutor;
            if (plan.joinProjection != null) {
                if (plan.joinFilter != null) {
                    TuplePredicate joinFilter = plan.joinFilter;
                    joinExecutor = new DataScannerJoinExecutor(finalSchemaFieldNames, finalSchema, scanResults, t -> {
                        if (joinFilter.matches(t, context)) {
                            finalResultSet.add(plan.joinProjection.map(t, context));
                        }
                    });
                } else {
                    joinExecutor = new DataScannerJoinExecutor(finalSchemaFieldNames, finalSchema, scanResults, t -> {
                        finalResultSet.add(plan.joinProjection.map(t, context));
                    });
                }
            } else {
                if (plan.joinFilter != null) {
                    TuplePredicate joinFilter = plan.joinFilter;
                    joinExecutor = new DataScannerJoinExecutor(finalSchemaFieldNames, finalSchema, scanResults, t -> {
                        if (joinFilter.matches(t, context)) {
                            finalResultSet.add(t);
                        }
                    });
                } else {
                    joinExecutor = new DataScannerJoinExecutor(finalSchemaFieldNames, finalSchema, scanResults, t -> {
                        finalResultSet.add(t);
                    });
                }
            }
            joinExecutor.executeJoin();

            finalResultSet.writeFinished();
            finalResultSet.sort(plan.comparator);
            finalResultSet.applyLimits(plan.limits, context);

            return new ScanResult(
                transactionContext.transactionId,
                new SimpleDataScanner(transactionContext.transactionId,
                    finalResultSet));
        } catch (DataScannerException err) {
            throw new StatementExecutionException(err);
        }
    }

    public DataScanner scan(ScanStatement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        context.setDefaultTablespace(statement.getTableSpace());
        context.setManager(this);
        context.setTransactionContext(transactionContext);
        String tableSpace = statement.getTableSpace();
        if (tableSpace == null) {
            throw new StatementExecutionException("invalid tableSpace " + tableSpace);
        }
        TableSpaceManager manager = tablesSpaces.get(tableSpace);
        if (manager == null) {
            throw new StatementExecutionException("No such tableSpace " + tableSpace + " here. "
                + "Maybe the server is starting ");
        }
        if (errorIfNotLeader && !manager.isLeader()) {
            throw new NotLeaderException("node " + nodeId + " is not leader for tableSpace " + tableSpace);
        }
        return manager.scan(statement, context, transactionContext);
    }

    /**
     * Utility method for DML/DDL statements
     *
     * @param statement
     * @param transaction
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
                LOGGER.log(Level.SEVERE, "waiting for  " + tableSpace.name + ", uuid " + tableSpace.uuid + ", to be up withint " + createTableSpaceStatement.getWaitForTableSpaceTimeout() + " ms");
                final int timeout = createTableSpaceStatement.getWaitForTableSpaceTimeout();
                for (int i = 0; i < timeout; i += poolTime) {
                    List<TableSpaceReplicaState> replicateStates = metadataStorageManager.getTableSpaceReplicaState(tableSpace.uuid);
                    for (TableSpaceReplicaState ts : replicateStates) {
                        LOGGER.log(Level.SEVERE, "waiting for  " + tableSpace.name + ", uuid " + tableSpace.uuid + ", to be up, replica state node: " + ts.nodeId + ", state: " + TableSpaceReplicaState.modeToSQLString(ts.mode) + ", ts " + new java.sql.Timestamp(ts.timestamp));
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
        triggerActivator(ActivatorRunRequest.NOOP);
        try {
            activator.join();
        } catch (InterruptedException ignore) {
            ignore.printStackTrace();
        }
        threadPool.shutdown();
    }

    public void checkpoint() throws DataStorageManagerException, LogNotAvailableException {
        for (TableSpaceManager man : tablesSpaces.values()) {
            man.checkpoint();
        }
    }

    public void triggerActivator(ActivatorRunRequest type) {
        activatorQueue.offer(type);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getVirtualTableSpaceId() {
        return virtualTableSpaceId;
    }

    void submit(Runnable runnable) {
        try {
            threadPool.submit(runnable);
        } catch (RejectedExecutionException err) {
            LOGGER.log(Level.SEVERE, "rejected " + runnable, err);
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

    public void dumpTableSpace(String tableSpace, String dumpId, Message message, Channel _channel, int fetchSize, boolean includeLog) {
        TableSpaceManager manager = tablesSpaces.get(tableSpace);
        if (manager == null) {
            _channel.sendReplyMessage(message, Message.ERROR(null, new Exception("tableSpace " + tableSpace + " not booted here")));
            return;
        } else {
            _channel.sendReplyMessage(message, Message.ACK(null));
        }
        try {
            manager.dumpTableSpace(dumpId, _channel, fetchSize, includeLog);
        } catch (Exception error) {
            LOGGER.log(Level.SEVERE, "error on dump", error);
        }
    }

    private String makeVirtualTableSpaceManagerId(String nodeId) {
        return nodeId.replace(":", "").replace(".", "").toLowerCase();
    }

    private void tryBecomeLeaderFor(TableSpace tableSpace) throws DDLException, MetadataStorageManagerException {
        LOGGER.log(Level.SEVERE, "node {0}, try to become leader of {1}", new Object[]{nodeId, tableSpace.name});
        TableSpace.Builder newTableSpaceBuilder
            = TableSpace
                .builder()
                .cloning(tableSpace)
                .leader(nodeId);
        TableSpace newTableSpace = newTableSpaceBuilder.build();
        boolean ok = metadataStorageManager.updateTableSpace(newTableSpace, tableSpace);
        if (!ok) {
            LOGGER.log(Level.SEVERE, "updating tableSpace {0} metadata failed", tableSpace.name);
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

        @Override
        public void run() {
            try {
                while (!stopped.get()) {
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
                        LOGGER.log(Level.SEVERE, "error during shutdown of manager of tablespace " + manager.getKey(), err);
                    }
                }
            } finally {
                generalLock.writeLock().unlock();
            }
            try {
                dataStorageManager.close();
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error during shutdown", err);
            }
            try {
                metadataStorageManager.close();
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error during shutdown", err);
            }

            try {
                commitLogManager.close();
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error during shutdown", err);
            }
            LOGGER.log(Level.SEVERE, "{0} activator stopped", nodeId);

        }

    }

    private void executeActivator(ActivatorRunRequest type) {
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
                    LOGGER.log(Level.SEVERE, "checkpoint failed:" + error, error);
                }
            }
        }
        if (!checkpointDone && type.enableTableCheckPoints()) {
            for (TableSpaceManager man : tablesSpaces.values()) {
                man.runLocalTableCheckPoints();
            }
        }
    }

    private boolean manageTableSpaces() {
        Collection<String> actualTablesSpaces;
        try {
            actualTablesSpaces = metadataStorageManager.listTableSpaces();
        } catch (MetadataStorageManagerException error) {
            LOGGER.log(Level.SEVERE, "cannot access tablespaces metadata", error);
            return true;
        }
        Map<String, TableSpace> actualTableSpaceMetadata = new HashMap<>();
        generalLock.writeLock().lock();
        try {
            for (String tableSpace : actualTablesSpaces) {
                TableSpace tableSpaceMetadata = metadataStorageManager.describeTableSpace(tableSpace);
                actualTableSpaceMetadata.put(tableSpaceMetadata.uuid, tableSpaceMetadata);
                try {
                    handleTableSpace(tableSpaceMetadata);
                } catch (Exception err) {
                    LOGGER.log(Level.SEVERE, "cannot handle tablespace " + tableSpace, err);
                    if (haltOnTableSpaceBootError && haltProcedure != null) {
                        err.printStackTrace();
                        haltProcedure.run();
                    }
                }
            }
        } catch (MetadataStorageManagerException error) {
            LOGGER.log(Level.SEVERE, "cannot access tablespaces metadata", error);
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
                    failedTableSpaces.add(entry.getKey());
                } else if (!entry.getKey().equals(virtualTableSpaceId) && !actualTablesSpaces.contains(entry.getKey())) {
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
                LOGGER.log(Level.SEVERE, "cannot access tablespace " + entry.getKey() + " metadata", error);
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
                LOGGER.log(Level.SEVERE, "cannot access tablespace metadata", error);
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
                    TableSpace tableSpaceInfo = actualTableSpaceMetadata.get(tableSpaceUuid);

                    if (tableSpaceInfo != null
                        && tableSpaceInfo.maxLeaderInactivityTime > 0
                        && !tableSpaceManager.isFailed()) {
                        List<TableSpaceReplicaState> allReplicas
                            = metadataStorageManager.getTableSpaceReplicaState(tableSpaceUuid);
                        TableSpaceReplicaState leaderState = allReplicas
                            .stream()
                            .filter(t -> t.mode == TableSpaceReplicaState.MODE_LEADER
                            && !t.nodeId.equals(nodeId)
                            )
                            .findAny()
                            .orElse(null);
                        if (leaderState == null) {
                            LOGGER.log(Level.SEVERE, "Leader for " + tableSpaceUuid + " should be " + tableSpaceInfo.leaderId + ", but it never sent pings or it disappeared");
                            tryBecomeLeaderFor(tableSpaceInfo);
                        } else {
                            long delta = now - leaderState.timestamp;
                            if (tableSpaceInfo.maxLeaderInactivityTime > delta) {
                                LOGGER.log(Level.FINER, "Leader for " + tableSpaceUuid + " is " + leaderState.nodeId + ", last ping " + new java.sql.Timestamp(leaderState.timestamp) + ". leader is healty");
                            } else {
                                LOGGER.log(Level.SEVERE, "Leader for " + tableSpaceUuid + " is " + leaderState.nodeId + ", last ping " + new java.sql.Timestamp(leaderState.timestamp) + ". leader is failed. trying to take leadership");
                                tryBecomeLeaderFor(tableSpaceInfo);
                                // only one change at a time
                                break;
                            }
                        }
                    }
                }
            } catch (MetadataStorageManagerException | DDLException error) {
                LOGGER.log(Level.SEVERE, "cannot access tablespace metadata", error);
                return true;
            }
        }
        return false;
    }

    private void stopTableSpace(String tableSpace, String uuid) throws MetadataStorageManagerException {
        LOGGER.log(Level.SEVERE, "stopTableSpace " + tableSpace + " uuid " + uuid + ", on " + nodeId);
        try {
            tablesSpaces.get(tableSpace).close();
        } catch (LogNotAvailableException err) {
            LOGGER.log(Level.SEVERE, "node " + nodeId + " cannot close for reboot tablespace " + tableSpace, err);
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

    public long getLastCheckPointTs() {
        return lastCheckPointTs.get();
    }

    @Override
    public void metadataChanged() {
        LOGGER.log(Level.SEVERE, "metadata changed");
        triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
    }

    public boolean isClearAtBoot() {
        return clearAtBoot;
    }

    public void setClearAtBoot(boolean clearAtBoot) {
        this.clearAtBoot = clearAtBoot;
    }

}
