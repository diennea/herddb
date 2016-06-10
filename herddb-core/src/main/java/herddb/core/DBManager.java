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

import herddb.client.ClientConfiguration;
import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.log.LogNotAvailableException;
import herddb.metadata.MetadataChangeListener;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
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
import herddb.model.TableSpace;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TransactionContext;
import herddb.model.Tuple;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.ScanStatement;
import herddb.network.Channel;
import herddb.network.Message;
import herddb.network.ServerHostData;
import herddb.sql.SQLTranslator;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import java.nio.file.Path;
import java.util.ArrayList;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
    private final ReentrantReadWriteLock generalLock = new ReentrantReadWriteLock();
    private final Thread activator;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final BlockingQueue<Object> activatorQueue = new LinkedBlockingDeque<>();
    private final SQLTranslator translator;
    private final Path tmpDirectory;
    private final RecordSetFactory recordSetFactory;
    private final ServerHostData hostData;
    private String serverToServerUsername = ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT;
    private String serverToServerPassword = ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT;
    private boolean errorIfNotLeader = true;
    private final ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, r + "");
            t.setDaemon(true);
            return t;
        }
    });

    public boolean isErrorIfNotLeader() {
        return errorIfNotLeader;
    }

    public void setErrorIfNotLeader(boolean errorIfNotLeader) {
        this.errorIfNotLeader = errorIfNotLeader;
    }

    public MetadataStorageManager getMetadataStorageManager() {
        return metadataStorageManager;
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

    public DBManager(String nodeId, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager, CommitLogManager commitLogManager, Path tmpDirectory, herddb.network.ServerHostData hostData) {
        this.tmpDirectory = tmpDirectory;
        this.recordSetFactory = dataStorageManager.createRecordSetFactory();
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.commitLogManager = commitLogManager;
        this.nodeId = nodeId;
        this.hostData = hostData != null ? hostData : new ServerHostData("localhost", 7000, "", false, new HashMap<>());
        this.translator = new SQLTranslator(this);
        this.activator = new Thread(new Activator(), "hdb-" + nodeId + "-activator");
        this.activator.setDaemon(true);
    }

    public SQLTranslator getTranslator() {
        return translator;
    }

    /**
     * Initial boot of the system
     *
     * @throws herddb.storage.DataStorageManagerException
     * @throws herddb.log.LogNotAvailableException
     * @throws herddb.metadata.MetadataStorageManagerException
     */
    public void start() throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException {

        metadataStorageManager.start();
        metadataStorageManager.setMetadataChangeListener(this);
        metadataStorageManager.registerNode(NodeMetadata
                .builder()
                .host(hostData.getHost())
                .port(hostData.getPort())
                .ssl(hostData.isSsl())
                .nodeId(nodeId)
                .build());

        metadataStorageManager.ensureDefaultTableSpace(nodeId);

        generalLock.writeLock().lock();
        try {
            dataStorageManager.start();
        } finally {
            generalLock.writeLock().unlock();
        }
        activator.start();

        triggerActivator();
    }

    public boolean waitForTablespace(String tableSpace, int millis) throws InterruptedException {
        return waitForTablespace(tableSpace, millis, true);
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
            stopTableSpace(tableSpaceName);
        }

        if (actual_manager != null && !actual_manager.isLeader() && tableSpace.leaderId.equals(nodeId)) {
            LOGGER.log(Level.SEVERE, "Tablespace {0} need to switch to leadership on node {1}", new Object[]{tableSpaceName, nodeId});
            stopTableSpace(tableSpaceName);
        }

        if (tableSpace.replicas.contains(nodeId) && !tablesSpaces.containsKey(tableSpaceName)) {
            LOGGER.log(Level.SEVERE, "Booting tablespace {0} on {1}", new Object[]{tableSpaceName, nodeId});
            CommitLog commitLog = commitLogManager.createCommitLog(tableSpaceName);
            TableSpaceManager manager = new TableSpaceManager(nodeId, tableSpaceName, metadataStorageManager, dataStorageManager, commitLog, this);
            try {
                manager.start();
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
            stopTableSpace(tableSpaceName);
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

            TableSpaceManager manager;
            generalLock.readLock().lock();
            try {
                manager = tablesSpaces.get(tableSpace);
            } finally {
                generalLock.readLock().unlock();
            }
            if (manager == null) {
                throw new StatementExecutionException("not such tableSpace " + tableSpace + " here");
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
        if (plan.mainStatement instanceof ScanStatement) {
            DataScanner result = scan((ScanStatement) plan.mainStatement, context, transactionContext);
            if (plan.mutator != null) {
                return executeMutatorPlan(result, plan, context, transactionContext);
            } else {
                return executeDataScannerPlan(plan, result, transactionContext);
            }
        } else {
            return executeStatement(plan.mainStatement, context, transactionContext);
        }

    }

    private StatementExecutionResult executeDataScannerPlan(ExecutionPlan plan, DataScanner result, TransactionContext transactionContext) throws StatementExecutionException {
        ScanResult scanResult;
        if (plan.mainAggregator != null) {
            scanResult = new ScanResult(plan.mainAggregator.aggregate(result));
        } else {
            scanResult = new ScanResult(result);
        }
        if (plan.comparator != null) {
            // SORT is to be applied before limits
            MaterializedRecordSet sortedSet = recordSetFactory.createRecordSet(scanResult.dataScanner.getSchema());
            try {
                scanResult.dataScanner.forEach(sortedSet::add);
                sortedSet.writeFinished();
                sortedSet.sort(plan.comparator);
                scanResult.dataScanner.close();
                scanResult = new ScanResult(new SimpleDataScanner(sortedSet));
            } catch (DataScannerException err) {
                throw new StatementExecutionException(err);
            }
        }
        if (plan.limits != null) {
            try {
                return new ScanResult(new LimitedDataScanner(scanResult.dataScanner, plan.limits));
            } catch (DataScannerException limitError) {
                throw new StatementExecutionException(limitError);
            }
        } else {
            return scanResult;
        }
    }

    private StatementExecutionResult executeMutatorPlan(DataScanner result, ExecutionPlan plan, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        try {
            int updateCount = 0;
            try {
                while (result.hasNext()) {
                    Tuple next = result.next();
                    context.setCurrentTuple(next);
                    try {
                        DMLStatementExecutionResult executeUpdate = executeUpdate(plan.mutator, context, transactionContext);
                        updateCount += executeUpdate.getUpdateCount();
                    } finally {
                        context.setCurrentTuple(null);
                    }
                }
                return new DMLStatementExecutionResult(updateCount);
            } finally {
                result.close();
            }
        } catch (DataScannerException ex) {
            throw new StatementExecutionException(ex);
        }
    }

    public DataScanner scan(ScanStatement statement, StatementEvaluationContext context, TransactionContext transactionContext) throws StatementExecutionException {
        context.setManager(this);
        context.setTransactionContext(transactionContext);
        String tableSpace = statement.getTableSpace();
        if (tableSpace == null) {
            throw new StatementExecutionException("invalid tableSpace " + tableSpace);
        }
        TableSpaceManager manager;
        generalLock.readLock().lock();
        try {
            manager = tablesSpaces.get(tableSpace);
        } finally {
            generalLock.readLock().unlock();
        }
        if (manager == null) {
            throw new StatementExecutionException("not such tableSpace " + tableSpace + " here");
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
            tableSpace = TableSpace.builder().leader(createTableSpaceStatement.getLeaderId()).name(createTableSpaceStatement.getTableSpace()).replicas(createTableSpaceStatement.getReplicas()).expectedReplicaCount(createTableSpaceStatement.getExpectedReplicaCount()).build();
        } catch (IllegalArgumentException invalid) {
            throw new StatementExecutionException("invalid CREATE TABLESPACE statement: " + invalid.getMessage(), invalid);
        }

        try {
            metadataStorageManager.registerTableSpace(tableSpace);
            triggerActivator();
            return new DDLStatementExecutionResult();
        } catch (StatementExecutionException err) {
            throw err;
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }
    }

    @Override
    public void close() throws DataStorageManagerException {
        stopped.set(true);
        triggerActivator();
        try {
            activator.join();
        } catch (InterruptedException ignore) {
            ignore.printStackTrace();
        }
        threadPool.shutdown();
    }

    public void checkpoint() throws DataStorageManagerException, LogNotAvailableException {

        List<TableSpaceManager> managers;
        generalLock.readLock().lock();
        try {
            managers = new ArrayList<>(tablesSpaces.values());
        } finally {
            generalLock.readLock().unlock();
        }
        for (TableSpaceManager man : managers) {
            man.checkpoint();
        }
    }

    private void triggerActivator() {
        activatorQueue.offer("");
    }

    public String getNodeId() {
        return nodeId;
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
            tableSpace = TableSpace.builder().leader(
                    alterTableSpaceStatement.getLeaderId())
                    .name(alterTableSpaceStatement.getTableSpace())
                    .replicas(alterTableSpaceStatement.getReplicas())
                    .expectedReplicaCount(alterTableSpaceStatement.getExpectedReplicaCount())
                    .build();
        } catch (IllegalArgumentException invalid) {
            throw new StatementExecutionException("invalid ALTER TABLESPACE statement: " + invalid.getMessage(), invalid);
        }

        try {
            TableSpace previous = metadataStorageManager.describeTableSpace(alterTableSpaceStatement.getTableSpace());
            if (previous == null) {
                throw new TableSpaceDoesNotExistException(alterTableSpaceStatement.getTableSpace());
            }
            metadataStorageManager.updateTableSpace(tableSpace, previous);
            triggerActivator();
            return new DDLStatementExecutionResult();
        } catch (Exception err) {
            throw new StatementExecutionException(err);
        }
    }

    public void dumpTableSpace(String tableSpace, String dumpId, Message message, Channel _channel, int fetchSize) {
        TableSpaceManager manager = tablesSpaces.get(tableSpace);
        if (manager == null) {
            _channel.sendReplyMessage(message, Message.ERROR(null, new Exception("tableSpace " + tableSpace + " not booted here")));
            return;
        } else {
            _channel.sendReplyMessage(message, Message.ACK(null));
        }
        try {
            manager.dumpTableSpace(dumpId, _channel, fetchSize);
        } catch (DataStorageManagerException | LogNotAvailableException error) {
            LOGGER.log(Level.SEVERE, "error before dump", error);
            _channel.sendReplyMessage(message, Message.ERROR(null, new Exception("internal error " + error, error)));
        }
    }

    private class Activator implements Runnable {

        @Override
        public void run() {
            try {
                while (!stopped.get()) {
                    activatorQueue.poll(1, TimeUnit.SECONDS);
                    activatorQueue.clear();
                    if (!stopped.get()) {
                        generalLock.writeLock().lock();
                        try {
                            Collection<String> actualTablesSpaces = metadataStorageManager.listTableSpaces();

                            for (String tableSpace : actualTablesSpaces) {
                                TableSpace tableSpaceMetadata = metadataStorageManager.describeTableSpace(tableSpace);
                                try {
                                    handleTableSpace(tableSpaceMetadata);
                                } catch (Exception err) {
                                    LOGGER.log(Level.SEVERE, "cannot handle tablespace " + tableSpace, err);
                                }
                            }
                        } catch (MetadataStorageManagerException error) {
                            LOGGER.log(Level.SEVERE, "cannot access tablespace metadata", error);
                        } finally {
                            generalLock.writeLock().unlock();
                        }
                        Set<String> failedTableSpaces = new HashSet<>();
                        for (Map.Entry<String, TableSpaceManager> entry : tablesSpaces.entrySet()) {
                            if (entry.getValue().isFailed()) {
                                failedTableSpaces.add(entry.getKey());
                            }
                        }
                        if (!failedTableSpaces.isEmpty()) {
                            generalLock.writeLock().lock();
                            try {
                                for (String tableSpace : failedTableSpaces) {
                                    stopTableSpace(tableSpace);
                                }
                            } finally {
                                generalLock.writeLock().unlock();
                            }
                        }
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
            LOGGER.log(Level.SEVERE, "{0} activator stopped", nodeId);

        }

    }

    private void stopTableSpace(String tableSpace) {
        LOGGER.log(Level.SEVERE, "stopTableSpace " + tableSpace + " on " + nodeId);
        try {
            tablesSpaces.get(tableSpace).close();
        } catch (LogNotAvailableException err) {
            LOGGER.log(Level.SEVERE, "node " + nodeId + " cannot close for reboot tablespace " + tableSpace, err);
        }
        tablesSpaces.remove(tableSpace);
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

    @Override
    public void metadataChanged() {
        LOGGER.log(Level.SEVERE, "metadata changed");
        triggerActivator();
    }

}
