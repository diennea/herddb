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

import herddb.log.CommitLog;
import herddb.log.CommitLogManager;
import herddb.log.LogNotAvailableException;
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
import herddb.model.ScanResult;
import herddb.model.TableSpace;
import herddb.model.Statement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.SQLTranslator;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
public class DBManager implements AutoCloseable {

    private final Logger LOGGER = Logger.getLogger(DBManager.class.getName());
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
    private final ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, r + "");
            t.setDaemon(true);
            return t;
        }
    });

    public DBManager(String nodeId, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager, CommitLogManager commitLogManager) {
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.commitLogManager = commitLogManager;
        this.nodeId = nodeId;
        this.translator = new SQLTranslator(this);
        this.activator = new Thread(new Activator(), "hdb-" + nodeId + "-activator");
        this.activator.setDaemon(true);
    }

    public SQLTranslator getTranslator() {
        return translator;
    }

    /**
     * Initial boot of the system
     */
    public void start() throws DataStorageManagerException, LogNotAvailableException, MetadataStorageManagerException {

        metadataStorageManager.start();
        metadataStorageManager.registerNode(NodeMetadata.builder().nodeId(nodeId).build());

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
        if (tableSpace.replicas.contains(nodeId) && !tablesSpaces.containsKey(tableSpaceName)) {
            LOGGER.log(Level.SEVERE, "Booting tablespace {0} on {1}", new Object[]{tableSpaceName, nodeId});
            CommitLog commitLog = commitLogManager.createCommitLog(tableSpaceName);
            TableSpaceManager manager = new TableSpaceManager(nodeId, tableSpaceName, metadataStorageManager, dataStorageManager, commitLog, this);
            tablesSpaces.put(tableSpaceName, manager);
            manager.start();
        } else {
            if (tablesSpaces.containsKey(tableSpaceName) && !tableSpace.replicas.contains(nodeId)) {
                stopTableSpace(tableSpaceName);
            }
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

    public StatementExecutionResult executeStatement(Statement statement) throws StatementExecutionException {
        return executeStatement(statement, new StatementEvaluationContext());
    }

    public StatementExecutionResult executeStatement(Statement statement, StatementEvaluationContext context) throws StatementExecutionException {
        LOGGER.log(Level.FINEST, "executeStatement {0}", new Object[]{statement});
        String tableSpace = statement.getTableSpace();
        if (tableSpace == null) {
            throw new StatementExecutionException("invalid tableSpace " + tableSpace);
        }
        try {
            if (statement instanceof CreateTableSpaceStatement) {
                if (statement.getTransactionId() > 0) {
                    throw new StatementExecutionException("CREATE TABLESPACE cannot be issued inside a transaction");
                }
                return createTableSpace((CreateTableSpaceStatement) statement);
            }

            if (statement instanceof AlterTableSpaceStatement) {
                if (statement.getTransactionId() > 0) {
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
            return manager.executeStatement(statement, context);
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
    public GetResult get(GetStatement statement) throws StatementExecutionException {
        return (GetResult) executeStatement(statement, new StatementEvaluationContext());
    }

    public GetResult get(GetStatement statement, StatementEvaluationContext context) throws StatementExecutionException {
        return (GetResult) executeStatement(statement, context);
    }

    public DataScanner scan(ScanStatement statement) throws StatementExecutionException {
        return scan(statement, new StatementEvaluationContext());
    }

    public StatementExecutionResult executePlan(ExecutionPlan plan, StatementEvaluationContext context) throws StatementExecutionException {
        if (plan.mainStatement instanceof ScanStatement) {
            DataScanner result = scan((ScanStatement) plan.mainStatement, context);
            ScanResult scanResult;
            if (plan.mainAggregator != null) {
                scanResult = new ScanResult(plan.mainAggregator.aggregate(result));
            } else {
                scanResult = new ScanResult(result);
            }
            if (plan.comparator != null) {
                // SORT is to by applied before limits                
                MaterializedRecordSet sortedSet = new MaterializedRecordSet(scanResult.dataScanner.getSchema());
                try {
                    scanResult.dataScanner.forEach(sortedSet.records::add);
                    sortedSet.sort(plan.comparator);
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
        } else {
            return executeStatement(plan.mainStatement, context);
        }

    }

    public DataScanner scan(ScanStatement statement, StatementEvaluationContext context) throws StatementExecutionException {
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
        return manager.scan(statement, context);
    }

    /**
     * Utility method for DML/DDL statements
     *
     * @param statement
     * @param transaction
     * @return
     * @throws herddb.model.StatementExecutionException
     */
    public DMLStatementExecutionResult executeUpdate(DMLStatement statement) throws StatementExecutionException {
        return (DMLStatementExecutionResult) executeStatement(statement, new StatementEvaluationContext());
    }

    public DMLStatementExecutionResult executeUpdate(DMLStatement statement, StatementEvaluationContext context) throws StatementExecutionException {
        return (DMLStatementExecutionResult) executeStatement(statement, context);
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

    public void flush() throws DataStorageManagerException {

        List<TableSpaceManager> managers;
        generalLock.readLock().lock();
        try {
            managers = new ArrayList<>(tablesSpaces.values());
        } finally {
            generalLock.readLock().unlock();
        }
        for (TableSpaceManager man : managers) {
            man.flush();
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
        try {
            tablesSpaces.get(tableSpace).close();
        } catch (LogNotAvailableException err) {
            LOGGER.log(Level.SEVERE, "cannot close for reboot tablespace " + tableSpace, err);
        }
        tablesSpaces.remove(tableSpace);
    }

    public TableSpaceManager getTableSpaceManager(String tableSpace) {
        return tablesSpaces.get(tableSpace);
    }
}
