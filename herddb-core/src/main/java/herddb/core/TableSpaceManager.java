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
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogNotAvailableException;
import herddb.metadata.MetadataStorageManager;
import herddb.model.DDLStatementExecutionResult;
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableAwareStatement;
import herddb.model.Transaction;
import herddb.model.commands.CreateTableStatement;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages a TableSet in memory
 *
 * @author enrico.olivelli
 */
public class TableSpaceManager {

    private final MetadataStorageManager metadataStorageManager;
    private final DataStorageManager dataStorageManager;
    private final CommitLog log;
    private final String tableSpaceName;
    private final Map<String, TableManager> tables = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock generalLock = new ReentrantReadWriteLock();

    public TableSpaceManager(String tableSpaceName, MetadataStorageManager metadataStorageManager, DataStorageManager dataStorageManager, CommitLog log) {
        this.metadataStorageManager = metadataStorageManager;
        this.dataStorageManager = dataStorageManager;
        this.log = log;
        this.tableSpaceName = tableSpaceName;
    }

    void start() throws DataStorageManagerException {
        generalLock.writeLock().lock();
        try {
            for (String tableName : metadataStorageManager.listTablesByTableSpace(tableSpaceName)) {
                Table table = metadataStorageManager.describeTable(tableName);
                bootTable(table);
            }
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    StatementExecutionResult executeStatement(Statement statement, Transaction transaction) throws StatementExecutionException {
        if (statement instanceof CreateTableStatement) {
            return createTable((CreateTableStatement) statement, transaction);
        }
        if (statement instanceof TableAwareStatement) {
            TableAwareStatement st = (TableAwareStatement) statement;
            String table = st.getTable();
            TableManager manager;
            generalLock.readLock().lock();
            try {
                manager = tables.get(table);
            } finally {
                generalLock.readLock().unlock();
            }
            if (manager == null) {
                throw new StatementExecutionException("no table " + table + " in tablespace " + tableSpaceName);
            }
            return manager.executeStatement(statement, transaction);
        }

        throw new StatementExecutionException("unsupported statement " + statement);
    }

    private StatementExecutionResult createTable(CreateTableStatement statement, Transaction transaction) throws StatementExecutionException {
        try {
            generalLock.writeLock().lock();

            try {
                LogEntry entry = LogEntryFactory.createTable(statement.getTableDefinition(), transaction);
                log.log(entry);
            } catch (LogNotAvailableException ex) {
                throw new StatementExecutionException(ex);
            }

            Table table = statement.getTableDefinition();
            metadataStorageManager.registerTable(table);
            bootTable(table);
            return new DDLStatementExecutionResult();
        } catch (DataStorageManagerException err) {
            throw new StatementExecutionException(err);
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    private void bootTable(Table table) throws DataStorageManagerException {
        TableManager tableManager = new TableManager(table, log, dataStorageManager);
        tables.put(table.name, tableManager);
        tableManager.start();
    }

    public void close() {
        try {
            generalLock.writeLock().lock();
            for (TableManager table : tables.values()) {
                table.close();
            }
        } finally {
            generalLock.writeLock().unlock();
        }
    }

    void flush() throws DataStorageManagerException {
        List<TableManager> managers;
        try {
            generalLock.writeLock().lock();
            managers = new ArrayList<>(tables.values());
        } finally {
            generalLock.writeLock().unlock();
        }
        for (TableManager manager : managers) {
            manager.flush();
        }
    }

}
