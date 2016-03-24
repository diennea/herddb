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
package herddb.mem;

import herddb.metadata.MetadataStorageManager;
import herddb.model.DDLException;
import herddb.model.InvalidTableException;
import herddb.model.Table;
import herddb.model.TableAlreadyExistsException;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TableSpaceAlreadyExistsException;
import herddb.model.TableSpaceDoesNotExistException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * In-memory MetadataStorageManager implementation. For tests
 *
 * @author enrico.olivelli
 */
public class MemoryMetadataStorageManager extends MetadataStorageManager {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, TableSpace> tableSpaces = new HashMap<>();
    private final Map<String, Table> tables = new HashMap<>();

    @Override
    public Collection<String> listTableSpaces() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(tableSpaces.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Collection<String> listTablesByTableSpace(String tableSpace) {
        lock.readLock().lock();
        try {
            return tables.values().stream().filter(s -> {
                return s.tablespace.equals(tableSpace);
            }).map(t -> {
                return t.name;
            }).collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public TableSpace describeTableSpace(String name) {
        lock.readLock().lock();
        try {
            return tableSpaces.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Table describeTable(String name) {
        lock.readLock().lock();
        try {
            return tables.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void registerTable(Table table) throws DDLException {
        validateTable(table);
        lock.writeLock().lock();
        try {
            if (!tableSpaces.containsKey(table.tablespace)) {
                throw new TableSpaceDoesNotExistException(table.tablespace);
            }
            if (tables.putIfAbsent(table.name, table) != null) {
                throw new TableAlreadyExistsException(table.name);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void registerTableSpace(TableSpace tableSpace) throws DDLException {
        validateTableSpace(tableSpace);
        lock.writeLock().lock();
        try {
            if (tableSpaces.putIfAbsent(tableSpace.name, tableSpace) != null) {
                throw new TableSpaceAlreadyExistsException(tableSpace.name);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void updateTable(Table table) throws DDLException {
        validateTable(table);
        lock.writeLock().lock();
        try {
            Table prev = tables.get(table.name);
            if (prev == null) {
                throw new TableDoesNotExistException(table.name);
            }
            if (!prev.tablespace.equals(table.tablespace)) {
                throw new InvalidTableException("cannot change tablespace");
            }
            if (!tableSpaces.containsKey(table.tablespace)) {
                throw new TableSpaceDoesNotExistException(table.tablespace);
            }
            tables.put(table.name, table);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void updateTableSpace(TableSpace tableSpace) throws DDLException {
        validateTableSpace(tableSpace);
        lock.writeLock().lock();
        try {
            TableSpace prev = tableSpaces.get(tableSpace.name);
            if (prev == null) {
                throw new TableSpaceDoesNotExistException(prev.name);
            }
            tableSpaces.put(tableSpace.name, tableSpace);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void validateTableSpace(TableSpace tableSpace) throws DDLException {
        // TODO: implement sensible validations
        if (tableSpace.name == null) {
            throw new InvalidTableException("null tablespace name");
        }
    }

    private void validateTable(Table table) throws DDLException {
        // TODO: implement sensible validations
        if (table.name == null) {
            throw new InvalidTableException("null table name");
        }
    }

}
