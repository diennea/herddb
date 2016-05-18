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

import herddb.client.HDBConnection;
import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * In memory StorageManager, for tests
 *
 * @author enrico.olivelli
 */
public class MemoryDataStorageManager extends DataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(MemoryDataStorageManager.class.getName());

    public static final class Page {

        private final List<Record> records;
        private final LogSequenceNumber sequenceNumber;

        public Page(List<Record> records, LogSequenceNumber sequenceNumber) {
            this.records = records;
            this.sequenceNumber = sequenceNumber;
        }

        public List<Record> getRecords() {
            return records;
        }

        public LogSequenceNumber getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public String toString() {
            return "Page{" + "records=" + records.size() + ", sequenceNumber=" + sequenceNumber + '}';
        }

    }
    private final ConcurrentHashMap<String, Page> pages = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Bytes>> keysByPage = new ConcurrentHashMap<>();
    private final AtomicLong newPageId = new AtomicLong();
    private final ConcurrentHashMap<String, List<Table>> tablesByTablespace = new ConcurrentHashMap<>();

    @Override
    public int getActualNumberOfPages(String tableSpace, String tableName) throws DataStorageManagerException {
        int res = 0;
        for (String key : pages.keySet()) {
            if (key.startsWith(tableName + "_")) {
                res++;
            }
        }
        return res;
    }

    public Page getPage(String tableName, Long pageId) {
        return pages.get(tableName + "_" + pageId);
    }

    @Override
    public List<Record> loadPage(String tableSpace, String tableName, Long pageId) {
        Page page = pages.get(tableName + "_" + pageId);
        LOGGER.log(Level.SEVERE, "loadPage " + tableName + " " + pageId + " -> " + page);
        return page != null ? page.records : null;
    }

    @Override
    public void restore(String tableSpace, String tableName, Consumer<TableStatus> tableStatusConsumer, BiConsumer<Bytes, Long> consumer) {
        // AT BOOT NO DATA IS PRESENT
    }

    @Override
    public Long writePage(String tableSpace, String tableName, TableStatus tableStatus, List<Record> newPage) {
        long pageId = newPageId.incrementAndGet();
        Page page = new Page(new ArrayList<>(newPage), tableStatus.sequenceNumber);
        pages.put(tableName + "_" + pageId, page);
        LOGGER.log(Level.SEVERE, "writePage " + tableName + " " + pageId + " -> " + newPage);
        return pageId;
    }

    @Override
    public void start() throws DataStorageManagerException {

    }

    @Override
    public void close() throws DataStorageManagerException {
        this.pages.clear();
        this.keysByPage.clear();
    }

    @Override
    public List<Table> loadTables(LogSequenceNumber sequenceNumber, String tableSpace) throws DataStorageManagerException {
        List<Table> res = tablesByTablespace.get(tableSpace);
        if (res != null) {
            return Collections.unmodifiableList(res);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public void writeTables(String tableSpace, LogSequenceNumber sequenceNumber, List<Table> tables) throws DataStorageManagerException {

        tables.forEach((t) -> {
            if (!t.tablespace.equals(tableSpace)) {
                throw new IllegalArgumentException("illegal tablespace");
            }
        });
        List<Table> res = tablesByTablespace.get(tableSpace);
        if (res == null) {
            this.tablesByTablespace.put(tableSpace, new ArrayList<>(tables));
        } else {
            res.addAll(tables);
        }
    }

    @Override
    public void writeCheckpointSequenceNumber(String tableSpace, LogSequenceNumber sequenceNumber) throws DataStorageManagerException {

    }

    @Override
    public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace) throws DataStorageManagerException {
        return LogSequenceNumber.START_OF_TIME;
    }

    @Override
    public void dropTable(String tablespace, String name) throws DataStorageManagerException {

    }

    @Override
    public void downloadTable(String tableSpaceName, String table, HDBConnection con) throws DataStorageManagerException {
    }

}
