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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Logger;

import herddb.core.PostCheckpointAction;
import herddb.core.RecordSetFactory;
import herddb.index.ConcurrentMapKeyToPageIndex;
import herddb.index.KeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullIndexScanConsumer;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataOutputStream;

/**
 * In memory StorageManager, for tests
 *
 * @author enrico.olivelli
 */
public class MemoryDataStorageManager extends DataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(MemoryDataStorageManager.class.getName());

    public static final class Page {

        private final List<Record> records;

        public Page(List<Record> records) {
            this.records = records;
        }

        public List<Record> getRecords() {
            return records;
        }

        @Override
        public String toString() {
            return "Page{" + "records=" + records.size();
        }

    }
    private final ConcurrentHashMap<String, Page> pages = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Bytes> indexpages = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Bytes>> keysByPage = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<Table>> tablesByTablespace = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<Index>> indexesByTablespace = new ConcurrentHashMap<>();

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
    public List<Record> readPage(String tableSpace, String tableName, Long pageId) {
        Page page = pages.get(tableName + "_" + pageId);
        //LOGGER.log(Level.SEVERE, "loadPage " + tableName + " " + pageId + " -> " + page);
        return page != null ? page.records : null;
    }

    @Override
    public byte[] readIndexPage(String tableSpace, String indexName, Long pageId) throws DataStorageManagerException {
        Bytes page = indexpages.get(indexName + "_" + pageId);
        //LOGGER.log(Level.SEVERE, "loadPage " + tableName + " " + pageId + " -> " + page);
        return page != null ? page.data : null;
    }

    @Override
    public void fullTableScan(String tableSpace, String tableName, FullTableScanConsumer consumer) throws DataStorageManagerException {
        consumer.acceptTableStatus(new TableStatus(tableName, LogSequenceNumber.START_OF_TIME, Bytes.from_long(1).data, 1, new HashSet<>()));
    }

    @Override
    public void fullIndexScan(String tableSpace, String tableName, FullIndexScanConsumer consumer) throws DataStorageManagerException {
        consumer.acceptIndexStatus(new IndexStatus(tableName, LogSequenceNumber.START_OF_TIME, null, null));
    }

    @Override
    public void writePage(String tableSpace, String tableName, long pageId, Collection<Record> newPage) throws DataStorageManagerException {
        Page page = new Page(new ArrayList<>(newPage));
        Page prev = pages.put(tableName + "_" + pageId, page);
        //LOGGER.log(Level.SEVERE, "writePage " + tableName + " " + pageId + " -> " + newPage);
    }

    @Override
    public void writeIndexPage(String tableSpace, String tableName, long pageId, byte[] page) throws DataStorageManagerException {
        Bytes page_wrapper = Bytes.from_array(page);
        indexpages.put(tableName + "_" + pageId, page_wrapper);
    }

    @Override
    public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String tableName, TableStatus tableStatus) throws DataStorageManagerException {

        List<Long> pagesForTable = new ArrayList<>();
        String prefix = tableName + "_";
        for (String key : pages.keySet()) {
            if (key.startsWith(prefix)) {
                long pageId = Long.parseLong(key.substring(prefix.length()));
                pagesForTable.add(pageId);
            }
        }

        pagesForTable.removeAll(tableStatus.activePages);
        List<PostCheckpointAction> result = new ArrayList<>();

        for (long pageId : pagesForTable) {
            result.add(new PostCheckpointAction(tableName, "drop page " + pageId) {
                @Override
                public void run() {
                    // remove only after checkpoint completed
                    pages.remove(prefix + pageId);
                }
            });
        }
        return result;
    }

    @Override
    public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String tableName, IndexStatus indexStatus) throws DataStorageManagerException {
        return Collections.emptyList();
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
    public List<Index> loadIndexes(LogSequenceNumber sequenceNumber, String tableSpace) throws DataStorageManagerException {
        List<Index> res = indexesByTablespace.get(tableSpace);
        if (res != null) {
            return Collections.unmodifiableList(res);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public void writeTables(String tableSpace, LogSequenceNumber sequenceNumber, List<Table> tables, List<Index> indexlist) throws DataStorageManagerException {

        List<Table> tablesOnTableSpaces = tablesByTablespace.get(tableSpace);
        if (tablesOnTableSpaces == null) {
            this.tablesByTablespace.put(tableSpace, new ArrayList<>(tables));
        } else {
            tablesOnTableSpaces.addAll(tables);
        }

        List<Index> indexesOnTableSpaces = indexesByTablespace.get(tableSpace);
        if (indexesOnTableSpaces == null) {
            this.indexesByTablespace.put(tableSpace, new ArrayList<>(indexlist));
        } else {
            indexesOnTableSpaces.addAll(indexlist);
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
        List<Table> tables = tablesByTablespace.get(tablespace);
        if (tables != null) {
            for (Iterator<Table> it = tables.iterator(); it.hasNext();) {
                Table table = it.next();
                if (table.name.equals(name)) {
                    it.remove();
                }
            }
        }
    }

    @Override
    public void dropIndex(String tablespace, String name) throws DataStorageManagerException {
        List<Index> indexes = indexesByTablespace.get(tablespace);
        if (indexes != null) {
            for (Iterator<Index> it = indexes.iterator(); it.hasNext();) {
                Index index = it.next();
                if (index.name.equals(name)) {
                    it.remove();
                }
            }
        }
    }

    @Override
    public KeyToPageIndex createKeyToPageMap(String tablespace, String name) {
        return new ConcurrentMapKeyToPageIndex(new ConcurrentHashMap<>());
    }

    @Override
    public void releaseKeyToPageMap(String tablespace, String name, KeyToPageIndex keyToPage) {
        if (keyToPage != null) {
            ConcurrentMapKeyToPageIndex impl = (ConcurrentMapKeyToPageIndex) keyToPage;
            impl.getMap().clear();
        }
    }

    @Override
    public RecordSetFactory createRecordSetFactory() {
        return new MemoryRecordSetFactory();
    }

    @Override
    public void cleanupAfterBoot(String tablespace, String name, Set<Long> activePagesAtBoot
    ) {
    }

    @Override
    public void loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace, Consumer<Transaction> consumer) throws DataStorageManagerException {
    }

    @Override
    public void writeTransactionsAtCheckpoint(String tableSpace, LogSequenceNumber sequenceNumber, Collection<Transaction> transactions) throws DataStorageManagerException {
        try {
            for (Transaction t : transactions) {
                // test serialization
                t.serialize(new ExtendedDataOutputStream(new ByteArrayOutputStream()));
            }
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

}
