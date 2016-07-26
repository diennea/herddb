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

import herddb.index.IndexOperation;
import herddb.index.KeyToPageIndex;
import herddb.log.CommitLog;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.TableContext;
import herddb.model.Transaction;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Index runtime
 *
 * @author enrico.olivelli
 */
public abstract class AbstractIndexManager implements AutoCloseable {

    protected final Index index;
    protected final AbstractTableManager tableManager;
    protected final DataStorageManager dataStorageManager;
    protected final String tableSpaceUUID;
    protected final CommitLog log;

    public abstract void start() throws DataStorageManagerException;

    public AbstractIndexManager(Index index, AbstractTableManager tableManager, DataStorageManager dataStorageManager, String tableSpaceUUID, CommitLog log) {
        this.index = index;
        this.tableManager = tableManager;
        this.dataStorageManager = dataStorageManager;
        this.tableSpaceUUID = tableSpaceUUID;
        this.log = log;
    }

    public final Index getIndex() {
        return index;
    }

    public final String getIndexName() {
        return index.name;
    }

    public final String[] getColumnNames() {
        return index.columnNames;
    }

    @Override
    public void close() {
    }

    /**
     * Rebuild entirely the index, usually performing a full table scan
     *
     * @param tableManager
     * @throws DataStorageManagerException
     */
    public void rebuild() throws DataStorageManagerException {
    }

    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        return Collections.emptyList();
    }

    public Stream<Bytes> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Stream<Map.Entry<Bytes, Long>> recordSetScanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext, KeyToPageIndex keyToPageIndex) throws DataStorageManagerException {
        return scanner(operation, context, tableContext).map((Bytes b) -> {
            Long idPage = keyToPageIndex.get(b);
            if (idPage == null) {
                return null;
            }
            return (Map.Entry<Bytes, Long>) new AbstractMap.SimpleImmutableEntry<>(b, idPage);
        }).filter(p -> p != null);
    }

    public void recordUpdated(Bytes key, Map<String, Object> previousValues, Map<String, Object> newValues) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void recordInserted(Bytes key, Map<String, Object> values) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void recordDeleted(Bytes key, Map<String, Object> values) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
