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
package herddb.index;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import herddb.core.AbstractIndexManager;
import herddb.core.PostCheckpointAction;
import herddb.log.LogSequenceNumber;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;

/**
 * Index which maps every key of a table to the page which contains the key.
 *
 * @author enrico.olivelli
 */
public interface KeyToPageIndex extends AutoCloseable {

    public long getUsedMemory();

    public boolean requireLoadAtStartup();

    public long size();

    void start(LogSequenceNumber sequenceNumber) throws DataStorageManagerException;

    @Override
    public void close();

    /**
     * Ensures that all data is persisted to memory
     */
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException;

    /**
     * Unpin a previously pinned checkpont (see
     * {@link #checkpoint(LogSequenceNumber, boolean)})
     *
     * @throws DataStorageManagerException
     */
    public abstract void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException;

    public void truncate();

    public Stream<Map.Entry<Bytes, Long>> scanner(IndexOperation operation, StatementEvaluationContext context,
            TableContext tableContext, AbstractIndexManager index) throws DataStorageManagerException, StatementExecutionException;

    public void put(Bytes key, Long currentPage);

    /**
     * Attempt to put a new value in the index. The mapping will be update only if current mapping
     * matches given expected one (provide null if no mapping is expected).
     * <p>
     * I current mapping differs it will be left untouched
     * </p>
     *
     * @return {@code false} if the put wasn't executed
     */
    public boolean put(Bytes key, Long newPage, Long expectedPage);

    public boolean containsKey(Bytes key);

    public Long get(Bytes key);

    public Long remove(Bytes key);

    public boolean isSortedAscending();

}
