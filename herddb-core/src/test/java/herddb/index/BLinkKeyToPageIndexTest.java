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

import herddb.core.AbstractIndexManager;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.index.blink.BLinkKeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

/**
 * Base test suite for {@link  BLinkKeyToPageIndex}
 *
 * @author diego.salvi
 */
public class BLinkKeyToPageIndexTest extends KeyToPageIndexTest {

    @Override
    KeyToPageIndex createIndex() {

        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        BLinkKeyToPageIndex idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);

        return new ResourceCloseKeyToPageIndex(idx, ds);

    }

    private final class ResourceCloseKeyToPageIndex implements KeyToPageIndex {

        KeyToPageIndex delegate;
        AutoCloseable[] closeables;

        public ResourceCloseKeyToPageIndex(KeyToPageIndex index, AutoCloseable... closeables) {
            this.delegate = index;
            this.closeables = closeables;
        }

        @Override
        public void close() {
            delegate.close();

            IllegalStateException exception = null;
            for (AutoCloseable closeable : closeables) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    if (exception == null) {
                        exception = new IllegalStateException("Failed to properly close resources", e);
                    } else {
                        exception.addSuppressed(e);
                    }
                }
            }

            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public long getUsedMemory() {
            return delegate.getUsedMemory();
        }

        @Override
        public boolean requireLoadAtStartup() {
            return delegate.requireLoadAtStartup();
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void start(LogSequenceNumber sequenceNumber, boolean created) throws DataStorageManagerException {
            delegate.start(sequenceNumber, created);
        }

        @Override
        public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin)
                throws DataStorageManagerException {
            return delegate.checkpoint(sequenceNumber, pin);
        }

        @Override
        public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
            delegate.unpinCheckpoint(sequenceNumber);
        }

        @Override
        public void truncate() {
            delegate.truncate();
        }

        @Override
        public void dropData() {
            delegate.dropData();
        }

        @Override
        public void init() throws DataStorageManagerException {
            delegate.init();
        }

        @Override
        public Stream<Entry<Bytes, Long>> scanner(
                IndexOperation operation, StatementEvaluationContext context,
                TableContext tableContext, AbstractIndexManager index
        )
                throws DataStorageManagerException, StatementExecutionException {
            return delegate.scanner(operation, context, tableContext, index);
        }

        @Override
        public void put(Bytes key, Long currentPage) {
            delegate.put(key, currentPage);
        }

        @Override
        public boolean put(Bytes key, Long newPage, Long expectedPage) {
            return delegate.put(key, newPage, expectedPage);
        }

        @Override
        public boolean containsKey(Bytes key) {
            return delegate.containsKey(key);
        }

        @Override
        public Long get(Bytes key) {
            return delegate.get(key);
        }

        @Override
        public Long remove(Bytes key) {
            return delegate.remove(key);
        }

        @Override
        public boolean isSortedAscending(int[] pkTypes) {
            return delegate.isSortedAscending(pkTypes);
        }

    }

}
