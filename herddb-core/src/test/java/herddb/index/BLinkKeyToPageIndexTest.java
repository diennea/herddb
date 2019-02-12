package herddb.index;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

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

/**
 * Suite di test base per {@link  BLinkKeyToPageIndex}
 *
 * @author diego.salvi
 */
public class BLinkKeyToPageIndexTest extends KeyToPageIndexTest {

    @Override
    KeyToPageIndex createIndex() {

        MemoryManager mem = new MemoryManager(5 * (1L << 20), 10 * (128L << 10), (128L << 10));
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        BLinkKeyToPageIndex idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);

        return new ResourceCloseKeyToPageIndex(idx,ds);

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
            for(AutoCloseable closeable : closeables) {
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
        public void start(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
            delegate.start(sequenceNumber);
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
        public Stream<Entry<Bytes, Long>> scanner(IndexOperation operation, StatementEvaluationContext context,
                                                  TableContext tableContext, AbstractIndexManager index)
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
        public boolean isSortedAscending() {
            return delegate.isSortedAscending();
        }

    }

}
