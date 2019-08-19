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
package herddb.collections;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.HerdDBInternalException;
import herddb.core.TableSpaceManager;
import herddb.core.stats.TableManagerStats;
import herddb.codec.DataAccessorForFullRecord;
import herddb.index.PrimaryIndexSeek;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Predicate;
import herddb.model.Projection;
import herddb.model.Record;
import herddb.model.RecordFunction;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import herddb.utils.VisibleByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

/**
 * Implementation of TmpMap
 *
 * @author eolivelli
 */
class TmpMapImpl<K, V> implements TmpMap<K, V> {

    private final String tmpTableName;
    private final Table table;
    private final Function<K, byte[]> keySerializer;
    private final ValueSerializer valuesSerializer;
    private final TableSpaceManager tableSpaceManager;
    private final TableManagerStats stats;

    private final InsertStatement insert;
    private final DeleteStatement delete;
    private final UpdateStatement update;
    private final ScanStatement scan;
    private final ScanStatement scanKeys;

    private static final class PutStatementEvaluationContext<K, V> extends StatementEvaluationContext {

        private final K key;
        private final V value;

        public PutStatementEvaluationContext(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

    }

    public TmpMapImpl(Table table, int expectedValueSize,
                      Function<K, byte[]> keySerializer, ValueSerializer valuesSerializer,
                      final TableSpaceManager tableSpaceManager) {
        this.table = table;
        this.tableSpaceManager = tableSpaceManager;
        this.tmpTableName = table.name;
        this.keySerializer = keySerializer;
        this.valuesSerializer = valuesSerializer;

        RecordFunction keyFunction = new RecordFunction() {
            @Override
            @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
            public byte[] computeNewValue(Record previous, StatementEvaluationContext context, TableContext tableContext)
                    throws StatementExecutionException {
                K key = ((PutStatementEvaluationContext<K, V>) context).getKey();
                return keySerializer.apply(key);
            }
        };
        RecordFunction valuesFunction = new RecordFunction() {
            @Override
            @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
            public byte[] computeNewValue(Record previous, StatementEvaluationContext context, TableContext tableContext)
                    throws StatementExecutionException {
                try {
                    V value = ((PutStatementEvaluationContext<K, V>) context).getValue();
                    VisibleByteArrayOutputStream buffer = new VisibleByteArrayOutputStream(expectedValueSize);
                    valuesSerializer.serialize(value, buffer);
                    return buffer.toByteArray();
                } catch (Exception ex) {
                    throw new StatementExecutionException(ex);
                }
            }
        };
        insert = new InsertStatement(TableSpace.DEFAULT, tmpTableName, keyFunction, valuesFunction);
        update = new UpdateStatement(TableSpace.DEFAULT, tmpTableName, keyFunction, valuesFunction, null);
        delete = new DeleteStatement(TableSpace.DEFAULT, tmpTableName, null, new PredicateDeleteImpl(keyFunction));
        scan = new ScanStatement(TableSpace.DEFAULT, table, null);
        scanKeys = new ScanStatement(TableSpace.DEFAULT, table.name, Projection.PRIMARY_KEY(table), null, null, null);
        stats = tableSpaceManager
                .getTableManager(tmpTableName)
                .getStats();
    }

    @Override
    public void close() {
        DropTableStatement drop = new DropTableStatement(TableSpace.DEFAULT, tmpTableName, true);
        tableSpaceManager.executeStatement(drop, new StatementEvaluationContext(),
                herddb.model.TransactionContext.NO_TRANSACTION);
    }

    @Override
    public void put(K key, V value) throws CollectionsException {
        StatementEvaluationContext context = new PutStatementEvaluationContext(key, value);
        // no concurrent access, no need to be super conservative, keep row-level locks..
        try {
            try {
                // our best guess it that the key is not already mapped
                tableSpaceManager.executeStatement(insert, context, TransactionContext.NO_TRANSACTION);
            } catch (DuplicatePrimaryKeyException alreadyExists) {
                tableSpaceManager.executeStatement(update, context, TransactionContext.NO_TRANSACTION);
            }
        } catch (HerdDBInternalException err) {
            throw new CollectionsException(err);
        }
    }

    @Override
    public void remove(K key) throws CollectionsException {
        StatementEvaluationContext context = new PutStatementEvaluationContext(key, null);
        try {
            tableSpaceManager.executeStatement(delete, context, TransactionContext.NO_TRANSACTION);
        } catch (HerdDBInternalException err) {
            throw new CollectionsException(err);
        }

    }

    @Override
    public boolean isSwapped() {
        return stats.getLoadedPagesCount() > 0
                || stats.getUnloadedPagesCount() > 0;

    }

    @Override
    public long size() {
        return stats.getTablesize();
    }

    @Override
    public long estimateCurrentMemoryUsage() {
        return stats.getKeysUsedMemory() + stats.getBuffersUsedMemory() + stats.getDirtyUsedMemory();
    }

    @Override
    public V get(K key) throws CollectionsException {
        byte[] serializedKey = keySerializer.apply(key);
        return (V) executeGet(serializedKey, tmpTableName);
    }

    @Override
    public boolean containsKey(K key) throws CollectionsException {
        byte[] serializedKey = keySerializer.apply(key);
        return executeContainsKey(serializedKey, tmpTableName);
    }

    @Override
    public void forEach(BiSink<K, V> sink) throws CollectionsException, SinkException {

        try (DataScanner dataScanner =
                tableSpaceManager.scan(scan, new StatementEvaluationContext(), TransactionContext.NO_TRANSACTION,
                        false,
                        false)) {
            while (dataScanner.hasNext()) {
                DataAccessorForFullRecord next = (DataAccessorForFullRecord) dataScanner.next();
                Object key = next.get(0);
                if (key instanceof RawString) {
                    key = ((RawString) key).toString();
                }
                Object value = valuesSerializer
                        .deserialize(next.getRecord().value);
                try {
                    if (!sink.accept((K) key, (V) value)) {
                        return;
                    }
                } catch (Exception err) {
                    throw new SinkException(err);
                }

            }
        } catch (SinkException err) {
            throw err;
        } catch (Exception err) {
            throw new CollectionsException(err);
        }

    }

    @Override
    public void forEachKey(Sink<K> sink) throws CollectionsException, SinkException {
        try (DataScanner dataScanner =
                tableSpaceManager.scan(scan, new StatementEvaluationContext(), TransactionContext.NO_TRANSACTION,
                        false,
                        false)) {
            while (dataScanner.hasNext()) {
                DataAccessor next = dataScanner.next();
                Object key = next.get(0);
                if (key instanceof RawString) {
                    key = ((RawString) key).toString();
                }
                try {
                    if (!sink.accept((K) key)) {
                        return;
                    }
                } catch (Exception err) {
                    throw new SinkException(err);
                }

            }
        } catch (SinkException err) {
            throw err;
        } catch (HerdDBInternalException | DataScannerException err) {
            throw new CollectionsException(err);
        }
    }

    private Object executeGet(byte[] serializedKey, String tmpTableName) throws CollectionsException {
        try {
            GetStatement get = new GetStatement(TableSpace.DEFAULT, tmpTableName, Bytes.from_array(serializedKey), null,
                    false);
            GetResult getResult = (GetResult) tableSpaceManager.executeStatement(get,
                    new StatementEvaluationContext(),
                    herddb.model.TransactionContext.NO_TRANSACTION);
            if (!getResult.found()) {
                return null;
            } else {
                return valuesSerializer
                        .deserialize(getResult.getRecord().value);
            }
        } catch (Exception err) {
            throw new CollectionsException(err);
        }
    }

    private boolean executeContainsKey(byte[] serializedKey, String tmpTableName) throws CollectionsException {
        try {
            GetStatement get = new GetStatement(TableSpace.DEFAULT, tmpTableName, Bytes.from_array(serializedKey), null,
                    false);
            GetResult getResult = (GetResult) tableSpaceManager.executeStatement(get,
                    new StatementEvaluationContext(),
                    herddb.model.TransactionContext.NO_TRANSACTION);
            return getResult.found();
        } catch (HerdDBInternalException err) {
            throw new CollectionsException(err);
        }
    }

    final class PredicateDeleteImpl extends Predicate {

        private final RecordFunction keyFunction;

        public PredicateDeleteImpl(RecordFunction keyFunction) {
            this.keyFunction = keyFunction;
            this.setIndexOperation(new PrimaryIndexSeek(keyFunction));
        }

        @Override
        public PrimaryKeyMatchOutcome matchesRawPrimaryKey(Bytes key, StatementEvaluationContext context) throws
                StatementExecutionException {
            if (key.equals(Bytes.from_array(keyFunction.computeNewValue(null, context, null)))) {
                return PrimaryKeyMatchOutcome.FULL_CONDITION_VERIFIED;
            } else {
                return PrimaryKeyMatchOutcome.FAILED;
            }
        }

        @Override
        public boolean evaluate(Record record, StatementEvaluationContext context) throws
                StatementExecutionException {
            // we are already covered 
            return true;
        }
    }

    @Override
    public String toString() {
        return "TmpMap{tableName=" + tmpTableName + '}';
    }

}
