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

import herddb.core.TableSpaceManager;
import herddb.model.DuplicatePrimaryKeyException;
import herddb.model.GetResult;
import herddb.model.Record;
import herddb.model.RecordFunction;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import herddb.utils.VisibleByteArrayOutputStream;
import java.util.function.Function;

/**
 * Implementation of TmpMap
 *
 * @author eolivelli
 */
class TmpMapImpl<K, V> implements TmpMap<K, V> {

    private final String tmpTableName;
    private final Function<K, byte[]> keySerializer;
    private final ValueSerializer valuesSerializer;
    private final boolean threadsafe;
    private final TableSpaceManager tableSpaceManager;
    private final InsertStatement insert;
    private final UpdateStatement update;

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

    public TmpMapImpl(String tmpTableName, int expectedValueSize, boolean concurrent,
            Function<K, byte[]> keySerializer, ValueSerializer valuesSerializer,
            final TableSpaceManager tableSpaceManager) {
        this.tableSpaceManager = tableSpaceManager;
        this.threadsafe = concurrent;
        this.tmpTableName = tmpTableName;
        this.keySerializer = keySerializer;
        this.valuesSerializer = valuesSerializer;

        RecordFunction keyFunction = new RecordFunction() {
            @Override
            public byte[] computeNewValue(Record previous, StatementEvaluationContext context, TableContext tableContext)
                    throws StatementExecutionException {
                K key = ((PutStatementEvaluationContext<K, V>) context).getKey();
                return keySerializer.apply(key);
            }
        };
        RecordFunction valuesFunction = new RecordFunction() {
            @Override
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
    }

    @Override
    public void close() {
        DropTableStatement drop = new DropTableStatement(TableSpace.DEFAULT, tmpTableName, true);
        tableSpaceManager.executeStatement(drop, new StatementEvaluationContext(),
                herddb.model.TransactionContext.NO_TRANSACTION);
    }

    @Override
    public void put(K key, V value) throws Exception {
        StatementEvaluationContext context = new PutStatementEvaluationContext(key, value);
        if (threadsafe) {
            long tx = ((TransactionResult) tableSpaceManager.executeStatement(BEGIN_TRANSACTION_STATEMENT,
                    context, TransactionContext.NO_TRANSACTION)).transactionId;
            TransactionContext transactionContext = new TransactionContext(tx);
            try {
                tableSpaceManager.executeStatement(insert, context, transactionContext);
            } catch (DuplicatePrimaryKeyException alreadyExists) {
                tableSpaceManager.executeStatement(update, context, transactionContext);
            } finally {
                tableSpaceManager.executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, tx),
                        context, TransactionContext.NO_TRANSACTION);
            }
        } else {
            // no concurrent access, no need to create a transaction
            try {
                tableSpaceManager.executeStatement(insert, context, TransactionContext.NO_TRANSACTION);
            } catch (DuplicatePrimaryKeyException alreadyExists) {
                tableSpaceManager.executeStatement(update, context, TransactionContext.NO_TRANSACTION);
            }
        }
    }

    @Override
    public V get(K key) throws Exception {
        byte[] serializedKey = keySerializer.apply(key);
        return (V) executeGet(serializedKey, tmpTableName);
    }

    @Override
    public boolean containsKey(K key) throws Exception {
        byte[] serializedKey = keySerializer.apply(key);
        return executeContainsKey(serializedKey, tmpTableName);
    }

    private Object executeGet(byte[] serializedKey, String tmpTableName) throws StatementExecutionException, Exception {
        GetStatement get = new GetStatement(TableSpace.DEFAULT, tmpTableName, Bytes.from_array(serializedKey), null, false);
        GetResult getResult = (GetResult) tableSpaceManager.executeStatement(get,
                new StatementEvaluationContext(),
                herddb.model.TransactionContext.NO_TRANSACTION);
        if (!getResult.found()) {
            return null;
        } else {
            return valuesSerializer
                    .deserialize(getResult.getRecord().value);
        }
    }

    private boolean executeContainsKey(byte[] serializedKey, String tmpTableName) throws StatementExecutionException,
            Exception {
        GetStatement get = new GetStatement(TableSpace.DEFAULT, tmpTableName, Bytes.from_array(serializedKey), null, false);
        GetResult getResult = (GetResult) tableSpaceManager.executeStatement(get,
                new StatementEvaluationContext(),
                herddb.model.TransactionContext.NO_TRANSACTION);
        return getResult.found();
    }

    private static final BeginTransactionStatement BEGIN_TRANSACTION_STATEMENT =
            new BeginTransactionStatement(TableSpace.DEFAULT);

}
