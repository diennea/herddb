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

package herddb.model.planner;

import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
import herddb.model.DMLStatement;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.Predicate;
import herddb.model.RecordFunction;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableAwareStatement;
import herddb.model.TransactionContext;
import herddb.model.commands.UpdateStatement;
import herddb.model.predicates.RawKeyEquals;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.Wrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

public class UpdateOp implements PlannerOp {

    private final String tableSpace;
    private final String tableName;
    private final PlannerOp input;
    private final boolean returnValues;
    private final RecordFunction recordFunction;

    public UpdateOp(
            String tableSpace, String tableName, PlannerOp input, boolean returnValues,
            RecordFunction recordFunction
    ) {
        this.tableSpace = tableSpace;
        this.tableName = tableName;
        this.input = input.optimize();
        this.returnValues = returnValues;
        this.recordFunction = recordFunction;
    }

    @Override
    public String getTablespace() {
        return tableSpace;
    }

    @Override
    public CompletableFuture<StatementExecutionResult> executeAsync(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext, StatementEvaluationContext context, boolean lockRequired, boolean forWrite
    ) {
        StatementExecutionResult input = this.input.execute(tableSpaceManager, transactionContext, context, true, true);
        ScanResult downstreamScanResult = (ScanResult) input;
        final Table table = tableSpaceManager.getTableManager(tableName).getTable();
        long transactionId = transactionContext.transactionId;

        List<DMLStatement> statements = new ArrayList<>();

        try (DataScanner inputScanner = downstreamScanResult.dataScanner) {
            while (inputScanner.hasNext()) {

                DataAccessor row = inputScanner.next();
                long transactionIdFromScanner = inputScanner.getTransactionId();
                if (transactionIdFromScanner > 0 && transactionIdFromScanner != transactionId) {
                    transactionId = transactionIdFromScanner;
                    transactionContext = new TransactionContext(transactionId);
                }
                Bytes key = RecordSerializer.serializeIndexKey(row, table, table.getPrimaryKey());
                Predicate pred = new RawKeyEquals(key);
                DMLStatement updateStatement = new UpdateStatement(tableSpace, tableName,
                        null, this.recordFunction, pred)
                        .setReturnValues(returnValues);

                statements.add(updateStatement);

            }
            if (statements.isEmpty()) {
                return CompletableFuture.completedFuture(new DMLStatementExecutionResult(transactionId, 0, null, null));
            }
            if (statements.size() == 1) {
                return tableSpaceManager.executeStatementAsync(statements.get(0), context, transactionContext);
            }

            CompletableFuture<StatementExecutionResult> finalResult = new CompletableFuture<>();

            AtomicInteger updateCounts = new AtomicInteger();
            AtomicReference<Bytes> lastKey = new AtomicReference<>();
            AtomicReference<Bytes> lastNewValue = new AtomicReference<>();

            class ComputeNext implements BiConsumer<StatementExecutionResult, Throwable> {

                int current;

                public ComputeNext(int current) {
                    this.current = current;
                }

                @Override
                public void accept(StatementExecutionResult res, Throwable error) {
                    if (error != null) {
                        finalResult.completeExceptionally(error);
                        return;
                    }
                    DMLStatementExecutionResult dml = (DMLStatementExecutionResult) res;
                    updateCounts.addAndGet(dml.getUpdateCount());
                    if (returnValues) {
                        lastKey.set(dml.getKey());
                        lastNewValue.set(dml.getNewvalue());
                    }
                    long newTransactionId = res.transactionId;
                    if (current == statements.size()) {
                        DMLStatementExecutionResult finalDMLResult =
                                new DMLStatementExecutionResult(newTransactionId, updateCounts.get(),
                                        lastKey.get(), lastNewValue.get());
                        finalResult.complete(finalDMLResult);
                        return;
                    }

                    DMLStatement nextStatement = statements.get(current);
                    TransactionContext transactionContext = new TransactionContext(newTransactionId);
                    CompletableFuture<StatementExecutionResult> nextPromise =
                            tableSpaceManager.executeStatementAsync(nextStatement, context, transactionContext);
                    nextPromise.whenComplete(new ComputeNext(current + 1));
                }
            }

            DMLStatement firstStatement = statements.get(0);
            tableSpaceManager.executeStatementAsync(firstStatement, context, transactionContext)
                    .whenComplete(new ComputeNext(1));

            return finalResult;
        } catch (DataScannerException err) {
            throw new StatementExecutionException(err);
        }

    }

    private static final Logger LOG = Logger.getLogger(UpdateOp.class.getName());

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(TableAwareStatement.class)) {
            return (T) new TableAwareStatement(tableName, tableSpace) {
            };
        }
        return Wrapper.unwrap(this, clazz);
    }

    @Override
    public String toString() {
        return String.format("UpdateOp=[ input=%s recordFunction= %s", input, recordFunction.toString());
    }
}
