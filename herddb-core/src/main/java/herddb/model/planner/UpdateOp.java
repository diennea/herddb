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
import herddb.model.ConstValueRecordFunction;
import herddb.model.DMLStatement;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.RecordFunction;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableAwareStatement;
import herddb.model.TransactionContext;
import herddb.model.commands.UpdateStatement;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.Wrapper;

public class UpdateOp implements PlannerOp {

    private final String tableSpace;
    private final String tableName;
    private final PlannerOp input;
    private final boolean returnValues;
    private final RecordFunction recordFunction;

    public UpdateOp(String tableSpace, String tableName, PlannerOp input, boolean returnValues,
            RecordFunction recordFunction) {
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
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext, StatementEvaluationContext context) {
        StatementExecutionResult input = this.input.execute(tableSpaceManager, transactionContext, context);
        ScanResult downstreamScanResult = (ScanResult) input;
        final Table table = tableSpaceManager.getTableManager(tableName).getTable();
        long transactionId = transactionContext.transactionId;
        System.out.println("starting txid " + transactionId);
        int updateCount = 0;
        Bytes key = null;
        Bytes newValue = null;
        try (DataScanner inputScanner = downstreamScanResult.dataScanner;) {
            while (inputScanner.hasNext()) {

                DataAccessor row = inputScanner.next();
                long transactionIdFromScanner = inputScanner.getTransactionId();
                System.out.println("transactionIdFromScanner:" + transactionIdFromScanner);
                if (transactionIdFromScanner > 0 && transactionIdFromScanner != transactionId) {
                    transactionId = transactionIdFromScanner;
                    transactionContext = new TransactionContext(transactionId);
                }
                key = RecordSerializer.serializePrimaryKey(row, table, table.getPrimaryKey());

                DMLStatement updateStatement = new UpdateStatement(tableSpace, tableName,
                        new ConstValueRecordFunction(key.data), this.recordFunction, null)
                        .setReturnValues(returnValues);

                DMLStatementExecutionResult _result
                        = (DMLStatementExecutionResult) tableSpaceManager.executeStatement(updateStatement, context, transactionContext);
                System.out.println("update ount " + _result.getUpdateCount() + " txid " + _result.transactionId);
                updateCount += _result.getUpdateCount();
                if (_result.transactionId > 0 && _result.transactionId != transactionId) {
                    transactionId = _result.transactionId;
                    transactionContext = new TransactionContext(transactionId);
                }
                key = _result.getKey();
                newValue = _result.getNewvalue();
            }
            return new DMLStatementExecutionResult(transactionId, updateCount, key, newValue);
        } catch (DataScannerException err) {
            throw new StatementExecutionException(err);
        }

    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(TableAwareStatement.class)) {
            return (T) new TableAwareStatement(tableName, tableSpace) {
            };
        }
        return Wrapper.unwrap(this, clazz);
    }
}
