/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0
direction
 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.model.planner;

import herddb.core.MaterializedRecordSet;
import herddb.core.SimpleDataScanner;
import herddb.core.TableSpaceManager;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.model.TupleComparator;
import herddb.sql.SQLRecordPredicate;
import herddb.utils.DataAccessor;
import herddb.utils.Wrapper;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic Sort
 *
 * @author eolivelli
 */
public class SortOp implements PlannerOp, TupleComparator {

    private final PlannerOp input;
    private final boolean[] directions;
    private final int[] fields;

    public SortOp(PlannerOp input, boolean[] directions, int[] fields) {
        this.input = input.optimize();
        this.directions = directions;
        this.fields = fields;
    }

    @Override
    public String getTablespace() {
        return input.getTablespace();
    }

    @Override
    public StatementExecutionResult execute(TableSpaceManager tableSpaceManager, TransactionContext transactionContext, StatementEvaluationContext context) throws StatementExecutionException {
        // TODO merge projection + scan + sort + limit
        StatementExecutionResult input = this.input.execute(tableSpaceManager, transactionContext, context);
        ScanResult downstreamScanResult = (ScanResult) input;
        final DataScanner inputScanner = downstreamScanResult.dataScanner;

        try (DataScanner dataScanner = inputScanner;) {
            MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                    .createRecordSet(inputScanner.getFieldNames(),
                            inputScanner.getSchema());
            while (dataScanner.hasNext()) {
                DataAccessor row = dataScanner.next();
                recordSet.add(row);
            }
            recordSet.writeFinished();
            recordSet.sort(this);
            SimpleDataScanner result = new SimpleDataScanner(downstreamScanResult.transactionId, recordSet);
            return new ScanResult(downstreamScanResult.transactionId, result);
        } catch (DataScannerException ex) {
            throw new StatementExecutionException(ex);
        }
    }

    @Override
    public boolean isOnlyPrimaryKeyAndAscending() {
        return false;
    }

    @Override
    public int compare(DataAccessor o1, DataAccessor o2) {
        for (int i = 0; i < fields.length; i++) {
            int index = fields[i];
            Object value1 = o1.get(index);
            Object value2 = o2.get(index);
            int result = SQLRecordPredicate.compare(value1, value2);
            if (result != 0) {
                if (directions[i]) {
                    return result;
                } else {
                    return -result;
                }
            }
        }
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        T unwrapped = input.unwrap(clazz);
        if (unwrapped != null) {
            return unwrapped;
        }
        return Wrapper.unwrap(this, clazz);
    }
}
