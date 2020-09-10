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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.MaterializedRecordSet;
import herddb.core.SimpleDataScanner;
import herddb.core.TableSpaceManager;
import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.ScanResult;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.TupleComparator;
import herddb.model.commands.ScanStatement;
import herddb.model.planner.ProjectOp.IdentityProjection;
import herddb.model.planner.ProjectOp.ZeroCopyProjection;
import herddb.utils.DataAccessor;
import herddb.utils.SQLRecordPredicateFunctions;
import herddb.utils.Wrapper;
import java.util.Arrays;

/**
 * Generic Sort
 *
 * @author eolivelli
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
public class SortOp implements PlannerOp, TupleComparator {

    private final PlannerOp input;
    private final boolean[] directions;
    private final boolean[] nullLastDirections;
    private final int[] fields;
    private boolean onlyPrimaryKeyAndAscending;

    public SortOp(PlannerOp input, boolean[] directions, int[] fields, boolean[] nullLastDirections) {
        this.input = input.optimize();
        this.directions = directions;
        this.fields = fields;
        this.nullLastDirections = nullLastDirections;
    }

    @Override
    public String getTablespace() {
        return input.getTablespace();
    }

    @Override
    public StatementExecutionResult execute(
            TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext, StatementEvaluationContext context, boolean lockRequired, boolean forWrite
    ) throws StatementExecutionException {
        // TODO merge projection + scan + sort + limit
        StatementExecutionResult input = this.input.execute(tableSpaceManager, transactionContext, context, lockRequired, forWrite);
        ScanResult downstreamScanResult = (ScanResult) input;
        final DataScanner inputScanner = downstreamScanResult.dataScanner;

        try (DataScanner dataScanner = inputScanner) {
            MaterializedRecordSet recordSet = tableSpaceManager.getDbmanager().getRecordSetFactory()
                    .createRecordSet(inputScanner.getFieldNames(),
                            inputScanner.getSchema());
            while (dataScanner.hasNext()) {
                DataAccessor row = dataScanner.next();
                recordSet.add(row);
            }
            recordSet.writeFinished();
            recordSet.sort(this);
            SimpleDataScanner result = new SimpleDataScanner(downstreamScanResult.dataScanner.getTransaction(), recordSet);
            return new ScanResult(downstreamScanResult.transactionId, result);
        } catch (DataScannerException ex) {
            throw new StatementExecutionException(ex);
        }
    }

    @Override
    public PlannerOp optimize() {
        if (input instanceof BindableTableScanOp) {
            BindableTableScanOp op = (BindableTableScanOp) input;
            // we can change the statement, this node will be lost and the tablescan too
            ScanStatement statement = op.getStatement();
            statement.setComparator(this);

            if (fields.length == 1 && directions[0]) {
                Table tableDef = statement.getTableDef();
                if (tableDef.getPrimaryKey().length == 1) {
                    if (statement.getProjection() != null && statement.getProjection() instanceof ZeroCopyProjection) {
                        ZeroCopyProjection zeroCopyProjection = (ZeroCopyProjection) statement.getProjection();
                        int index = zeroCopyProjection.mapPosition(fields[0]);
                        Column col = tableDef.resolveColumName(index);
                        if (col.name.equals(tableDef.getPrimaryKey()[0])) {
                            this.onlyPrimaryKeyAndAscending = true;
                        }
                    } else if (statement.getProjection() != null && statement.getProjection() instanceof IdentityProjection) {
                        Column col = tableDef.resolveColumName(fields[0]);
                        if (col.name.equals(tableDef.getPrimaryKey()[0])) {
                            this.onlyPrimaryKeyAndAscending = true;
                        }
                    }
                }
            }
            return new SortedBindableTableScanOp(statement);
        } else if (input instanceof TableScanOp) {
            TableScanOp op = (TableScanOp) input;
            // we can change the statement, this node will be lost and the tablescan too
            ScanStatement statement = op.getStatement();
            statement.setComparator(this);

            if (fields.length == 1 && directions[0]) {
                Table tableDef = statement.getTableDef();
                if (tableDef.getPrimaryKey().length == 1) {
                    if (statement.getProjection() != null && statement.getProjection() instanceof ZeroCopyProjection) {
                        ZeroCopyProjection zeroCopyProjection = (ZeroCopyProjection) statement.getProjection();
                        int index = zeroCopyProjection.mapPosition(fields[0]);
                        Column col = tableDef.resolveColumName(index);
                        if (col.name.equals(tableDef.getPrimaryKey()[0])) {
                            this.onlyPrimaryKeyAndAscending = true;
                        }
                    } else if (statement.getProjection() != null && statement.getProjection() instanceof IdentityProjection) {
                        Column col = tableDef.resolveColumName(fields[0]);
                        if (col.name.equals(tableDef.getPrimaryKey()[0])) {
                            this.onlyPrimaryKeyAndAscending = true;
                        }
                    }
                }
            }
            return new SortedTableScanOp(statement);
        }
        return this;
    }

    @Override
    public boolean isOnlyPrimaryKeyAndAscending() {
        return onlyPrimaryKeyAndAscending;
    }

    @Override
    @SuppressFBWarnings("RV_NEGATING_RESULT_OF_COMPARETO")
    public int compare(DataAccessor o1, DataAccessor o2) {
        for (int i = 0; i < fields.length; i++) {
            int index = fields[i];
            Object value1 = o1.get(index);
            Object value2 = o2.get(index);
            // this version of compare sorts NULL BEFORE all other values
            boolean nullLastDirection = nullLastDirections[i];
            if (nullLastDirection) { // NULL AST
                // NULL LAST is preferred for us, as it is the default
                int result = SQLRecordPredicateFunctions.compare(value1, value2);
                if (result != 0) {
                    if (directions[i]) { // ASC/DESC
                        return result;
                    } else {
                        return -result;
                    }
                }
                return 0;
            } else { // NULL FIRST
                SQLRecordPredicateFunctions.CompareResult resWithNull = SQLRecordPredicateFunctions.compareConsiderNull(value1, value2);
                if (directions[i]) { // ASC/DEC
                    switch (resWithNull) {
                        case EQUALS:
                            return 0;
                        case NULL: // ASC NULL FIRST
                            return -1;
                        case GREATER:
                            return 1;
                        case MINOR:
                            return -1;
                        default:
                            throw new IllegalStateException(resWithNull + "");
                    }
                } else {
                    switch (resWithNull) {
                        case EQUALS:
                            return 0;
                        case NULL: // DESC NULL FIRST
                            return 1;
                        case GREATER:
                            return -1;
                        case MINOR:
                            return 1;
                        default:
                            throw new IllegalStateException(resWithNull + "");
                    }
                }
            }
        }
        // no columns ?
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

    @Override
    public String toString() {
        return "SortOp{fields=" + Arrays.toString(fields) + ", dirs=" + Arrays.toString(directions) + ",nullDirs=" + Arrays.toString(nullLastDirections) + ", onlyPrimaryKeyAndAscending=" + onlyPrimaryKeyAndAscending + '}';
    }

}
