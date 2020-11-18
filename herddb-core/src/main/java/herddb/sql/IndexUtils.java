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

package herddb.sql;

import herddb.core.AbstractIndexManager;
import herddb.core.TableSpaceManager;
import herddb.index.IndexOperation;
import herddb.index.PrimaryIndexPrefixScan;
import herddb.index.PrimaryIndexRangeScan;
import herddb.index.PrimaryIndexSeek;
import herddb.index.SecondaryIndexPrefixScan;
import herddb.index.SecondaryIndexRangeScan;
import herddb.index.SecondaryIndexSeek;
import herddb.model.ColumnsList;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.sql.expressions.BindableTableScanColumnNameResolver;
import herddb.sql.expressions.CompiledMultiAndExpression;
import herddb.sql.expressions.CompiledSQLExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for searching for possible Index usages
 */
public class IndexUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IndexUtils.class
            .getName());

    static void discoverIndexOperations(final String tableSpace,
                                        CompiledSQLExpression where,
                                        Table table,
                                        SQLRecordPredicate predicate,
                                        Object debug,
                                        TableSpaceManager tableSpaceManager) throws StatementExecutionException {
        IndexOperation op = scanForIndexAccess(where, table, tableSpaceManager);
        predicate.setIndexOperation(op);
        CompiledSQLExpression filterPk = findFiltersOnPrimaryKey(table, where);

        if (filterPk != null) {
            filterPk = remapPositionalAccessToToPrimaryKeyAccessor(filterPk, table, debug);
        }
        predicate.setPrimaryKeyFilter(filterPk);
    }

    private static IndexOperation scanForIndexAccess(CompiledSQLExpression expressionWhere, Table table, TableSpaceManager tableSpaceManager) {
        SQLRecordKeyFunction keyFunction = findIndexAccess(expressionWhere, table.primaryKey, table,
                "=", table);
        IndexOperation result = null;
        if (keyFunction != null) {
            if (keyFunction.isFullPrimaryKey()) {
                result = new PrimaryIndexSeek(keyFunction);
            } else {
                result = new PrimaryIndexPrefixScan(keyFunction);
            }
        } else {
            SQLRecordKeyFunction rangeMin = findIndexAccess(expressionWhere, table.primaryKey,
                    table, ">=", table
            );
            if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                rangeMin = null;
            }
            if (rangeMin == null) {
                rangeMin = findIndexAccess(expressionWhere, table.primaryKey, table,
                        ">", table);
                if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                    rangeMin = null;
                }
            }

            SQLRecordKeyFunction rangeMax = findIndexAccess(expressionWhere, table.primaryKey,
                    table, "<=", table);
            if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                rangeMax = null;
            }
            if (rangeMax == null) {
                rangeMax = findIndexAccess(expressionWhere, table.primaryKey, table, "<", table);
                if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                    rangeMax = null;
                }
            }
            if (rangeMin != null || rangeMax != null) {
                result = new PrimaryIndexRangeScan(table.primaryKey, rangeMin, rangeMax);
            }
        }

        if (result == null && tableSpaceManager != null) {
            Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(table.name);
            if (indexes != null) {
                // TODO: use some kind of statistics, maybe using an index is more expensive than a full table scan
                for (AbstractIndexManager index : indexes.values()) {
                    if (!index.isAvailable()) {
                        continue;
                    }
                    IndexOperation secondaryIndexOperation = findSecondaryIndexOperation(index, expressionWhere, table);
                    if (secondaryIndexOperation != null) {
                        result = secondaryIndexOperation;
                        break;
                    }
                }
            }
        }
        return result;
    }

    private static IndexOperation findSecondaryIndexOperation(
            AbstractIndexManager index,
            CompiledSQLExpression where, Table table
    ) throws StatementExecutionException {
        IndexOperation secondaryIndexOperation = null;
        String[] columnsToMatch = index.getColumnNames();
        SQLRecordKeyFunction indexSeekFunction = findIndexAccess(where, columnsToMatch,
                index.getIndex(), "=", table);
        if (indexSeekFunction != null) {
            if (indexSeekFunction.isFullPrimaryKey()) {
                secondaryIndexOperation = new SecondaryIndexSeek(index.getIndexName(), columnsToMatch, indexSeekFunction);
            } else {
                secondaryIndexOperation = new SecondaryIndexPrefixScan(index.getIndexName(), columnsToMatch, indexSeekFunction);
            }
        } else {
            SQLRecordKeyFunction rangeMin = findIndexAccess(where, columnsToMatch,
                    index.getIndex(), ">=", table);
            if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                rangeMin = null;

            }
            if (rangeMin == null) {
                rangeMin = findIndexAccess(where, columnsToMatch,
                        index.getIndex(), ">", table);
                if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                    rangeMin = null;
                }
            }

            SQLRecordKeyFunction rangeMax = findIndexAccess(where, columnsToMatch,
                    index.getIndex(), "<=", table);
            if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                rangeMax = null;
            }
            if (rangeMax == null) {
                rangeMax = findIndexAccess(where, columnsToMatch,
                        index.getIndex(), "<", table);
                if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                    rangeMax = null;
                }
            }
            if (rangeMin != null || rangeMax != null) {
                secondaryIndexOperation = new SecondaryIndexRangeScan(index.getIndexName(), columnsToMatch, rangeMin, rangeMax);
            }

        }
        return secondaryIndexOperation;
    }

    /**
     * the function {@link Predicate#matchesRawPrimaryKey(herddb.utils.Bytes, herddb.model.StatementEvaluationContext)
     * }
     * works on a projection of the table wich contains only the pk fields of the table for instance if the predicate wants to access first element of the pk, and this field is the 3rd in the column
     * list then you will find {@link AccessCurrentRowExpression} with index=2. To this expression you have to apply the projection and map 2 (3rd element of the table) to 0 (1st element of the pk)
     *
     * @param filterPk
     * @param table
     */
    private static CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(CompiledSQLExpression filterPk, Table table, Object debug) {
        try {
            int[] projectionToKey = table.getPrimaryKeyProjection();
            return filterPk.remapPositionalAccessToToPrimaryKeyAccessor(projectionToKey);
        } catch (IllegalStateException notImplemented) {
            if (debug instanceof RelNode) {
                LOG.info("Not implemented best access for PK on "
                        + RelOptUtil.dumpPlan("", (RelNode) debug, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES), notImplemented);
            } else {
                LOG.info("Not implemented best access for PK on {}", debug);
            }
            return null;
        }
    }

    public static SQLRecordKeyFunction findIndexAccess(
            CompiledSQLExpression where,
            String[] columnsToMatch, ColumnsList table,
            String operator, BindableTableScanColumnNameResolver res
    ) throws StatementExecutionException {
        List<CompiledSQLExpression> expressions = new ArrayList<>();
        List<String> columns = new ArrayList<>();

        for (String pk : columnsToMatch) {
            List<CompiledSQLExpression> conditions = where.scanForConstraintedValueOnColumnWithOperator(pk, operator, res);
            if (conditions.isEmpty()) {
                break;
            }
            columns.add(pk);
            expressions.add(conditions.get(0));
        }
        if (expressions.isEmpty()) {
            // no match at all, there is no direct constraint on PK
            return null;
        }
        return new SQLRecordKeyFunction(columns, expressions, table);
    }

    private static CompiledSQLExpression findFiltersOnPrimaryKey(Table table, CompiledSQLExpression where) throws StatementExecutionException {
        List<CompiledSQLExpression> expressions = new ArrayList<>();

        for (String pk : table.primaryKey) {
            List<CompiledSQLExpression> conditions =
                    where.scanForConstraintsOnColumn(pk, table);
            if (conditions.isEmpty()) {
                break;
            }
            expressions.addAll(conditions);
        }
        if (expressions.isEmpty()) {
            // no match at all, there is no direct constraint on PK
            return null;
        } else if (expressions.size() == 1) {
            return expressions.get(0);
        } else {
            return new CompiledMultiAndExpression(expressions.toArray(new CompiledSQLExpression[expressions.size()]));
        }
    }

}
