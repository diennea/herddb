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

import com.google.common.collect.ImmutableList;
import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.index.IndexOperation;
import herddb.index.PrimaryIndexPrefixScan;
import herddb.index.PrimaryIndexRangeScan;
import herddb.index.PrimaryIndexSeek;
import herddb.index.SecondaryIndexPrefixScan;
import herddb.index.SecondaryIndexRangeScan;
import herddb.index.SecondaryIndexSeek;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.AutoIncrementPrimaryKeyRecordFunction;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.ColumnsList;
import herddb.model.DMLStatement;
import herddb.model.ExecutionPlan;
import herddb.model.Predicate;
import herddb.model.Projection;
import herddb.model.RecordFunction;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.SQLPlannedOperationStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.UpdateStatement;
import herddb.model.planner.AggregateOp;
import herddb.model.planner.BindableTableScanOp;
import herddb.model.planner.DeleteOp;
import herddb.model.planner.SimpleDeleteOp;
import herddb.model.planner.FilterOp;
import herddb.model.planner.FilteredTableScanOp;
import herddb.model.planner.InsertOp;
import herddb.model.planner.JoinOp;
import herddb.model.planner.LimitOp;
import herddb.model.planner.PlannerOp;
import herddb.model.planner.ProjectOp;
import herddb.model.planner.SemiJoinOp;
import herddb.model.planner.SimpleInsertOp;
import herddb.model.planner.SortOp;
import herddb.model.planner.TableScanOp;
import herddb.model.planner.UnionAllOp;
import herddb.model.planner.SimpleUpdateOp;
import herddb.model.planner.ThetaJoinOp;
import herddb.model.planner.UpdateOp;
import herddb.model.planner.ValuesOp;
import herddb.sql.expressions.AccessCurrentRowExpression;
import herddb.sql.expressions.BindableTableScanColumnNameResolver;
import herddb.sql.expressions.CompiledMultiAndExpression;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.sql.expressions.ConstantExpression;
import herddb.sql.expressions.JdbcParameterExpression;
import herddb.sql.expressions.SQLExpressionCompiler;
import herddb.sql.expressions.TypedJdbcParameterExpression;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.statement.Statement;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSemiJoin;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.adapter.enumerable.EnumerableTableModify;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.enumerable.EnumerableThetaJoin;
import org.apache.calcite.adapter.enumerable.EnumerableUnion;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.interpreter.Bindables.BindableTableScan;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * SQL Planner based upon Apache Calcite
 *
 * @author eolivelli
 */
public class CalcitePlanner implements AbstractSQLPlanner {

    private final DBManager manager;
    private final AbstractSQLPlanner fallback;

    public CalcitePlanner(DBManager manager, long maxPlanCacheSize) {
        this.manager = manager;
        this.cache = new PlansCache(maxPlanCacheSize);
        //used only for DDL
        this.fallback = new SQLPlanner(manager, maxPlanCacheSize);
    }

    private final PlansCache cache;
    private SchemaPlus rootSchema;

    @Override
    public long getCacheSize() {
        return cache.getCacheSize();
    }

    @Override
    public long getCacheHits() {
        return cache.getCacheHits();
    }

    @Override
    public long getCacheMisses() {
        return cache.getCacheMisses();
    }

    @Override
    public void clearCache() {
        rootSchema = null;
        cache.clear();
        fallback.clearCache();
    }

    @Override
    public ExecutionPlan plan(String defaultTableSpace, Statement stmt, boolean scan, boolean returnValues, int maxRows) {
        return fallback.plan(defaultTableSpace, stmt, scan, returnValues, maxRows);
    }

    @Override
    public TranslatedQuery translate(String defaultTableSpace, String query, List<Object> parameters, boolean scan, boolean allowCache, boolean returnValues, int maxRows) throws StatementExecutionException {
        query = SQLPlanner.rewriteExecuteSyntax(query);
        if (query.startsWith("CREATE")
            || query.startsWith("DROP")
            || query.startsWith("EXECUTE")
            || query.startsWith("ALTER")
            || query.startsWith("BEGIN")
            || query.startsWith("COMMIT")
            || query.startsWith("ROLLBACK")
            //                || (query.startsWith("UPDATE") && query.contains("?")) // this needs some fixes on Calcite
            || query.startsWith("TRUNCATE")) {
            return fallback.translate(defaultTableSpace, query, parameters, scan, allowCache, returnValues, maxRows);
        }
        if (parameters == null) {
            parameters = Collections.emptyList();
        }
        String cacheKey = "scan:" + scan
            + ",defaultTableSpace:" + defaultTableSpace
            + ",query:" + query
            + ",returnValues:" + returnValues
            + ",maxRows:" + maxRows;
        if (allowCache) {
            ExecutionPlan cached = cache.get(cacheKey);
            if (cached != null) {
                return new TranslatedQuery(cached, new SQLStatementEvaluationContext(query, parameters));
            }
        }
        if (!isCachable(query)) {
            allowCache = false;
        }
        try {
            SchemaPlus schema = getRootSchema();
            List<RelTraitDef> traitDefs = new ArrayList<>();
            traitDefs.add(ConventionTraitDef.INSTANCE);
            SchemaPlus subSchema = schema.getSubSchema(defaultTableSpace);
            if (subSchema == null) {
                TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(defaultTableSpace);
                throw new StatementExecutionException("internal error, Calcite subSchema for " + defaultTableSpace + " is null, tableSpaceManager is " + tableSpaceManager);
            }

            final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SQL_PARSER_CONFIG)
                .defaultSchema(subSchema)
                .traitDefs(traitDefs)
                // define the rules you want to apply

                .programs(Programs.ofRules(Programs.RULE_SET))
                .build();
            PlannerResult plan = runPlanner(config, query);
            SQLPlannedOperationStatement sqlPlannedOperationStatement = new SQLPlannedOperationStatement(
                convertRelNode(plan.topNode, plan.originalRowType, returnValues)
                    .optimize());
            if (!scan) {
                ScanStatement scanStatement = sqlPlannedOperationStatement.unwrap(ScanStatement.class);
                if (scanStatement != null) {
                    Table tableDef = scanStatement.getTableDef();
                    CompiledSQLExpression where = scanStatement.getPredicate().unwrap(CompiledSQLExpression.class);
                    SQLRecordKeyFunction keyFunction = findIndexAccess(where, tableDef.getPrimaryKey(),
                        tableDef, "=", tableDef);
                    if (keyFunction == null || !keyFunction.isFullPrimaryKey()) {
                        throw new StatementExecutionException("unsupported GET not on PK, bad where clause: " + query);
                    }
                    GetStatement get = new GetStatement(scanStatement.getTableSpace(),
                        scanStatement.getTable(), keyFunction, scanStatement.getPredicate(), true);
                    ExecutionPlan executionPlan = ExecutionPlan.simple(
                        get
                    );
                    if (allowCache) {
                        cache.put(cacheKey, executionPlan);
                    }
                    return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters));
                }
            }
            if (maxRows > 0) {
                PlannerOp op = new LimitOp(sqlPlannedOperationStatement.getRootOp(),
                    new ConstantExpression(maxRows), new ConstantExpression(0))
                    .optimize();
                sqlPlannedOperationStatement = new SQLPlannedOperationStatement(op);
            }
            ExecutionPlan executionPlan = ExecutionPlan.simple(
                sqlPlannedOperationStatement
            );
            if (allowCache) {
                cache.put(cacheKey, executionPlan);
            }
            return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters));
        } catch (CalciteContextException ex) {
            LOG.log(Level.INFO, "Error while parsing '" + ex.getOriginalStatement() + "'", ex);
            //TODO can this be done better ?
            throw new StatementExecutionException(ex.getMessage());
        } catch (RelConversionException | ValidationException | SqlParseException ex) {
            LOG.log(Level.INFO, "Error while parsing '" + query + "'", ex);
            //TODO can this be done better ?
            throw new StatementExecutionException(ex.getMessage().replace("org.apache.calcite.runtime.CalciteContextException: ", ""));
        } catch (MetadataStorageManagerException ex) {
            LOG.log(Level.INFO, "Error while parsing '" + query + "'", ex);
            throw new StatementExecutionException(ex);
        }
    }
    private static final SqlParser.Config SQL_PARSER_CONFIG
        = SqlParser.configBuilder(SqlParser.Config.DEFAULT)
            .setCaseSensitive(false)
            .setConformance(SqlConformanceEnum.MYSQL_5)
            .setQuoting(Quoting.BACK_TICK)
            .build();

    /**
     * the function {@link Predicate#matchesRawPrimaryKey(herddb.utils.Bytes, herddb.model.StatementEvaluationContext)
     * }
     * works on a projection of the table wich contains only the pk fields of the table for instance if the predicate
     * wants to access first element of the pk, and this field is the 3rd in the column list then you will find
     * {@link AccessCurrentRowExpression} with index=2. To this expression you have to apply the projection and map 2
     * (3rd element of the table) to 0 (1st element of the pk)
     *
     * @param filterPk
     * @param table
     */
    private CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(CompiledSQLExpression filterPk, Table table) {
        try {
            int[] projectionToKey = table.getPrimaryKeyProjection();
            return filterPk.remapPositionalAccessToToPrimaryKeyAccessor(projectionToKey);
        } catch (IllegalStateException notImplemented) {
            LOG.log(Level.INFO, "Not implemented best accessnfor PK ", notImplemented);
            return null;
        }
    }
    private static final Logger LOG = Logger.getLogger(CalcitePlanner.class.getName());

    private static class PlannerResult {

        private final RelNode topNode;
        private final RelDataType originalRowType;

        public PlannerResult(RelNode topNode, RelDataType originalRowType) {
            this.topNode = topNode;
            this.originalRowType = originalRowType;
        }

    }

    private PlannerResult runPlanner(FrameworkConfig config, String query) throws RelConversionException, SqlParseException, ValidationException {
        Planner planner = Frameworks.getPlanner(config);
        if (LOG.isLoggable(Level.FINER)) {
            LOG.log(Level.FINER, "Query: {0}", query);
        }
        SqlNode n = planner.parse(query);
        n = planner.validate(n);
        RelNode root = planner.rel(n).project();
        if (LOG.isLoggable(Level.FINER)) {
            LOG.log(Level.FINER, "Query: {0} {1}", new Object[]{query,
                RelOptUtil.dumpPlan("-- Logical Plan", root, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES)});
        }
        RelDataType originalRowType = root.getRowType();
        RelOptCluster cluster = root.getCluster();
        final RelOptPlanner optPlanner = cluster.getPlanner();
        RelTraitSet desiredTraits
            = cluster.traitSet().replace(EnumerableConvention.INSTANCE);
        final RelNode newRoot = optPlanner.changeTraits(root, desiredTraits);
        optPlanner.setRoot(newRoot);
        RelNode bestExp = optPlanner.findBestExp();
        if (LOG.isLoggable(Level.FINER)) {
            LOG.log(Level.FINER, "Query: {0} {1}", new Object[]{query,
                RelOptUtil.dumpPlan("-- Best  Plan", bestExp, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES)});
        }
//        System.out.println("Query: "+query+"\n"+
//                RelOptUtil.dumpPlan("-- Best  Plan", bestExp, SqlExplainFormat.TEXT,
//                SqlExplainLevel.ALL_ATTRIBUTES));
        return new PlannerResult(bestExp, originalRowType);
    }

    private SchemaPlus getRootSchema() throws MetadataStorageManagerException {
        if (rootSchema != null) {
            return rootSchema;
        }
        final SchemaPlus _rootSchema = Frameworks.createRootSchema(true);
        for (String tableSpace : manager.getLocalTableSpaces()) {
            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
            SchemaPlus schema = _rootSchema.add(tableSpace, new AbstractSchema());
            List<Table> tables = tableSpaceManager.getAllTablesForPlanner();
            for (Table table : tables) {
                AbstractTableManager tableManager = tableSpaceManager.getTableManager(table.name);
                TableImpl tableDef = new TableImpl(tableManager);
                schema.add(table.name, tableDef);
            }
        }
        rootSchema = _rootSchema;
        return _rootSchema;
    }

    private PlannerOp convertRelNode(RelNode plan,
        RelDataType rowType, boolean returnValues) throws StatementExecutionException {
        if (plan instanceof EnumerableTableModify) {
            EnumerableTableModify dml = (EnumerableTableModify) plan;
            switch (dml.getOperation()) {
                case INSERT:
                    return planInsert(dml, returnValues);
                case DELETE:
                    return planDelete(dml);
                case UPDATE:
                    return planUpdate(dml, returnValues);
                default:
                    throw new StatementExecutionException("unsupport DML operation " + dml.getOperation());
            }
        } else if (plan instanceof BindableTableScan) {
            BindableTableScan scan = (BindableTableScan) plan;
            return planBindableTableScan(scan, rowType);
        } else if (plan instanceof EnumerableTableScan) {
            EnumerableTableScan scan = (EnumerableTableScan) plan;
            return planEnumerableTableScan(scan, rowType);
        } else if (plan instanceof EnumerableProject) {
            EnumerableProject scan = (EnumerableProject) plan;
            return planProject(scan, rowType);
        } else if (plan instanceof EnumerableSemiJoin) {
            EnumerableSemiJoin scan = (EnumerableSemiJoin) plan;
            return planEnumerableSemiJoin(scan, rowType);
        } else if (plan instanceof EnumerableThetaJoin) {
            EnumerableThetaJoin scan = (EnumerableThetaJoin) plan;
            return planEnumerableThetaJoin(scan, rowType);
        } else if (plan instanceof EnumerableJoin) {
            EnumerableJoin scan = (EnumerableJoin) plan;
            return planEnumerableJoin(scan, rowType);
        } else if (plan instanceof EnumerableMergeJoin) {
            EnumerableMergeJoin scan = (EnumerableMergeJoin) plan;
            return planEnumerableMergeJoin(scan, rowType);
        } else if (plan instanceof EnumerableValues) {
            EnumerableValues scan = (EnumerableValues) plan;
            return planValues(scan);
        } else if (plan instanceof EnumerableSort) {
            EnumerableSort scan = (EnumerableSort) plan;
            return planSort(scan, rowType);
        } else if (plan instanceof EnumerableLimit) {
            EnumerableLimit scan = (EnumerableLimit) plan;
            return planLimit(scan, rowType);
        } else if (plan instanceof EnumerableInterpreter) {
            EnumerableInterpreter scan = (EnumerableInterpreter) plan;
            return planInterpreter(scan, rowType, returnValues);
        } else if (plan instanceof EnumerableFilter) {
            EnumerableFilter scan = (EnumerableFilter) plan;
            return planFilter(scan, rowType, returnValues);
        } else if (plan instanceof EnumerableUnion) {
            EnumerableUnion scan = (EnumerableUnion) plan;
            return planEnumerableUnion(scan, rowType, returnValues);
        } else if (plan instanceof EnumerableAggregate) {
            EnumerableAggregate scan = (EnumerableAggregate) plan;
            return planAggregate(scan, rowType, returnValues);
        }

        throw new StatementExecutionException("not implented " + plan.getRelTypeName());
    }

    private PlannerOp planInsert(EnumerableTableModify dml,
        boolean returnValues) {

        final String tableSpace = dml.getTable().getQualifiedName().get(0);
        final String tableName = dml.getTable().getQualifiedName().get(1);
        DMLStatement statement = null;
        if (dml.getInput() instanceof EnumerableProject) {
            // fastest path for insert into TABLE(s,b,c) values(?,?,?)
            EnumerableProject project = (EnumerableProject) dml.getInput();
            if (project.getInput() instanceof EnumerableValues) {
                EnumerableValues values = (EnumerableValues) project.getInput();
                if (values.getTuples().size() == 1) {
                    final TableImpl tableImpl
                        = (TableImpl) dml.getTable().unwrap(org.apache.calcite.schema.Table.class);
                    Table table = tableImpl.tableManager.getTable();
                    int index = 0;
                    List<RexNode> projects = project.getProjects();
                    List<CompiledSQLExpression> keyValueExpression = new ArrayList<>();
                    List<String> keyExpressionToColumn = new ArrayList<>();
                    List<CompiledSQLExpression> valuesExpressions = new ArrayList<>();
                    List<String> valuesColumns = new ArrayList<>();
                    boolean invalid = false;
                    for (Column column : table.getColumns()) {
                        CompiledSQLExpression exp
                            = SQLExpressionCompiler.compileExpression(projects.get(index));
                        if (exp instanceof ConstantExpression
                            || exp instanceof JdbcParameterExpression
                            || exp instanceof TypedJdbcParameterExpression) {
                            boolean isAlwaysNull = (exp instanceof ConstantExpression)
                                && ((ConstantExpression) exp).isNull();
                            if (!isAlwaysNull) {
                                if (table.isPrimaryKeyColumn(column.name)) {
                                    keyExpressionToColumn.add(column.name);
                                    keyValueExpression.add(exp);
                                }
                                valuesColumns.add(column.name);
                                valuesExpressions.add(exp);
                            }
                            index++;
                        } else {
                            invalid = true;
                            break;
                        }
                    }
                    if (!invalid) {
                        RecordFunction keyfunction;
                        if (keyValueExpression.isEmpty()
                            && table.auto_increment) {
                            keyfunction = new AutoIncrementPrimaryKeyRecordFunction();
                        } else {
                            if (keyValueExpression.size() != table.primaryKey.length) {
                                throw new StatementExecutionException("you must set a value for the primary key (expressions=" + keyValueExpression.size() + ")");
                            }
                            keyfunction = new SQLRecordKeyFunction(keyExpressionToColumn, keyValueExpression, table);
                        }
                        RecordFunction valuesfunction = new SQLRecordFunction(valuesColumns, table, valuesExpressions);
                        statement = new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction).setReturnValues(returnValues);
                    }
                }
            }
        }
        if (statement != null) {
            return new SimpleInsertOp(statement);
        }
        PlannerOp input = convertRelNode(dml.getInput(),
            null, false);

        try {
            return new InsertOp(tableSpace, tableName, input, returnValues);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private PlannerOp planDelete(EnumerableTableModify dml) {
        PlannerOp input = convertRelNode(dml.getInput(), null, false);

        final String tableSpace = dml.getTable().getQualifiedName().get(0);
        final String tableName = dml.getTable().getQualifiedName().get(1);
        final TableImpl tableImpl
            = (TableImpl) dml.getTable().unwrap(org.apache.calcite.schema.Table.class);
        Table table = tableImpl.tableManager.getTable();
        DeleteStatement delete = null;
        if (input instanceof TableScanOp) {
            delete = new DeleteStatement(tableSpace, tableName, null, null);
        } else if (input instanceof FilterOp) {
            FilterOp filter = (FilterOp) input;
            if (filter.getInput() instanceof TableScanOp) {
                SQLRecordPredicate pred = new SQLRecordPredicate(table, null, filter.getCondition());
                delete = new DeleteStatement(tableSpace, tableName, null, pred);
            }
        } else if (input instanceof BindableTableScanOp) {
            BindableTableScanOp filter = (BindableTableScanOp) input;
            Predicate pred = filter
                .getStatement()
                .getPredicate();
            delete = new DeleteStatement(tableSpace, tableName, null, pred);
        }
        if (delete != null) {

            return new SimpleDeleteOp(delete);
        } else {
            return new DeleteOp(tableSpace, tableName, input);
        }
    }

    private PlannerOp planUpdate(EnumerableTableModify dml, boolean returnValues) {
        PlannerOp input = convertRelNode(dml.getInput(), null, false);
        List<String> updateColumnList = dml.getUpdateColumnList();
        List<RexNode> sourceExpressionList = dml.getSourceExpressionList();
        final String tableSpace = dml.getTable().getQualifiedName().get(0);
        final String tableName = dml.getTable().getQualifiedName().get(1);
        final TableImpl tableImpl
            = (TableImpl) dml.getTable().unwrap(org.apache.calcite.schema.Table.class);
        Table table = tableImpl.tableManager.getTable();
        List<CompiledSQLExpression> expressions = new ArrayList<>(sourceExpressionList.size());

        for (RexNode node : sourceExpressionList) {
            CompiledSQLExpression exp = SQLExpressionCompiler.compileExpression(node);
            expressions.add(exp);
        }
        RecordFunction function = new SQLRecordFunction(updateColumnList, table, expressions);
        UpdateStatement update = null;
        if (input instanceof TableScanOp) {
            update = new UpdateStatement(tableSpace, tableName, null, function, null);
        } else if (input instanceof FilterOp) {
            FilterOp filter = (FilterOp) input;
            if (filter.getInput() instanceof TableScanOp) {
                SQLRecordPredicate pred = new SQLRecordPredicate(table, null, filter.getCondition());
                update = new UpdateStatement(tableSpace, tableName, null, function, pred);
            }
        } else if (input instanceof ProjectOp) {
            ProjectOp proj = (ProjectOp) input;
            if (proj.getInput() instanceof TableScanOp) {
                update = new UpdateStatement(tableSpace, tableName, null, function, null);
            } else if (proj.getInput() instanceof FilterOp) {
                FilterOp filter = (FilterOp) proj.getInput();
                if (filter.getInput() instanceof TableScanOp) {
                    SQLRecordPredicate pred = new SQLRecordPredicate(table, null, filter.getCondition());
                    update = new UpdateStatement(tableSpace, tableName, null, function, pred);
                }
            } else if (proj.getInput() instanceof FilteredTableScanOp) {
                FilteredTableScanOp filter = (FilteredTableScanOp) proj.getInput();
                Predicate pred = filter.getPredicate();
                update = new UpdateStatement(tableSpace, tableName, null, function, pred);
            } else if (proj.getInput() instanceof BindableTableScanOp) {
                BindableTableScanOp filter = (BindableTableScanOp) proj.getInput();
                ScanStatement scan = filter.getStatement();
                if (scan.getComparator() == null && scan.getLimits() == null
                    && scan.getTableDef() != null) {
                    Predicate pred = scan.getPredicate();
                    update = new UpdateStatement(tableSpace, tableName, null, function, pred);
                }
            }
        }
        if (update != null) {
            return new SimpleUpdateOp(update.setReturnValues(returnValues));
        } else {
            return new UpdateOp(tableSpace, tableName, input, returnValues, function);
        }

    }

    private PlannerOp planEnumerableTableScan(EnumerableTableScan scan, RelDataType rowType) {
        final String tableSpace = scan.getTable().getQualifiedName().get(0);
        final TableImpl tableImpl
            = (TableImpl) scan.getTable().unwrap(org.apache.calcite.schema.Table.class);
        Table table = tableImpl.tableManager.getTable();
        ScanStatement scanStatement = new ScanStatement(tableSpace, table, null);
        return new TableScanOp(scanStatement);
    }

    private PlannerOp planBindableTableScan(BindableTableScan scan, RelDataType rowType) {
        if (rowType == null) {
            rowType = scan.getRowType();
        }
        final String tableSpace = scan.getTable().getQualifiedName().get(0);
        final TableImpl tableImpl
            = (TableImpl) scan.getTable().unwrap(org.apache.calcite.schema.Table.class);
        Table table = tableImpl.tableManager.getTable();
        SQLRecordPredicate predicate = null;
        if (!scan.filters.isEmpty()) {
            CompiledSQLExpression where = null;
            if (scan.filters.size() == 1) {
                RexNode expr = scan.filters.get(0);
                where = SQLExpressionCompiler.compileExpression(expr);
            } else {
                CompiledSQLExpression[] operands = new CompiledSQLExpression[scan.filters.size()];
                int i = 0;
                for (RexNode expr : scan.filters) {
                    CompiledSQLExpression condition = SQLExpressionCompiler.compileExpression(expr);
                    operands[i++] = condition;
                }
                where = new CompiledMultiAndExpression(operands);
            }
            predicate = new SQLRecordPredicate(table, null, where);
            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);

            IndexOperation op = scanForIndexAccess(where, table, tableSpaceManager);
            predicate.setIndexOperation(op);
            CompiledSQLExpression filterPk = findFiltersOnPrimaryKey(table, where);

            if (filterPk != null) {
                filterPk = remapPositionalAccessToToPrimaryKeyAccessor(filterPk, table);
            }
            predicate.setPrimaryKeyFilter(filterPk);
        }
        List<RexNode> projections = new ArrayList<>(scan.projects.size());

        int i = 0;

        for (int fieldpos : scan.projects) {
            projections.add(new RexInputRef(fieldpos, rowType
                .getFieldList()
                .get(i++).getType()));
        }
        Projection projection = buildProjection(projections, rowType);
        ScanStatement scanStatement = new ScanStatement(tableSpace, table.name, projection, predicate, null, null);
        scanStatement.setTableDef(table);
        return new BindableTableScanOp(scanStatement);
    }

    private CompiledSQLExpression findFiltersOnPrimaryKey(Table table, CompiledSQLExpression where) throws StatementExecutionException {
        List<CompiledSQLExpression> expressions = new ArrayList<>();

        for (String pk : table.primaryKey) {
            List<CompiledSQLExpression> conditions
                = where.scanForConstraintsOnColumn(pk, table);
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

    private PlannerOp planProject(EnumerableProject op, RelDataType rowType) {
        PlannerOp input = convertRelNode(op.getInput(), null, false);

        final List<RexNode> projects = op.getProjects();
        final RelDataType _rowType = rowType == null ? op.getRowType() : rowType;
        Projection projection = buildProjection(projects, _rowType);
        return new ProjectOp(projection, input);
    }

    private PlannerOp planEnumerableSemiJoin(EnumerableSemiJoin op, RelDataType rowType) {
        // please note that EnumerableSemiJoin has a condition field which actually is not useful
        PlannerOp left = convertRelNode(op.getLeft(), null, false);
        PlannerOp right = convertRelNode(op.getRight(), null, false);
        int[] leftKeys = op.getLeftKeys().toIntArray();
        int[] rightKeys = op.getRightKeys().toIntArray();
        final RelDataType _rowType = rowType == null ? op.getRowType() : rowType;
        List<RelDataTypeField> fieldList = _rowType.getFieldList();
        Column[] columns = new Column[fieldList.size()];
        String[] fieldNames = new String[columns.length];
        int i = 0;
        for (RelDataTypeField field : fieldList) {
            Column col = Column.column(field.getName().toLowerCase(),
                convertToHerdType(field.getType()));
            fieldNames[i] = col.name;
            columns[i++] = col;
        }
        return new SemiJoinOp(fieldNames, columns,
            leftKeys, left, rightKeys, right);
    }

    private PlannerOp planEnumerableJoin(EnumerableJoin op, RelDataType rowType) {
        // please note that EnumerableJoin has a condition field which actually is not useful
        PlannerOp left = convertRelNode(op.getLeft(), null, false);
        PlannerOp right = convertRelNode(op.getRight(), null, false);
        int[] leftKeys = op.getLeftKeys().toIntArray();
        int[] rightKeys = op.getRightKeys().toIntArray();
        boolean generateNullsOnLeft = op.getJoinType().generatesNullsOnLeft();
        boolean generateNullsOnRight = op.getJoinType().generatesNullsOnRight();
        final RelDataType _rowType = rowType == null ? op.getRowType() : rowType;
        List<RelDataTypeField> fieldList = _rowType.getFieldList();
        Column[] columns = new Column[fieldList.size()];
        String[] fieldNames = new String[columns.length];
        int i = 0;
        for (RelDataTypeField field : fieldList) {
            Column col = Column.column(field.getName().toLowerCase(),
                convertToHerdType(field.getType()));
            fieldNames[i] = col.name;
            columns[i++] = col;
        }
        return new JoinOp(fieldNames, columns,
            leftKeys, left, rightKeys, right, generateNullsOnLeft, generateNullsOnRight, false);
    }

    private PlannerOp planEnumerableThetaJoin(EnumerableThetaJoin op, RelDataType rowType) {
        PlannerOp left = convertRelNode(op.getLeft(), null, false);
        PlannerOp right = convertRelNode(op.getRight(), null, false);
        CompiledSQLExpression condition = SQLExpressionCompiler.compileExpression(op.getCondition());
        boolean generateNullsOnLeft = op.getJoinType().generatesNullsOnLeft();
        boolean generateNullsOnRight = op.getJoinType().generatesNullsOnRight();
        final RelDataType _rowType = rowType == null ? op.getRowType() : rowType;
        List<RelDataTypeField> fieldList = _rowType.getFieldList();
        Column[] columns = new Column[fieldList.size()];
        String[] fieldNames = new String[columns.length];
        int i = 0;
        for (RelDataTypeField field : fieldList) {
            Column col = Column.column(field.getName().toLowerCase(),
                convertToHerdType(field.getType()));
            fieldNames[i] = col.name;
            columns[i++] = col;
        }
        return new ThetaJoinOp(fieldNames, columns,
            left, right, condition, generateNullsOnLeft, generateNullsOnRight, false);
    }

    private PlannerOp planEnumerableMergeJoin(EnumerableMergeJoin op, RelDataType rowType) {
        // please note that EnumerableMergeJoin has a condition field which actually is not useful
        PlannerOp left = convertRelNode(op.getLeft(), null, false);
        PlannerOp right = convertRelNode(op.getRight(), null, false);
        int[] leftKeys = op.getLeftKeys().toIntArray();
        int[] rightKeys = op.getRightKeys().toIntArray();
        boolean generateNullsOnLeft = op.getJoinType().generatesNullsOnLeft();
        boolean generateNullsOnRight = op.getJoinType().generatesNullsOnRight();
        final RelDataType _rowType = rowType == null ? op.getRowType() : rowType;
        List<RelDataTypeField> fieldList = _rowType.getFieldList();
        Column[] columns = new Column[fieldList.size()];
        String[] fieldNames = new String[columns.length];
        int i = 0;
        for (RelDataTypeField field : fieldList) {
            Column col = Column.column(field.getName().toLowerCase(),
                convertToHerdType(field.getType()));
            fieldNames[i] = col.name;
            columns[i++] = col;
        }
        return new JoinOp(fieldNames, columns,
            leftKeys, left, rightKeys, right, generateNullsOnLeft, generateNullsOnRight, true);
    }

    private Projection buildProjection(
        final List<RexNode> projects,
        final RelDataType rowType) {
        boolean allowZeroCopyProjection = true;
        List<CompiledSQLExpression> fields = new ArrayList<>(projects.size());
        Column[] columns = new Column[projects.size()];
        String[] fieldNames = new String[columns.length];
        int i = 0;
        int[] zeroCopyProjections = new int[fieldNames.length];
        for (RexNode node : projects) {
            CompiledSQLExpression exp = SQLExpressionCompiler.compileExpression(node);
            if (exp instanceof AccessCurrentRowExpression) {
                AccessCurrentRowExpression accessCurrentRowExpression = (AccessCurrentRowExpression) exp;
                zeroCopyProjections[i] = accessCurrentRowExpression.getIndex();
            } else {
                allowZeroCopyProjection = false;
            }
            fields.add(exp);
            Column col = Column.column(rowType.getFieldNames().get(i).toLowerCase(),
                convertToHerdType(node.getType()));
            fieldNames[i] = col.name;
            columns[i++] = col;
        }
        if (allowZeroCopyProjection) {
            return new ProjectOp.ZeroCopyProjection(
                fieldNames,
                columns,
                zeroCopyProjections);
        } else {
            return new ProjectOp.BasicProjection(
                fieldNames,
                columns,
                fields);
        }
    }

    private PlannerOp planValues(EnumerableValues op) {

        List<List<CompiledSQLExpression>> tuples = new ArrayList<>(op.getTuples().size());
        RelDataType rowType = op.getRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();

        Column[] columns = new Column[fieldList.size()];
        for (ImmutableList<RexLiteral> tuple : op.getTuples()) {
            List<CompiledSQLExpression> row = new ArrayList<>(tuple.size());
            for (RexLiteral node : tuple) {
                CompiledSQLExpression exp = SQLExpressionCompiler.compileExpression(node);
                row.add(exp);
            }
            tuples.add(row);
        }
        int i = 0;
        String[] fieldNames = new String[fieldList.size()];
        for (RelDataTypeField field : fieldList) {
            Column col = Column.column(field.getName(), convertToHerdType(field.getType()));
            fieldNames[i] = field.getName();
            columns[i++] = col;
        }
        return new ValuesOp(manager.getNodeId(), fieldNames,
            columns, tuples);

    }

    private PlannerOp planSort(EnumerableSort op, RelDataType rowType) {
        PlannerOp input = convertRelNode(op.getInput(), rowType, false);
        RelCollation collation = op.getCollation();
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        boolean[] directions = new boolean[fieldCollations.size()];
        int[] fields = new int[fieldCollations.size()];
        int i = 0;
        for (RelFieldCollation col : fieldCollations) {
            RelFieldCollation.Direction direction = col.getDirection();
            int index = col.getFieldIndex();
            directions[i] = direction == RelFieldCollation.Direction.ASCENDING
                || direction == RelFieldCollation.Direction.STRICTLY_ASCENDING;
            fields[i++] = index;
        }
        return new SortOp(input, directions, fields);

    }

    private PlannerOp planInterpreter(EnumerableInterpreter op, RelDataType rowType, boolean returnValues) {
        // NOOP
        return convertRelNode(op.getInput(), rowType, returnValues);
    }

    private PlannerOp planLimit(EnumerableLimit op, RelDataType rowType) {
        PlannerOp input = convertRelNode(op.getInput(), rowType, false);
        CompiledSQLExpression maxRows = SQLExpressionCompiler.compileExpression(op.fetch);
        CompiledSQLExpression offset = SQLExpressionCompiler.compileExpression(op.offset);
        return new LimitOp(input, maxRows, offset);

    }

    private PlannerOp planFilter(EnumerableFilter op, RelDataType rowType, boolean returnValues) {
        PlannerOp input = convertRelNode(op.getInput(), rowType, returnValues);
        CompiledSQLExpression condition = SQLExpressionCompiler.compileExpression(op.getCondition());
        return new FilterOp(input, condition);

    }

    private PlannerOp planEnumerableUnion(EnumerableUnion op, RelDataType rowType, boolean returnValues) {
        if (!op.all) {
            throw new StatementExecutionException("not suppoer UNION, all=false");
        }
        List<PlannerOp> inputs = new ArrayList<>(op.getInputs().size());
        for (RelNode input : op.getInputs()) {
            PlannerOp inputOp = convertRelNode(input, rowType, false).optimize();
            inputs.add(inputOp);
        }
        return new UnionAllOp(inputs);

    }

    private PlannerOp planAggregate(EnumerableAggregate op, RelDataType rowType, boolean returnValues) {

        List<RelDataTypeField> fieldList = op.getRowType().getFieldList();

        List<AggregateCall> calls = op.getAggCallList();
        String[] fieldnames = new String[fieldList.size()];
        String[] aggtypes = new String[calls.size()];
        Column[] columns = new Column[fieldList.size()];
        List<Integer> groupedFiledsIndexes = op.getGroupSet().toList();
        List<List<Integer>> argLists = new ArrayList<>(calls.size());
        int i = 0;

        int idaggcall = 0;
        for (RelDataTypeField c : fieldList) {
            int type = convertToHerdType(c.getType());
            Column co = Column.column(c.getName(), type);
            columns[i] = co;
            fieldnames[i] = c.getName().toLowerCase();
            i++;
        }
        for (AggregateCall call : calls) {
            aggtypes[idaggcall++] = call.getAggregation().getName();
            argLists.add(call.getArgList());
        }
        PlannerOp input = convertRelNode(op.getInput(), null, returnValues);
        return new AggregateOp(input, fieldnames, columns, aggtypes, argLists, groupedFiledsIndexes);
    }

    public static int convertToHerdType(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR:
            case CHAR:
                return ColumnTypes.STRING;
            case BOOLEAN:
                return ColumnTypes.BOOLEAN;
            case INTEGER:
                return ColumnTypes.INTEGER;
            case BIGINT:
                return ColumnTypes.LONG;
            case VARBINARY:
                return ColumnTypes.BYTEARRAY;
            case NULL:
                return ColumnTypes.NULL;
            case TIMESTAMP:
                return ColumnTypes.TIMESTAMP;
            case DECIMAL:
                return ColumnTypes.DOUBLE;
            case ANY:
                return ColumnTypes.ANYTYPE;
            default:
                throw new StatementExecutionException("unsupported expression type " + type.getSqlTypeName());
        }
    }

    private static SQLRecordKeyFunction findIndexAccess(CompiledSQLExpression where,
        String[] columnsToMatch, ColumnsList table,
        String operator, BindableTableScanColumnNameResolver res) throws StatementExecutionException {
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

    private IndexOperation scanForIndexAccess(CompiledSQLExpression expressionWhere, Table table, TableSpaceManager tableSpaceManager) {
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

        if (result == null) {
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

    private static IndexOperation findSecondaryIndexOperation(AbstractIndexManager index,
        CompiledSQLExpression where, Table table) throws StatementExecutionException {
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

    private static boolean isCachable(String query) {
        return true;
    }

    private static class TableImpl extends AbstractTable
        implements ModifiableTable, ScannableTable, ProjectableFilterableTable {

        final AbstractTableManager tableManager;
        final ImmutableList<ImmutableBitSet> keys;

        private TableImpl(AbstractTableManager tableManager) {
            this.tableManager = tableManager;
            ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
            Table table = tableManager.getTable();
            int index = 0;
            for (Column c : table.getColumns()) {
                if (table.isPrimaryKeyColumn(c.name)) {
                    builder.set(index);
                }
                index++;
            }
            keys = ImmutableList.of(builder.build());
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
            Table table = tableManager.getTable();
            for (Column c : table.getColumns()) {
                boolean nullable = !table.isPrimaryKeyColumn(c.name) || table.auto_increment;
                builder.add(c.name, convertType(c.type, typeFactory, nullable));
            }
            return builder.build();
        }

        @Override
        public Statistic getStatistic() {
            return Statistics.of(tableManager.getStats().getTablesize(),
                keys);
        }

        @Override
        public Collection getModifiableCollection() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
            return LogicalTableModify.create(table, catalogReader, child, operation,
                updateColumnList, sourceExpressionList, flattened);
        }

        @Override
        public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Type getElementType() {
            return Object.class;
        }

        @Override
        public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        private static RelDataType convertType(int type,
            RelDataTypeFactory typeFactory, boolean nullable) {
            RelDataType relDataType = convertTypeNotNull(type, typeFactory);
            if (nullable) {
                return typeFactory.createTypeWithNullability(relDataType, true);
            } else {
                return relDataType;
            }
        }

        private static RelDataType convertTypeNotNull(int type,
            RelDataTypeFactory typeFactory) {

            switch (type) {
                case ColumnTypes.BOOLEAN:
                    return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                case ColumnTypes.INTEGER:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case ColumnTypes.STRING:
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR);
                case ColumnTypes.BYTEARRAY:
                    return typeFactory.createSqlType(SqlTypeName.VARBINARY);
                case ColumnTypes.LONG:
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case ColumnTypes.TIMESTAMP:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                default:
                    return typeFactory.createSqlType(SqlTypeName.ANY);

            }
        }

    }

}
