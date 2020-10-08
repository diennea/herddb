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

import static herddb.model.Column.column;
import static herddb.sql.functions.ShowCreateTableCalculator.calculateShowCreateTable;
import com.google.common.collect.ImmutableList;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.AutoIncrementPrimaryKeyRecordFunction;
import herddb.model.Column;
import herddb.model.ColumnTypes;
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
import herddb.model.planner.FilterOp;
import herddb.model.planner.FilteredTableScanOp;
import herddb.model.planner.InsertOp;
import herddb.model.planner.JoinOp;
import herddb.model.planner.LimitOp;
import herddb.model.planner.NestedLoopJoinOp;
import herddb.model.planner.PlannerOp;
import herddb.model.planner.ProjectOp;
import herddb.model.planner.ReplaceOp;
import herddb.model.planner.SemiJoinOp;
import herddb.model.planner.SimpleDeleteOp;
import herddb.model.planner.SimpleInsertOp;
import herddb.model.planner.SimpleUpdateOp;
import herddb.model.planner.SortOp;
import herddb.model.planner.TableScanOp;
import herddb.model.planner.UnionAllOp;
import herddb.model.planner.UpdateOp;
import herddb.model.planner.ValuesOp;
import herddb.sql.expressions.AccessCurrentRowExpression;
import herddb.sql.expressions.CompiledMultiAndExpression;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.sql.expressions.ConstantExpression;
import herddb.sql.expressions.JdbcParameterExpression;
import herddb.sql.expressions.SQLExpressionCompiler;
import herddb.sql.expressions.TypedJdbcParameterExpression;
import herddb.utils.SQLUtils;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.adapter.enumerable.EnumerableTableModify;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.enumerable.EnumerableUnion;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.interpreter.Bindables.BindableTableScan;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
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
public class CalcitePlanner extends AbstractSQLPlanner {

    /**
     * Time to wait for the requested tablespace to be up
     */

    private static final Pattern USE_DDL_PARSER = Pattern.compile("^[\\s]*(EXECUTE|CREATE|DROP|ALTER|TRUNCATE|BEGIN|COMMIT|ROLLBACK).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final AbstractSQLPlanner fallback;
    public static final String TABLE_CONSISTENCY_COMMAND = "tableconsistencycheck";
    public static final String TABLESPACE_CONSISTENCY_COMMAND = "tablespaceconsistencycheck";

    private static final SqlTypeFactoryImpl SQL_TYPE_FACTORY_IMPL = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private static final RexBuilder REX_BUILDER  = new RexBuilder(SQL_TYPE_FACTORY_IMPL);

    public CalcitePlanner(DBManager manager, PlansCache plansCache) {
        super(manager);
        this.cache = plansCache;
        //used only for DDL
        this.fallback = new JSQLParserPlanner(manager, cache, null);
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

    static final boolean isDDL(String query) {
        // this is quite expensive and it allocates temporary objects
        return USE_DDL_PARSER.matcher(query).matches();
    }

    @Override
    public TranslatedQuery translate(String defaultTableSpace, String query, List<Object> parameters, boolean scan, boolean allowCache, boolean returnValues, int maxRows) throws StatementExecutionException {
        ensureDefaultTableSpaceBootedLocally(defaultTableSpace);
        /* Strips out leading comments */
        int idx = SQLUtils.findQueryStart(query);
        if (idx != -1) {
            query = query.substring(idx);
        }
        if (parameters == null) {
            parameters = Collections.emptyList();
        }
        String cacheKey = "scan:" + scan
                + ",defaultTableSpace:" + defaultTableSpace
                + ",query:" + query
                + ",returnValues:" + returnValues
                + ",maxRows:" + maxRows;
        boolean forceAcquireWriteLock;
        if (query.endsWith(" FOR UPDATE") // this looks very hacky
                && query.substring(0, 6).toLowerCase().equals("select")) {
            forceAcquireWriteLock = true;
            query = query.substring(0, query.length() - " FOR UPDATE".length());
        } else {
            forceAcquireWriteLock = false;
        }
        if (allowCache) {
            ExecutionPlan cached = cache.get(cacheKey);
            if (cached != null) {
                return new TranslatedQuery(cached, new SQLStatementEvaluationContext(query, parameters, forceAcquireWriteLock, false));
            }
        }

        if (isDDL(query)) {
            query = JSQLParserPlanner.rewriteExecuteSyntax(query);
            return fallback.translate(defaultTableSpace, query, parameters, scan, allowCache, returnValues, maxRows);
        }
        if (query.startsWith(TABLE_CONSISTENCY_COMMAND)) {
            query = JSQLParserPlanner.rewriteExecuteSyntax(query);
            return fallback.translate(defaultTableSpace, query, parameters, scan, allowCache, returnValues, maxRows);
        }
        if (query.startsWith(TABLESPACE_CONSISTENCY_COMMAND)) {
            query = JSQLParserPlanner.rewriteExecuteSyntax(query);
            return fallback.translate(defaultTableSpace, query, parameters, scan, allowCache, returnValues, maxRows);
        }
        if (!isCachable(query)) {
            allowCache = false;
        }
        try {
            if (query.startsWith("EXPLAIN ")) {
                query = query.substring("EXPLAIN ".length());
                PlannerResult plan = runPlanner(defaultTableSpace, query);
                boolean upsert = detectUpsert(plan);
                PlannerOp finalPlan = convertRelNode(plan.topNode, plan.originalRowType, returnValues, upsert)
                        .optimize();
                ValuesOp values = new ValuesOp(manager.getNodeId(),
                        new String[]{"name", "value"},
                        new Column[]{
                                column("name", ColumnTypes.STRING),
                                column("value", ColumnTypes.STRING)},
                        java.util.Arrays.asList(
                                java.util.Arrays.asList(
                                        new ConstantExpression("query", ColumnTypes.NOTNULL_STRING),
                                        new ConstantExpression(query, ColumnTypes.NOTNULL_STRING)
                                ),
                                java.util.Arrays.asList(
                                        new ConstantExpression("logicalplan", ColumnTypes.NOTNULL_STRING),
                                        new ConstantExpression(RelOptUtil.dumpPlan("", plan.logicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES)
                                        , ColumnTypes.NOTNULL_STRING)),
                                java.util.Arrays.asList(
                                        new ConstantExpression("plan", ColumnTypes.NOTNULL_STRING),
                                        new ConstantExpression(RelOptUtil.dumpPlan("", plan.topNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES), ColumnTypes.NOTNULL_STRING)
                                ),
                                java.util.Arrays.asList(
                                        new ConstantExpression("finalplan", ColumnTypes.NOTNULL_STRING),
                                        new ConstantExpression(finalPlan + "", ColumnTypes.NOTNULL_STRING)
                                )
                        )
                );
                ExecutionPlan executionPlan = ExecutionPlan.simple(
                        new SQLPlannedOperationStatement(values),
                        values
                );
                return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters, false, false));
            }

            if (query.startsWith("SHOW")) {
                return calculateShowCreateTable(query, defaultTableSpace, parameters, manager);
            }

            PlannerResult plan = runPlanner(defaultTableSpace, query);
            boolean upsert = detectUpsert(plan);
            SQLPlannedOperationStatement sqlPlannedOperationStatement = new SQLPlannedOperationStatement(
                    convertRelNode(plan.topNode, plan.originalRowType, returnValues, upsert)
                            .optimize());
            if (LOG.isLoggable(DUMP_QUERY_LEVEL)) {
                LOG.log(DUMP_QUERY_LEVEL, "Query: {0} --HerdDB Plan\n{1}",
                        new Object[]{query, sqlPlannedOperationStatement.getRootOp()});
            }
            if (!scan) {
                ScanStatement scanStatement = sqlPlannedOperationStatement.unwrap(ScanStatement.class);
                if (scanStatement != null) {
                    Table tableDef = scanStatement.getTableDef();
                    CompiledSQLExpression where = scanStatement.getPredicate().unwrap(CompiledSQLExpression.class);
                    SQLRecordKeyFunction keyFunction = IndexUtils.findIndexAccess(where, tableDef.getPrimaryKey(),
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
                    return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters, forceAcquireWriteLock, false));
                }
            }
            if (maxRows > 0) {
                PlannerOp op = new LimitOp(sqlPlannedOperationStatement.getRootOp(),
                        new ConstantExpression(maxRows, ColumnTypes.NOTNULL_LONG), new ConstantExpression(0, ColumnTypes.NOTNULL_LONG))
                        .optimize();
                sqlPlannedOperationStatement = new SQLPlannedOperationStatement(op);
            }
            PlannerOp rootOp = sqlPlannedOperationStatement.getRootOp();
            ExecutionPlan executionPlan;
            if (rootOp.isSimpleStatementWrapper()) {
                executionPlan = ExecutionPlan.simple(rootOp.unwrap(herddb.model.Statement.class), rootOp);
            } else {
                executionPlan = ExecutionPlan.simple(sqlPlannedOperationStatement, rootOp);
            }
            if (allowCache) {
                cache.put(cacheKey, executionPlan);
            }
            return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters, forceAcquireWriteLock, false));
        } catch (CalciteContextException ex) {
            LOG.log(Level.INFO, "Error while parsing '" + ex.getOriginalStatement() + "'", ex);
            //TODO can this be done better ?
            throw new StatementExecutionException(ex.getMessage());
        } catch (RelConversionException | ValidationException | SqlParseException ex) {
            LOG.log(Level.INFO, "Error while parsing '" + query + "'", ex);
            //TODO can this be done better ?
            throw new StatementExecutionException(ex.getMessage().replace("org.apache.calcite.runtime.CalciteContextException: ", ""), ex);
        } catch (MetadataStorageManagerException ex) {
            LOG.log(Level.INFO, "Error while parsing '" + query + "'", ex);
            throw new StatementExecutionException(ex);
        }
    }

    private static boolean detectUpsert(PlannerResult res) {
        if (res.sql instanceof SqlInsert) {
            SqlInsert si = (SqlInsert) res.sql;
            return si.isUpsert();
        }
        return false;
    }

    private SchemaPlus getSchemaForTableSpace(String defaultTableSpace) {
        long startTs = System.currentTimeMillis();
        while (true) {
            SchemaPlus schema = getRootSchema();
            SchemaPlus result = schema.getSubSchema(defaultTableSpace);
            if (result != null) {
                return result;
            }
            long delta = System.currentTimeMillis() - startTs;
            LOG.log(Level.FINE, "schema {0} not available yet, after waiting {1}/{2} ms",
                    new Object[]{defaultTableSpace, delta, waitForSchemaTimeout});
            if (delta >= waitForSchemaTimeout) {
                return null;
            }
            clearCache();
            try {
                Thread.sleep(100);
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final SqlParser.Config SQL_PARSER_CONFIG =
            SqlParser.configBuilder(SqlParser.Config.DEFAULT)
                    .setCaseSensitive(false)
                    .setConformance(SqlConformanceEnum.MYSQL_5)
                    .setQuoting(Quoting.BACK_TICK)
                    .build();


    private static final Logger LOG = Logger.getLogger(CalcitePlanner.class
            .getName());

    private static class PlannerResult {

        private final RelNode topNode;
        private final RelDataType originalRowType;
        private final RelNode logicalPlan;
        private final SqlNode sql;

        public PlannerResult(RelNode topNode, RelDataType originalRowType, RelNode logicalPlan, final SqlNode sql) {
            this.topNode = topNode;
            this.originalRowType = originalRowType;
            this.logicalPlan = logicalPlan;
            this.sql = sql;
        }

    }

    private static final List<RelTraitDef> TRAITS = Collections
            .unmodifiableList(java.util.Arrays.asList(ConventionTraitDef.INSTANCE,
                    RelCollationTraitDef.INSTANCE));

    private PlannerResult runPlanner(String defaultTableSpace, String query) throws RelConversionException,
            SqlParseException, ValidationException, MetadataStorageManagerException, StatementExecutionException {
        try {
            SchemaPlus subSchema = getSchemaForTableSpace(defaultTableSpace);
            if (subSchema == null) {
                clearCache();
                throw new StatementExecutionException("tablespace " + defaultTableSpace + " is not available");
            }

            Properties props = new Properties();
            props.put(CalciteConnectionProperty.TIME_ZONE.camelName(), TimeZone.getDefault().getID());
            props.put(CalciteConnectionProperty.LOCALE.camelName(), Locale.ROOT.toString());
            props.put(CalciteConnectionProperty.DEFAULT_NULL_COLLATION.camelName(), NullCollation.LAST.toString());
            final CalciteConnectionConfigImpl calciteRuntimeContextConfig = new CalciteConnectionConfigImpl(props);

            final FrameworkConfig config = Frameworks.newConfigBuilder()
                    .parserConfig(SQL_PARSER_CONFIG)
                    .defaultSchema(subSchema)
                    .traitDefs(TRAITS)
                    .context(new Context() {
                        @Override
                        public <C> C unwrap(Class<C> aClass) {
                            if (aClass == CalciteConnectionConfigImpl.class
                                    || aClass == CalciteConnectionConfig.class) {
                                return (C) calciteRuntimeContextConfig;
                            }
                            return null;
                        }
                    })
                    // define the rules you want to apply
                    .programs(Programs.ofRules(Programs.RULE_SET))
                    .build();
            Planner planner = new PlannerImpl(config);
            if (LOG.isLoggable(Level.FINER)) {
                LOG.log(Level.FINER, "Query: {0}", query);
            }
            try {
                SqlNode n = planner.parse(query);
                n = planner.validate(n);
                RelNode logicalPlan = planner.rel(n).project();
                if (LOG.isLoggable(DUMP_QUERY_LEVEL)) {
                    LOG.log(DUMP_QUERY_LEVEL, "Query: {0} {1}", new Object[]{query,
                        RelOptUtil.dumpPlan("-- Logical Plan", logicalPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.ALL_ATTRIBUTES)});
                }
                RelDataType originalRowType = logicalPlan.getRowType();
                RelOptCluster cluster = logicalPlan.getCluster();
                final RelOptPlanner optPlanner = cluster.getPlanner();

                optPlanner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);
                RelTraitSet desiredTraits =
                        cluster.traitSet()
                                .replace(EnumerableConvention.INSTANCE);
                final RelCollation collation =
                        logicalPlan instanceof Sort
                                ? ((Sort) logicalPlan).collation
                                : null;
                if (collation != null) {
                    desiredTraits = desiredTraits.replace(collation);
                }
                final RelNode newRoot = optPlanner.changeTraits(logicalPlan, desiredTraits);
                optPlanner.setRoot(newRoot);
                RelNode bestExp = optPlanner.findBestExp();
                if (LOG.isLoggable(DUMP_QUERY_LEVEL)) {
                    LOG.log(DUMP_QUERY_LEVEL, "Query: {0} {1}", new Object[]{query,
                        RelOptUtil.dumpPlan("-- Best  Plan", bestExp, SqlExplainFormat.TEXT,
                        SqlExplainLevel.ALL_ATTRIBUTES)});
                }

                return new PlannerResult(bestExp, originalRowType, logicalPlan, n);
            } catch (AssertionError err) {
                throw new StatementExecutionException("Internal Calcite error " + err, err);
            }
        } catch (java.lang.LinkageError err) {
            LOG.log(Level.SEVERE, "Error on Java Classpath", err);
            throw new StatementExecutionException("Internal Calcite error " + err, err);
        }
    }

    private SchemaPlus getRootSchema() {
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

    private PlannerOp convertRelNode(
            RelNode plan,
            RelDataType rowType, boolean returnValues, boolean upsert
    ) throws StatementExecutionException {
        if (plan instanceof EnumerableTableModify) {
            EnumerableTableModify dml = (EnumerableTableModify) plan;
            switch (dml.getOperation()) {
                case INSERT:
                    return planInsert(dml, returnValues, upsert);
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
        } else if (plan instanceof EnumerableHashJoin) {
            EnumerableHashJoin scan = (EnumerableHashJoin) plan;
            return planEnumerableHashJoin(scan, rowType);
        } else if (plan instanceof EnumerableNestedLoopJoin) {
            EnumerableNestedLoopJoin scan = (EnumerableNestedLoopJoin) plan;
            return planEnumerableNestedLoopJoin(scan, rowType);
        } else if (plan instanceof EnumerableHashJoin) {
            EnumerableHashJoin scan = (EnumerableHashJoin) plan;
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

    private PlannerOp planInsert(
            EnumerableTableModify dml,
            boolean returnValues,
            boolean upsert
    ) {

        final String tableSpace = dml.getTable().getQualifiedName().get(0);
        final String tableName = dml.getTable().getQualifiedName().get(1);
        DMLStatement statement = null;
        if (dml.getInput() instanceof EnumerableProject) {
            // fastest path for insert into TABLE(s,b,c) values(?,?,?)
            EnumerableProject project = (EnumerableProject) dml.getInput();
            if (project.getInput() instanceof EnumerableValues) {
                EnumerableValues values = (EnumerableValues) project.getInput();
                if (values.getTuples().size() == 1) {
                    final TableImpl tableImpl =
                            (TableImpl) dml.getTable().unwrap(org.apache.calcite.schema.Table.class
                            );
                    Table table = tableImpl.tableManager.getTable();
                    int index = 0;
                    List<RexNode> projects = project.getProjects();
                    List<CompiledSQLExpression> keyValueExpression = new ArrayList<>();
                    List<String> keyExpressionToColumn = new ArrayList<>();
                    List<CompiledSQLExpression> valuesExpressions = new ArrayList<>();
                    List<String> valuesColumns = new ArrayList<>();
                    boolean invalid = false;
                    for (Column column : table.getColumns()) {
                        CompiledSQLExpression exp =
                                SQLExpressionCompiler.compileExpression(projects.get(index));
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
                        statement = new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction, upsert).setReturnValues(returnValues);
                    }
                }
            }
        }
        if (statement != null) {
            return new SimpleInsertOp(statement);
        }
        PlannerOp input = convertRelNode(dml.getInput(),
                null, false, false);

        try {
            return new InsertOp(tableSpace, tableName, input, returnValues, upsert);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private PlannerOp planDelete(EnumerableTableModify dml) {
        PlannerOp input = convertRelNode(dml.getInput(), null, false, false);

        final String tableSpace = dml.getTable().getQualifiedName().get(0);
        final String tableName = dml.getTable().getQualifiedName().get(1);
        final TableImpl tableImpl =
                (TableImpl) dml.getTable().unwrap(org.apache.calcite.schema.Table.class
                );
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
        PlannerOp input = convertRelNode(dml.getInput(), null, false, false);
        List<String> updateColumnList = dml.getUpdateColumnList();
        List<RexNode> sourceExpressionList = dml.getSourceExpressionList();
        final String tableSpace = dml.getTable().getQualifiedName().get(0);
        final String tableName = dml.getTable().getQualifiedName().get(1);
        final TableImpl tableImpl =
                (TableImpl) dml.getTable().unwrap(org.apache.calcite.schema.Table.class
                );
        Table table = tableImpl.tableManager.getTable();
        List<CompiledSQLExpression> expressionsForValue = new ArrayList<>(sourceExpressionList.size());
        List<CompiledSQLExpression> expressionsForKey = new ArrayList<>(sourceExpressionList.size());

        List<String> updateColumnListInValue = new ArrayList<>(updateColumnList.size());
        List<String> updateColumnListInPk = new ArrayList<>();
        for (int i = 0; i < updateColumnList.size(); i++) {
            String columnName = updateColumnList.get(i);
            boolean isPk = table.isPrimaryKeyColumn(columnName);
            RexNode node = sourceExpressionList.get(i);
            CompiledSQLExpression exp = SQLExpressionCompiler.compileExpression(node);
            if (isPk) {
                updateColumnListInPk.add(columnName);
                expressionsForKey.add(exp);
            } else {
                updateColumnListInValue.add(columnName);
                expressionsForValue.add(exp);
            }
        }
        if (expressionsForKey.isEmpty()) {
            // standard UPDATE, we are not updating any column in the PK
            RecordFunction function = new SQLRecordFunction(updateColumnListInValue, table, expressionsForValue);
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
        } else {
            // bad stuff ! we are updating the PK, we need to transform this to a sequence of delete and inserts
            // ReplaceOp won't execute the two statements atomically
            RecordFunction functionForValue = new SQLRecordFunction(updateColumnListInValue, table, expressionsForValue);
            SQLRecordKeyFunction functionForKey = new SQLRecordKeyFunction(updateColumnListInPk, expressionsForKey, table);
            return new ReplaceOp(tableSpace, tableName, input, returnValues, functionForKey, functionForValue);
        }
    }

    private PlannerOp planEnumerableTableScan(EnumerableTableScan scan, RelDataType rowType) {
        final String tableSpace = scan.getTable().getQualifiedName().get(0);
        final TableImpl tableImpl =
                (TableImpl) scan.getTable().unwrap(org.apache.calcite.schema.Table.class
                );
        Table table = tableImpl.tableManager.getTable();
        Column[] columns = table.getColumns();
        int numColumns = columns.length;
        boolean usingAliases = false;
        if (rowType != null) {
            List<String> fieldNamesFromQuery = rowType.getFieldNames();
            for (int i = 0; i < numColumns; i++) {
                String fieldName = fieldNamesFromQuery.get(i);
                String alias = fieldName.toLowerCase();
                String colName = columns[i].name;
                if (!alias.equals(colName)) {
                    usingAliases = true;
                    break;
                }
            }
        }
        if (usingAliases) {
            List<String> fieldNamesFromQuery = rowType.getFieldNames();
            String[] fieldNames = new String[numColumns];
            int[] projections = new int[numColumns];
            for (int i = 0; i < numColumns; i++) {
                String alias = fieldNamesFromQuery.get(i).toLowerCase();
                fieldNames[i] = alias;
                projections[i] = i;
            }
            Projection zeroCopy = new ProjectOp.ZeroCopyProjection(
                    fieldNames,
                    columns,
                    projections);
             ScanStatement scanStatement = new ScanStatement(tableSpace, table, zeroCopy, null);
            return new TableScanOp(scanStatement);
        } else {
            ScanStatement scanStatement = new ScanStatement(tableSpace, table, null);
            return new TableScanOp(scanStatement);
        }
    }

    private PlannerOp planBindableTableScan(BindableTableScan scan, RelDataType rowType) {
        if (rowType == null) {
            rowType = scan.getRowType();
        }
        final String tableSpace = scan.getTable().getQualifiedName().get(0);
        final TableImpl tableImpl =
                (TableImpl) scan.getTable().unwrap(org.apache.calcite.schema.Table.class
                );
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
            IndexUtils.discoverIndexOperations(tableSpace, where, table, predicate, scan, tableSpaceManager);
        }
        List<RexNode> projections = new ArrayList<>(scan.projects.size());

        int i = 0;

        for (int fieldpos : scan.projects) {
            projections.add(new RexInputRef(fieldpos, rowType
                    .getFieldList()
                    .get(i++).getType()));
        }
        Projection projection = buildProjection(projections, rowType, true, table.columns);
        ScanStatement scanStatement = new ScanStatement(tableSpace, table.name, projection, predicate, null, null);
        scanStatement.setTableDef(table);
        return new BindableTableScanOp(scanStatement);
    }

    private PlannerOp planProject(EnumerableProject op, RelDataType rowType) {
        PlannerOp input = convertRelNode(op.getInput(), null, false, false);

        final List<RexNode> projects = op.getProjects();
        final RelDataType _rowType = rowType == null ? op.getRowType() : rowType;
        Projection projection = buildProjection(projects, _rowType, false, null);
        return new ProjectOp(projection, input);
    }

    private PlannerOp planEnumerableHashJoin(EnumerableHashJoin op, RelDataType rowType) {
        // please note that EnumerableSemiJoin has a condition field which actually is not useful
        PlannerOp left = convertRelNode(op.getLeft(), null, false, false);
        PlannerOp right = convertRelNode(op.getRight(), null, false, false);
        final JoinInfo analyzeCondition = op.analyzeCondition();
        int[] leftKeys = analyzeCondition.leftKeys.toIntArray();
        int[] rightKeys = analyzeCondition.rightKeys.toIntArray();
        boolean generateNullsOnLeft = op.getJoinType().generatesNullsOnLeft();
        boolean generateNullsOnRight = op.getJoinType().generatesNullsOnRight();
        List<CompiledSQLExpression> nonEquiConditions = convertJoinNonEquiConditions(analyzeCondition);

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
        if (op.isSemiJoin()) {
            return new SemiJoinOp(fieldNames, columns, leftKeys, left, rightKeys, right);
        } else {
            return new JoinOp(fieldNames, columns,
                    leftKeys, left, rightKeys, right,
                    generateNullsOnLeft, generateNullsOnRight, false,
                    nonEquiConditions);
        }
    }

    private List<CompiledSQLExpression> convertJoinNonEquiConditions(final JoinInfo analyzeCondition) throws IllegalStateException {
        List<CompiledSQLExpression> nonEquiConditions = new ArrayList<>();
        if (!analyzeCondition.isEqui()) {
            for (RexNode rexNode : analyzeCondition.nonEquiConditions) {
                nonEquiConditions.add(SQLExpressionCompiler.compileExpression(rexNode));
            }
        } else {
            if (!analyzeCondition.nonEquiConditions.isEmpty()) {
                throw new IllegalStateException("Unexpected non equi with " + analyzeCondition.nonEquiConditions + " conditions");
            }
        }
        return nonEquiConditions;
    }

    private PlannerOp planEnumerableJoin(EnumerableHashJoin op, RelDataType rowType) {
        // please note that EnumerableJoin has a condition field which actually is not useful
        PlannerOp left = convertRelNode(op.getLeft(), null, false, false);
        PlannerOp right = convertRelNode(op.getRight(), null, false, false);
        final JoinInfo analyzeCondition = op.analyzeCondition();
        int[] leftKeys = analyzeCondition.leftKeys.toIntArray();
        int[] rightKeys = analyzeCondition.rightKeys.toIntArray();
        boolean generateNullsOnLeft = op.getJoinType().generatesNullsOnLeft();
        boolean generateNullsOnRight = op.getJoinType().generatesNullsOnRight();
        List<CompiledSQLExpression> nonEquiConditions = convertJoinNonEquiConditions(analyzeCondition);
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
                leftKeys, left, rightKeys, right,
                generateNullsOnLeft, generateNullsOnRight, false,
                nonEquiConditions);
    }

    private PlannerOp planEnumerableNestedLoopJoin(EnumerableNestedLoopJoin op, RelDataType rowType) {
        PlannerOp left = convertRelNode(op.getLeft(), null, false, false);
        PlannerOp right = convertRelNode(op.getRight(), null, false, false);
        CompiledSQLExpression condition = SQLExpressionCompiler.compileExpression(op.getCondition());
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
        return new NestedLoopJoinOp(fieldNames, columns,
                left, right, condition, op.getJoinType(), false);
    }

    private PlannerOp planEnumerableMergeJoin(EnumerableMergeJoin op, RelDataType rowType) {
        // please note that EnumerableMergeJoin has a condition field which actually is not useful
        PlannerOp left = convertRelNode(op.getLeft(), null, false, false);
        PlannerOp right = convertRelNode(op.getRight(), null, false, false);
        final JoinInfo analyzeCondition = op.analyzeCondition();
        int[] leftKeys = analyzeCondition.leftKeys.toIntArray();
        int[] rightKeys = analyzeCondition.rightKeys.toIntArray();
        boolean generateNullsOnLeft = op.getJoinType().generatesNullsOnLeft();
        boolean generateNullsOnRight = op.getJoinType().generatesNullsOnRight();
        final RelDataType _rowType = rowType == null ? op.getRowType() : rowType;
        List<CompiledSQLExpression> nonEquiConditions = convertJoinNonEquiConditions(analyzeCondition);
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
                leftKeys, left, rightKeys, right,
                generateNullsOnLeft, generateNullsOnRight, true,
                nonEquiConditions);
    }

    private Projection buildProjection(
            final List<RexNode> projects,
            final RelDataType rowType,
            final boolean allowIdentity,
            final Column[] tableSchema
    ) {
        boolean allowZeroCopyProjection = true;
        List<CompiledSQLExpression> fields = new ArrayList<>(projects.size());
        Column[] columns = new Column[projects.size()];
        String[] fieldNames = new String[columns.length];
        int i = 0;
        int[] zeroCopyProjections = new int[fieldNames.length];
        boolean identity = allowIdentity
                && tableSchema != null
                && tableSchema.length == fieldNames.length;
        for (RexNode node : projects) {
            CompiledSQLExpression exp = SQLExpressionCompiler.compileExpression(node);
            if (exp instanceof AccessCurrentRowExpression) {
                AccessCurrentRowExpression accessCurrentRowExpression = (AccessCurrentRowExpression) exp;
                int mappedIndex = accessCurrentRowExpression.getIndex();
                zeroCopyProjections[i] = mappedIndex;
                if (i != mappedIndex) {
                    identity = false;
                }
            } else {
                allowZeroCopyProjection = false;
            }
            fields.add(exp);
            Column col = Column.column(rowType.getFieldNames().get(i).toLowerCase(),
                    convertToHerdType(node.getType()));
            identity = identity && col.name.equals(tableSchema[i].name);
            fieldNames[i] = col.name;
            columns[i++] = col;
        }
        if (allowZeroCopyProjection) {
            if (identity) {
                return Projection.IDENTITY(fieldNames, columns);
            }
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
        PlannerOp input = convertRelNode(op.getInput(), rowType, false, false);
        RelCollation collation = op.getCollation();
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        boolean[] directions = new boolean[fieldCollations.size()];
        boolean[] nullLastdirections = new boolean[fieldCollations.size()];
        int[] fields = new int[fieldCollations.size()];
        int i = 0;
        for (RelFieldCollation col : fieldCollations) {
            RelFieldCollation.Direction direction = col.getDirection();
            int index = col.getFieldIndex();
            RelFieldCollation.NullDirection nullDirection = col.nullDirection;
            directions[i] = direction == RelFieldCollation.Direction.ASCENDING
                    || direction == RelFieldCollation.Direction.STRICTLY_ASCENDING;
            // default is NULL LAST
            nullLastdirections[i] = nullDirection == RelFieldCollation.NullDirection.LAST
                    || nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED;
            fields[i++] = index;
        }
        return new SortOp(input, directions, fields, nullLastdirections);

    }

    private PlannerOp planInterpreter(EnumerableInterpreter op, RelDataType rowType, boolean returnValues) {
        // NOOP
        return convertRelNode(op.getInput(), rowType, returnValues, false);
    }

    private PlannerOp planLimit(EnumerableLimit op, RelDataType rowType) {
        PlannerOp input = convertRelNode(op.getInput(), rowType, false, false);
        CompiledSQLExpression maxRows = SQLExpressionCompiler.compileExpression(op.fetch);
        CompiledSQLExpression offset = SQLExpressionCompiler.compileExpression(op.offset);
        return new LimitOp(input, maxRows, offset);

    }

    private PlannerOp planFilter(EnumerableFilter op, RelDataType rowType, boolean returnValues) {
        PlannerOp input = convertRelNode(op.getInput(), rowType, returnValues, false);
        CompiledSQLExpression condition = SQLExpressionCompiler.compileExpression(op.getCondition());
        return new FilterOp(input, condition);

    }

    private PlannerOp planEnumerableUnion(EnumerableUnion op, RelDataType rowType, boolean returnValues) {
        if (!op.all) {
            throw new StatementExecutionException("not suppoer UNION, all=false");
        }
        List<PlannerOp> inputs = new ArrayList<>(op.getInputs().size());
        for (RelNode input : op.getInputs()) {
            PlannerOp inputOp = convertRelNode(input, rowType, false, false).optimize();
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
        PlannerOp input = convertRelNode(op.getInput(), null, returnValues, false);
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
            case DATE:
                return ColumnTypes.TIMESTAMP;
            case DECIMAL:
            case DOUBLE:
                return ColumnTypes.DOUBLE;
            case ANY:
            case SYMBOL:
            case INTERVAL_DAY:
                return ColumnTypes.ANYTYPE;
            default:
                throw new StatementExecutionException("unsupported expression type " + type.getSqlTypeName());
        }
    }

    private static boolean isCachable(String query) {
        return true;

    }

    private static final class TableImpl extends AbstractTable
            implements ModifiableTable, ScannableTable, ProjectableFilterableTable, InitializerExpressionFactory, Wrapper {

        final AbstractTableManager tableManager;
        final Table table;
        final ImmutableList<ImmutableBitSet> keys;


        private TableImpl(AbstractTableManager tableManager) {
            this.tableManager = tableManager;
            ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
            this.table = tableManager.getTable();
            int index = 0;
            for (Column c : table.getColumns()) {
                if (table.isPrimaryKeyColumn(c.name)) {
                    builder.set(index);
                }
                index++;
            }
            keys = ImmutableList.of(builder.build());
        }

        private static boolean isColumnNullable(Column c, Table t) {
            return  (!t.isPrimaryKeyColumn(c.name) || t.auto_increment) && !ColumnTypes.isNotNullDataType(c.type);
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            if (aClass == InitializerExpressionFactory.class) {
                return (C) this;
            }
            return super.unwrap(aClass);
        }

        @Override
        public boolean isGeneratedAlways(RelOptTable table, int iColumn) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ColumnStrategy generationStrategy(RelOptTable table, int iColumn) {
            Column col = this.table.getColumn(iColumn);
            if (col.defaultValue != null) {
                return ColumnStrategy.DEFAULT;
            } else if (!isColumnNullable(col, this.table)) {
                return ColumnStrategy.NOT_NULLABLE;
            } else {
                return ColumnStrategy.NULLABLE;
            }
        }

        @Override
        public RexNode newColumnDefaultValue(RelOptTable table, int iColumn, InitializerContext context) {
            Column col = this.table.getColumn(iColumn);
            if (col.defaultValue != null) {
                switch (col.type) {
                    case ColumnTypes.NOTNULL_STRING:
                    case ColumnTypes.STRING:
                        return REX_BUILDER.makeLiteral(col.defaultValue.to_string());
                    case ColumnTypes.NOTNULL_INTEGER:
                    case ColumnTypes.INTEGER:
                        return REX_BUILDER.makeLiteral(col.defaultValue.to_int(), SQL_TYPE_FACTORY_IMPL.createSqlType(SqlTypeName.INTEGER), true);
                    case ColumnTypes.NOTNULL_LONG:
                    case ColumnTypes.LONG:
                        return REX_BUILDER.makeLiteral(col.defaultValue.to_long(), SQL_TYPE_FACTORY_IMPL.createSqlType(SqlTypeName.BIGINT), true);
                    case ColumnTypes.NOTNULL_DOUBLE:
                    case ColumnTypes.DOUBLE:
                        return REX_BUILDER.makeLiteral(col.defaultValue.to_double(), SQL_TYPE_FACTORY_IMPL.createSqlType(SqlTypeName.DOUBLE), true);
                    case ColumnTypes.NOTNULL_BOOLEAN:
                    case ColumnTypes.BOOLEAN:
                        return REX_BUILDER.makeLiteral(col.defaultValue.to_boolean());
                    case ColumnTypes.NOTNULL_TIMESTAMP:
                    case ColumnTypes.TIMESTAMP:
                        return REX_BUILDER.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP);
                    default:
                        throw new UnsupportedOperationException("Not supported for type " + ColumnTypes.typeToString(col.type));
                }
            }
            return REX_BUILDER.makeNullLiteral(convertType(col.type, SQL_TYPE_FACTORY_IMPL, true));
        }

        @Override
        public BiFunction<InitializerContext, RelNode, RelNode> postExpressionConversionHook() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public RexNode newAttributeInitializer(RelDataType type, SqlFunction constructor, int iAttribute, List<RexNode> constructorArgs, InitializerContext context) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
            Table table = tableManager.getTable();
            for (Column c : table.getColumns()) {
                boolean nullable = isColumnNullable(c, table);
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
            return null;
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        private static RelDataType convertType(
                int type,
                RelDataTypeFactory typeFactory, boolean nullable
        ) {
            RelDataType relDataType = convertTypeNotNull(type, typeFactory);
            if (nullable) {
                return typeFactory.createTypeWithNullability(relDataType, true);
            } else {
                return relDataType;
            }
        }

        private static RelDataType convertTypeNotNull(
                int type,
                RelDataTypeFactory typeFactory
        ) {

            switch (type) {
                case ColumnTypes.BOOLEAN:
                    return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                case ColumnTypes.INTEGER:
                case ColumnTypes.NOTNULL_INTEGER:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case ColumnTypes.STRING:
                case ColumnTypes.NOTNULL_STRING:
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR);
                case ColumnTypes.BYTEARRAY:
                    return typeFactory.createSqlType(SqlTypeName.VARBINARY);
                case ColumnTypes.LONG:
                case ColumnTypes.NOTNULL_LONG:
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case ColumnTypes.TIMESTAMP:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                default:
                    return typeFactory.createSqlType(SqlTypeName.ANY);

            }
        }

    }

}
