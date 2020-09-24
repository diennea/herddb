/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.sql;

import static herddb.sql.expressions.SQLParserExpressionCompiler.findColumnInSchema;
import static herddb.sql.expressions.SQLParserExpressionCompiler.isBooleanLiteral;
import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.core.HerdDBInternalException;
import herddb.core.TableSpaceManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.AutoIncrementPrimaryKeyRecordFunction;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.DMLStatement;
import herddb.model.ExecutionPlan;
import herddb.model.FullTableScanPredicate;
import herddb.model.Predicate;
import herddb.model.Projection;
import herddb.model.RecordFunction;
import herddb.model.ScanLimits;
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TupleComparator;
import herddb.model.commands.AlterTableSpaceStatement;
import herddb.model.commands.AlterTableStatement;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.DropIndexStatement;
import herddb.model.commands.DropTableSpaceStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.SQLPlannedOperationStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.TableConsistencyCheckStatement;
import herddb.model.commands.TableSpaceConsistencyCheckStatement;
import herddb.model.commands.TruncateTableStatement;
import herddb.model.commands.UpdateStatement;
import herddb.model.planner.AggregateOp;
import herddb.model.planner.BindableTableScanOp;
import herddb.model.planner.LimitOp;
import herddb.model.planner.PlannerOp;
import herddb.model.planner.ProjectOp;
import herddb.model.planner.SimpleDeleteOp;
import herddb.model.planner.SimpleInsertOp;
import herddb.model.planner.SimpleUpdateOp;
import herddb.model.planner.SortOp;
import herddb.server.ServerConfiguration;
import herddb.sql.expressions.AccessCurrentRowExpression;
import herddb.sql.expressions.CompiledFunction;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.sql.expressions.ConstantExpression;
import herddb.sql.expressions.JdbcParameterExpression;
import herddb.sql.expressions.SQLParserExpressionCompiler;
import herddb.sql.expressions.TypedJdbcParameterExpression;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.Bytes;
import herddb.utils.IntHolder;
import herddb.utils.SQLUtils;
import herddb.utils.SystemProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;

/**
 * Translates SQL to Internal API
 *
 * @author enrico.olivelli
 */
public class DDLSQLPlanner implements AbstractSQLPlanner {

    private static final Level DUMP_QUERY_LEVEL = Level.parse(SystemProperties.getStringSystemProperty("herddb.planner.dumpqueryloglevel", Level.FINE.toString()));
    public static final String TABLE_CONSISTENCY_COMMAND = "tableconsistencycheck";
    public static final String TABLESPACE_CONSISTENCY_COMMAND = "tablespaceconsistencycheck";
    private final DBManager manager;
    private final PlansCache cache;
    /**
     * Used in case of unsupported Statement
     */
    private final AbstractSQLPlanner fallback;

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
        cache.clear();
        if (fallback != null) {
            fallback.clearCache();
        }
    }

    public DDLSQLPlanner(DBManager manager, PlansCache plansCache,  AbstractSQLPlanner fallback) {
        this.manager = manager;
        this.cache = plansCache;
        this.fallback = fallback;
    }

    public static String rewriteExecuteSyntax(String query) {
        char ch = query.charAt(0);

        /*
         * "empty" data skipped now we must recognize instructions to rewrite
         */
        switch (ch) {
            /*
             * ALTER
             */
            case 'A':
            case 'a':
                if (query.regionMatches(true, 0, "ALTER TABLESPACE ", 0, 17)) {
                    return "EXECUTE altertablespace " + query.substring(17);
                }

                return query;

            /*
             * BEGIN
             */
            case 'B':
            case 'b':
                if (query.regionMatches(true, 0, "BEGIN TRANSACTION", 0, 17)) {
                    return "EXECUTE begintransaction" + query.substring(17);
                }

                return query;

            /*
             * COMMIT / CREATE
             */
            case 'C':
            case 'c':
                ch = query.charAt(1);
                switch (ch) {
                    case 'O':
                    case 'o':
                        if (query.regionMatches(true, 0, "COMMIT TRANSACTION", 0, 18)) {
                            return "EXECUTE committransaction" + query.substring(18);
                        }

                        break;

                    case 'R':
                    case 'r':
                        if (query.regionMatches(true, 0, "CREATE TABLESPACE ", 0, 18)) {
                            return "EXECUTE createtablespace " + query.substring(18);
                        }

                        break;
                }

                return query;

            /*
             * DROP
             */
            case 'D':
            case 'd':
                if (query.regionMatches(true, 0, "DROP TABLESPACE ", 0, 16)) {
                    return "EXECUTE droptablespace " + query.substring(16);
                }

                return query;

            /*
             * ROLLBACK
             */
            case 'R':
            case 'r':
                if (query.regionMatches(true, 0, "ROLLBACK TRANSACTION", 0, 20)) {
                    return "EXECUTE rollbacktransaction" + query.substring(20);
                }
                return query;

            /*
             * TRUNCATE
             */
            case 'T':
            case 't':
                if (query.regionMatches(true, 0, "TRUNCATE", 0, 8)) {
                    return "TRUNCATE" + query.substring(8);
                }
                return query;

            default:
                /*RETURN also consistency command */
                return query;
        }
    }

    @Override
    public TranslatedQuery translate(
            String defaultTableSpace, String query, List<Object> parameters,
            boolean scan, boolean allowCache, boolean returnValues, int maxRows
    ) throws StatementExecutionException {
        if (parameters == null) {
            parameters = Collections.emptyList();
        }

        /*
         * Strips out leading comments
         */
        int idx = SQLUtils.findQueryStart(query);
        if (idx != -1) {
            query = query.substring(idx);
        }

        query = rewriteExecuteSyntax(query);
        String cacheKey = "scan:" + scan
                + ",defaultTableSpace:" + defaultTableSpace
                + ",query:" + query
                + ",returnValues:" + returnValues
                + ",maxRows:" + maxRows;
        try {
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
            if (query.startsWith(TABLE_CONSISTENCY_COMMAND)) {
                ExecutionPlan executionPlan = ExecutionPlan.simple(DDLSQLPlanner.this.queryConsistencyCheckStatement(defaultTableSpace, query, parameters));
                return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters, false, false));
            }
            if (query.startsWith(TABLESPACE_CONSISTENCY_COMMAND)) {
                ExecutionPlan executionPlan = ExecutionPlan.simple(DDLSQLPlanner.this.queryConsistencyCheckStatement(query));
                return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters, false, false));
            }

            net.sf.jsqlparser.statement.Statement stmt = parseStatement(query);
            if (!isCachable(stmt)) {
                allowCache = false;
            }
            ExecutionPlan executionPlan = plan(defaultTableSpace, stmt, scan, returnValues, maxRows);
            if (LOG.isLoggable(DUMP_QUERY_LEVEL)) {
                LOG.log(DUMP_QUERY_LEVEL, "Query: {0} --HerdDB Plan\n{1}",
                        new Object[]{query, executionPlan.mainStatement});
            }
            if (allowCache) {
                cache.put(cacheKey, executionPlan);
            }
            return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters, forceAcquireWriteLock, false));
        } catch (StatementNotSupportedException err) {
            if (fallback == null) {
                throw new StatementExecutionException("I am sorry, I cannot plan SQL \"" + query + "\" with simple jSQLParser planner,"
                        + " consider setting " + ServerConfiguration.PROPERTY_PLANNER_TYPE + "=" + ServerConfiguration.PLANNER_TYPE_AUTO, err);
            }
            TranslatedQuery res =  fallback.translate(defaultTableSpace, query, parameters, scan, allowCache, returnValues, maxRows);
            if (allowCache) {
                // cache plan from Calcite, not need to try jSQLParser again
                cache.put(cacheKey, res.plan);
            }
            return res;
        }
    }

    private net.sf.jsqlparser.statement.Statement parseStatement(String query) throws StatementExecutionException {
        net.sf.jsqlparser.statement.Statement stmt;

        CCJSqlParser parser = new CCJSqlParser(new StringProvider(query));
//        parser.setErrorRecovery(true);
        try {
            return parser.Statement();
        } catch (ParseException err) {
            throw new StatementExecutionException("unable to parse query " + query, err);
        }

    }

    private ExecutionPlan plan(
            String defaultTableSpace, net.sf.jsqlparser.statement.Statement stmt,
            boolean scan, boolean returnValues, int maxRows
    ) {
        ExecutionPlan result;
        if (stmt instanceof CreateTable) {
            result = ExecutionPlan.simple(buildCreateTableStatement(defaultTableSpace, (CreateTable) stmt));
        } else if (stmt instanceof CreateIndex) {
            result = ExecutionPlan.simple(buildCreateIndexStatement(defaultTableSpace, (CreateIndex) stmt));
        } else if (stmt instanceof Execute) {
            result = ExecutionPlan.simple(buildExecuteStatement(defaultTableSpace, (Execute) stmt));
        } else if (stmt instanceof Alter) {
            result = ExecutionPlan.simple(buildAlterStatement(defaultTableSpace, (Alter) stmt));
        } else if (stmt instanceof Drop) {
            result = ExecutionPlan.simple(buildDropStatement(defaultTableSpace, (Drop) stmt));
        } else if (stmt instanceof Truncate) {
            result = ExecutionPlan.simple(buildTruncateStatement(defaultTableSpace, (Truncate) stmt));
        } else if (stmt instanceof Insert) {
            result = buildInsertStatement(defaultTableSpace, (Insert) stmt, returnValues);
        } else if (stmt instanceof Upsert) {
            result = buildUpsertStatement(defaultTableSpace, (Upsert) stmt, returnValues);
        } else if (stmt instanceof Update) {
            result = buildUpdateStatement(defaultTableSpace, (Update) stmt, returnValues);
        } else if (stmt instanceof Select) {
            result = buildSelectStatement(defaultTableSpace, maxRows, (Select) stmt, scan);
        } else if (stmt instanceof Delete) {
            result = buildDeleteStatement(defaultTableSpace, (Delete) stmt);
        } else {
            throw new StatementNotSupportedException("Not implemented " + stmt.getClass().getName());
        }
        return result;
    }

    private static boolean isCachable(net.sf.jsqlparser.statement.Statement stmt) {
        if (stmt instanceof Execute) {
            return false;
        } else if (stmt instanceof Alter) {
            return false;
        } else if (stmt instanceof Drop) {
            return false;
        } else {
            return !(stmt instanceof Truncate);
        }
    }

    private static String fixMySqlBackTicks(String s) {
        if (s == null || s.length() < 2) {
            return s;
        }
        if (s.startsWith("`") && s.endsWith("`")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private Statement buildCreateTableStatement(String defaultTableSpace, CreateTable s) throws StatementExecutionException {
        String tableSpace = fixMySqlBackTicks(s.getTable().getSchemaName());
        String tableName = fixMySqlBackTicks(s.getTable().getName());
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        if (s.getColumnDefinitions() == null) {
            throw new StatementExecutionException("A table must have at least 1 column");
        }
        final boolean isNotExsists = s.isIfNotExists();
        try {
            boolean foundPk = false;
            Table.Builder tablebuilder = Table.builder()
                    .uuid(UUID.randomUUID().toString())
                    .name(tableName)
                    .tablespace(tableSpace);
            Set<String> primaryKey = new HashSet<>();
            Set<String> simpleUniqueFields = new HashSet<>();

            if (s.getIndexes() != null) {
                for (Index index : s.getIndexes()) {
                    if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {
                        for (String n : index.getColumnsNames()) {
                            n = fixMySqlBackTicks(n.toLowerCase());
                            tablebuilder.primaryKey(n);
                            primaryKey.add(n);
                            foundPk = true;
                        }
                    }
                }
            }

            int position = 0;
            for (ColumnDefinition cf : s.getColumnDefinitions()) {
                String columnName = fixMySqlBackTicks(cf.getColumnName().toLowerCase());
                int type;
                String dataType = cf.getColDataType().getDataType();
                List<String> columnSpecs = decodeColumnSpecs(cf.getColumnSpecs());
                type = sqlDataTypeToColumnType(dataType,
                        cf.getColDataType().getArgumentsStringList(), columnSpecs);
                Bytes defaultValue = decodeDefaultValue(cf, type);

                if (!columnSpecs.isEmpty()) {
                    boolean auto_increment = decodeAutoIncrement(columnSpecs);
                    if (columnSpecs.contains("PRIMARY")) {
                        foundPk = true;
                        tablebuilder.primaryKey(columnName, auto_increment);
                    }
                    if (auto_increment && primaryKey.contains(columnName)) {
                        tablebuilder.primaryKey(columnName, auto_increment);
                    }
                    boolean isUnique = columnSpecs.contains("UNIQUE");
                    if (isUnique) {
                        simpleUniqueFields.add(columnName);
                    }
                }

                tablebuilder.column(columnName, type, position++, defaultValue);

            }

            if (!foundPk) {
                tablebuilder.column("_pk", ColumnTypes.LONG, position++, null);
                tablebuilder.primaryKey("_pk", true);
            }

            Table table = tablebuilder.build();
            List<herddb.model.Index> otherIndexes = new ArrayList<>();
            if (s.getIndexes() != null) {
                for (Index index : s.getIndexes()) {
                    if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {

                    } else if (index.getType().equalsIgnoreCase("INDEX")
                            || index.getType().equalsIgnoreCase("KEY")
                            || index.getType().equalsIgnoreCase("UNIQUE KEY")
                            ) {
                        String indexName = fixMySqlBackTicks(index.getName().toLowerCase());
                        String indexType = convertIndexType(null);
                        boolean unique = index.getType().equalsIgnoreCase("UNIQUE KEY");

                        herddb.model.Index.Builder builder = herddb.model.Index
                                .builder()
                                .onTable(table)
                                .name(indexName)
                                .unique(unique)
                                .type(indexType)
                                .uuid(UUID.randomUUID().toString());

                        for (String columnName : index.getColumnsNames()) {
                            columnName = fixMySqlBackTicks(columnName.toLowerCase());
                            Column column = table.getColumn(columnName);
                            if (column == null) {
                                throw new StatementExecutionException(
                                        "no such column " + columnName + " on table " + tableName + " in tablespace " + tableSpace);
                            }
                            builder.column(column.name, column.type);
                        }

                        otherIndexes.add(builder.build());
                    }
                }
            }
            for (String col : simpleUniqueFields) {
                herddb.model.Index.Builder builder = herddb.model.Index
                        .builder()
                        .onTable(table)
                        .name(table.name + "_unique_" + col)
                        .unique(true)
                        .type(herddb.model.Index.TYPE_BRIN)
                        .uuid(UUID.randomUUID().toString())
                        .column(col, table.getColumn(col).type);
                otherIndexes.add(builder.build());
            }

            CreateTableStatement statement = new CreateTableStatement(table, otherIndexes, isNotExsists);
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad table definition: " + err.getMessage(), err);
        }
    }

    private boolean decodeAutoIncrement(List<String> columnSpecs) {
        boolean auto_increment = columnSpecs.contains("AUTO_INCREMENT");
        return auto_increment;
    }

    private List<String> decodeColumnSpecs(List<String> columnSpecs) {
        if (columnSpecs == null || columnSpecs.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> columnSpecsDecoded = columnSpecs.stream().map(String::toUpperCase).collect(Collectors.toList());
        return columnSpecsDecoded;
    }

    private Statement buildCreateIndexStatement(String defaultTableSpace, CreateIndex s) throws StatementExecutionException {
        try {
            String tableSpace = s.getTable().getSchemaName();
            if (tableSpace == null) {
                tableSpace = defaultTableSpace;
            }
            tableSpace = fixMySqlBackTicks(tableSpace);
            String tableName = fixMySqlBackTicks(s.getTable().getName().toLowerCase());

            String indexName = fixMySqlBackTicks(s.getIndex().getName().toLowerCase());
            boolean unique = isUnique(s.getIndex().getType());
            String indexType = convertIndexType(s.getIndex().getType());

            herddb.model.Index.Builder builder = herddb.model.Index
                    .builder()
                    .name(indexName)
                    .uuid(UUID.randomUUID().toString())
                    .type(indexType)
                    .unique(unique)
                    .table(tableName)
                    .tablespace(tableSpace);

            AbstractTableManager tableDefinition = manager.getTableSpaceManager(tableSpace).getTableManager(tableName);
            if (tableDefinition == null) {
                throw new TableDoesNotExistException("no such table " + tableName + " in tablespace " + tableSpace);
            }
            for (String columnName : s.getIndex().getColumnsNames()) {
                columnName = fixMySqlBackTicks(columnName.toLowerCase());
                Column column = tableDefinition.getTable().getColumn(columnName);
                if (column == null) {
                    throw new StatementExecutionException(
                            "no such column " + columnName + " on table " + tableName + " in tablespace " + tableSpace);
                }
                builder.column(column.name, column.type);
            }

            CreateIndexStatement statement = new CreateIndexStatement(builder.build());
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad index definition: " + err.getMessage(), err);
        }
    }

    private static boolean isUnique(String indexType) throws StatementExecutionException {
        return indexType != null && indexType.equalsIgnoreCase("UNIQUE");
    }
    private static String convertIndexType(String indexType) throws StatementExecutionException {
        if (indexType == null) {
            indexType = herddb.model.Index.TYPE_BRIN;
        } else {
            indexType = indexType.toLowerCase();
        }
        if (indexType.equals("unique")) {
            return herddb.model.Index.TYPE_BRIN;
        }
        switch (indexType) {
            case herddb.model.Index.TYPE_HASH:
            case herddb.model.Index.TYPE_BRIN:
                break;
            default:
                throw new StatementExecutionException("Invalid index type " + indexType);
        }
        return indexType;
    }

    public static int sqlDataTypeToColumnType(ColDataType dataType) throws StatementExecutionException {
        return sqlDataTypeToColumnType(dataType.getDataType(), dataType.getArgumentsStringList(), Collections.emptyList());
    }

    private static int sqlDataTypeToColumnType(String dataType,
            List<String> arguments, List<String> columnSpecs) throws StatementExecutionException {
        int type;
        switch (dataType.toLowerCase()) {
            case "string":
            case "varchar":
            case "nvarchar":
            case "nvarchar2":
            case "nclob":
            case "text":
            case "longtext":
            case "clob":
            case "char":
                type = ColumnTypes.STRING;
                break;
            case "long":
            case "bigint":
                type = ColumnTypes.LONG;
                break;
            case "int":
            case "integer":
            case "tinyint":
            case "smallint":
                type = ColumnTypes.INTEGER;
                break;
            case "bytea":
            case "blob":
            case "image":
                type = ColumnTypes.BYTEARRAY;
                break;
            case "timestamp":
            case "timestamptz":
            case "timestamp with time zone":
            case "datetime":
            case "date":
            case "time":
            case "time with time zone":
                type = ColumnTypes.TIMESTAMP;
                break;
            case "boolean":
            case "bool":
            case "bit":
                type = ColumnTypes.BOOLEAN;
                break;
            case "double":
            case "float":
            case "real":
                type = ColumnTypes.DOUBLE;
                break;
            case "numeric":
            case "decimal":
                if (arguments == null || arguments.isEmpty()) {
                    type = ColumnTypes.DOUBLE;
                } else if (arguments.size() == 2) {
                    int precision = Integer.parseInt(arguments.get(0));
                    int scale = Integer.parseInt(arguments.get(1));
                    if (scale == 0) {
                        if (precision > 0) {
                            type = ColumnTypes.INTEGER;
                        } else {
                            type = ColumnTypes.LONG;
                        }
                    } else {
                        type = ColumnTypes.DOUBLE;
                    }
                } else {
                    throw new StatementExecutionException("bad type " + dataType + " with arguments " + arguments);
                }

                break;

            default:
                throw new StatementExecutionException("bad type " + dataType);
        }
        if (String.join("_", columnSpecs).contains("NOT_NULL")) {
             type = ColumnTypes.getNonNullTypeForPrimitiveType(type);
        }
        return type;
    }

    public static Object resolveValue(Expression expression, boolean allowColumn) throws StatementExecutionException {
        if (expression instanceof JdbcParameter) {
            throw new StatementExecutionException("jdbcparameter expression not usable in this query");
        } else if (allowColumn && expression instanceof net.sf.jsqlparser.schema.Column) {
            // this is only for supporting back ticks in DDL
            return fixMySqlBackTicks(((net.sf.jsqlparser.schema.Column) expression).getColumnName());
        } else if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        } else if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        } else if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue();
        } else if (expression instanceof NullValue) {
            return null;
        } else if (expression instanceof TimestampValue) {
            return ((TimestampValue) expression).getValue();
        } else if (expression instanceof SignedExpression) {
            SignedExpression se = (SignedExpression) expression;
            switch (se.getSign()) {
                case '+': {
                    return resolveValue(se.getExpression(), allowColumn);
                }
                case '-': {
                    Object value = resolveValue(se.getExpression(), allowColumn);
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof Integer) {
                        return -1L * ((Integer) value);
                    } else if (value instanceof Long) {
                        return -1L * ((Long) value);
                    } else {
                        throw new StatementExecutionException(
                                "unsupported value type " + expression.getClass() + " with sign " + se.getSign() + " on value " + value + " of type " + value.
                                getClass());
                    }
                }
                default:
                    throw new StatementExecutionException(
                            "unsupported value type " + expression.getClass() + " with sign " + se.getSign());
            }

        } else {
            throw new StatementExecutionException("unsupported value type " + expression.getClass());
        }
    }

    private static final Logger LOG = Logger.getLogger(DDLSQLPlanner.class.getName());

    private Statement buildExecuteStatement(String defaultTableSpace, Execute execute) throws StatementExecutionException {
        switch (execute.getName().toUpperCase()) {
            case "BEGINTRANSACTION": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException(
                            "BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException(
                            "BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName)");
                }
                return new BeginTransactionStatement(tableSpaceName.toString());
            }
            case "COMMITTRANSACTION": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException(
                            "COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException(
                            "COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1), true);
                if (transactionId == null) {
                    throw new StatementExecutionException(
                            "COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                try {
                    return new CommitTransactionStatement(tableSpaceName.toString(), Long.parseLong(transactionId.
                            toString()));
                } catch (NumberFormatException err) {
                    throw new StatementExecutionException(
                            "COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }

            }
            case "ROLLBACKTRANSACTION": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException(
                            "COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException(
                            "COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1), true);
                if (transactionId == null) {
                    throw new StatementExecutionException(
                            "COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                try {
                    return new RollbackTransactionStatement(tableSpaceName.toString(), Long.parseLong(transactionId.
                            toString()));
                } catch (NumberFormatException err) {
                    throw new StatementExecutionException(
                            "COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
            }
            case "CREATETABLESPACE": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() < 1) {
                    throw new StatementExecutionException(
                            "CREATETABLESPACE syntax (EXECUTE CREATETABLESPACE tableSpaceName ['leader:LEADERID'],['wait:TIMEOUT'] )");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                String leader = null;
                Set<String> replica = new HashSet<>();
                int expectedreplicacount = this.manager.getServerConfiguration().getInt(ServerConfiguration.PROPERTY_DEFAULT_REPLICA_COUNT, ServerConfiguration.PROPERTY_DEFAULT_REPLICA_COUNT_DEFAULT);
                long maxleaderinactivitytime = 0;
                int wait = 0;
                for (int i = 1; i < execute.getExprList().getExpressions().size(); i++) {
                    String property = (String) resolveValue(execute.getExprList().getExpressions().get(i), true);
                    int colon = property.indexOf(':');
                    if (colon <= 0) {
                        throw new StatementExecutionException(
                                "bad property " + property + " in " + execute + " statement");
                    }
                    String pName = property.substring(0, colon);
                    String value = property.substring(colon + 1);
                    switch (pName.toLowerCase()) {
                        case "leader":
                            leader = value;
                            break;
                        case "replica":
                            replica = Arrays.asList(value.split(",")).stream().map(String::trim).filter(s -> !s.
                                    isEmpty()).collect(Collectors.toSet());
                            break;
                        case "wait":
                            wait = Integer.parseInt(value);
                            break;
                        case "expectedreplicacount":
                            try {
                                expectedreplicacount = Integer.parseInt(value.trim());
                                if (expectedreplicacount <= 0) {
                                    throw new StatementExecutionException(
                                            "invalid expectedreplicacount " + value + " must be positive");
                                }
                            } catch (NumberFormatException err) {
                                throw new StatementExecutionException(
                                        "invalid expectedreplicacount " + value + ": " + err);
                            }
                            break;
                        case "maxleaderinactivitytime":
                            try {
                                maxleaderinactivitytime = Long.parseLong(value.trim());
                                if (maxleaderinactivitytime < 0) {
                                    throw new StatementExecutionException(
                                            "invalid maxleaderinactivitytime " + value + " must be positive or zero");
                                }
                            } catch (NumberFormatException err) {
                                throw new StatementExecutionException(
                                        "invalid maxleaderinactivitytime " + value + ": " + err);
                            }
                            break;
                        default:
                            throw new StatementExecutionException("bad property " + pName);
                    }
                }
                if (leader == null) {
                    leader = this.manager.getNodeId();
                }
                if (replica.isEmpty()) {
                    replica.add(leader);
                }
                return new CreateTableSpaceStatement(tableSpaceName + "", replica, leader, expectedreplicacount, wait,
                        maxleaderinactivitytime);
            }
            case "ALTERTABLESPACE": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() < 2) {
                    throw new StatementExecutionException(
                            "ALTERTABLESPACE syntax (EXECUTE ALTERTABLESPACE tableSpaceName,'property:value','property2:value2')");
                }
                String tableSpaceName = (String) resolveValue(execute.getExprList().getExpressions().get(0), true);
                try {
                    TableSpace tableSpace = manager.getMetadataStorageManager().describeTableSpace(tableSpaceName + "");
                    if (tableSpace == null) {
                        throw new TableSpaceDoesNotExistException(tableSpaceName);
                    }
                    Set<String> replica = tableSpace.replicas;
                    String leader = tableSpace.leaderId;
                    int expectedreplicacount = tableSpace.expectedReplicaCount;
                    long maxleaderinactivitytime = tableSpace.maxLeaderInactivityTime;
                    for (int i = 1; i < execute.getExprList().getExpressions().size(); i++) {
                        String property = (String) resolveValue(execute.getExprList().getExpressions().get(i), true);
                        int colon = property.indexOf(':');
                        if (colon <= 0) {
                            throw new StatementExecutionException(
                                    "bad property " + property + " in " + execute + " statement");
                        }
                        String pName = property.substring(0, colon);
                        String value = property.substring(colon + 1);
                        switch (pName.toLowerCase()) {
                            case "leader":
                                leader = value;
                                break;
                            case "replica":
                                replica = Arrays.asList(value.split(",")).stream().map(String::trim).filter(s -> !s.
                                        isEmpty()).collect(Collectors.toSet());
                                break;
                            case "expectedreplicacount":
                                try {
                                    expectedreplicacount = Integer.parseInt(value.trim());
                                    if (expectedreplicacount <= 0) {
                                        throw new StatementExecutionException(
                                                "invalid expectedreplicacount " + value + " must be positive");
                                    }
                                } catch (NumberFormatException err) {
                                    throw new StatementExecutionException(
                                            "invalid expectedreplicacount " + value + ": " + err);
                                }
                                break;
                            case "maxleaderinactivitytime":
                                try {
                                    maxleaderinactivitytime = Long.parseLong(value.trim());
                                    if (maxleaderinactivitytime < 0) {
                                        throw new StatementExecutionException(
                                                "invalid maxleaderinactivitytime " + value + " must be positive or zero");
                                    }
                                } catch (NumberFormatException err) {
                                    throw new StatementExecutionException(
                                            "invalid maxleaderinactivitytime " + value + ": " + err);
                                }
                                break;
                            default:
                                throw new StatementExecutionException("bad property " + pName);
                        }
                    }
                    return new AlterTableSpaceStatement(tableSpaceName + "", replica, leader, expectedreplicacount,
                            maxleaderinactivitytime);
                } catch (MetadataStorageManagerException err) {
                    throw new StatementExecutionException(err);
                }
            }
            case "DROPTABLESPACE": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException(
                            "DROPTABLESPACE syntax (EXECUTE DROPTABLESPACE tableSpaceName)");
                }
                String tableSpaceName = (String) resolveValue(execute.getExprList().getExpressions().get(0), true);
                try {
                    TableSpace tableSpace = manager.getMetadataStorageManager().describeTableSpace(tableSpaceName + "");
                    if (tableSpace == null) {
                        throw new TableSpaceDoesNotExistException(tableSpaceName);
                    }
                    return new DropTableSpaceStatement(tableSpaceName + "");
                } catch (MetadataStorageManagerException err) {
                    throw new StatementExecutionException(err);
                }
            }
            case "RENAMETABLE": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 3) {
                    throw new StatementExecutionException(
                            "RENAMETABLE syntax (EXECUTE RENAMETABLE 'tableSpaceName','tablename','nametablename')");
                }
                String tableSpaceName = (String) resolveValue(execute.getExprList().getExpressions().get(0), true);
                String oldTableName = (String) resolveValue(execute.getExprList().getExpressions().get(1), true);
                String newTableName = (String) resolveValue(execute.getExprList().getExpressions().get(2), true);
                try {
                    TableSpace tableSpace = manager.getMetadataStorageManager().describeTableSpace(tableSpaceName + "");
                    if (tableSpace == null) {
                        throw new TableSpaceDoesNotExistException(tableSpaceName);
                    }
                    return new AlterTableStatement(Collections.emptyList(),
                            Collections.emptyList(), Collections.emptyList(),
                            null, oldTableName.toLowerCase(), tableSpaceName, newTableName.toLowerCase());
                } catch (MetadataStorageManagerException err) {
                    throw new StatementExecutionException(err);
                }
            }
            default:
                throw new StatementExecutionException("Unsupported command " + execute.getName());
        }
    }

    public Statement queryConsistencyCheckStatement(String defaultTablespace, String query, List<Object> parameters) {
        if (query.startsWith(TABLE_CONSISTENCY_COMMAND)) {
            query = query.substring(query.substring(0, 21).length());
            String tableSpace = defaultTablespace;
            String tableName;

            if (query.contains(".")) {
                String[] tokens = query.split("\\.");
                tableSpace = tokens[0].trim().replaceAll("\'", "");
                tableName = tokens[1].trim().replaceAll("\'", "");
            } else {
                tableName = query.trim();
            }
            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
            if (tableSpaceManager == null) {
                throw new TableSpaceDoesNotExistException(String.format("Tablespace %s does not exist.", tableSpace));
            }
            AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);

            if (tableManager == null || tableManager.getCreatedInTransaction() > 0) {
                throw new TableDoesNotExistException(String.format("Table %s does not exist.", tableName));
            }

            return new TableConsistencyCheckStatement(tableName, tableSpace);
        } else {
            throw new StatementExecutionException(String.format("Incorrect Syntax for tableconsistencycheck"));
        }

    }

    public Statement queryConsistencyCheckStatement(String query) {
        if (query.startsWith(TABLESPACE_CONSISTENCY_COMMAND)) {
            String tableSpace = query.substring(query.substring(0, 26).length()).replace("\'", "");
            TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace.trim());

            if (tableSpaceManager == null) {
                throw new TableSpaceDoesNotExistException(String.format("Tablespace %s does not exist.", tableSpace));
            }
            return new TableSpaceConsistencyCheckStatement(tableSpace.trim());
        } else {
            throw new StatementExecutionException(String.format("Incorrect Syntax for tablespaceconsistencycheck"));
        }
    }

    private Statement buildAlterStatement(String defaultTableSpace, Alter alter) throws StatementExecutionException {
        if (alter.getTable() == null) {
            throw new StatementExecutionException("missing table name");
        }
        String tableSpace = alter.getTable().getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        List<Column> addColumns = new ArrayList<>();
        List<Column> modifyColumns = new ArrayList<>();
        List<String> dropColumns = new ArrayList<>();
        String tableName = fixMySqlBackTicks(alter.getTable().getName().toLowerCase());
        if (alter.getAlterExpressions() == null || alter.getAlterExpressions().size() != 1) {
            throw new StatementExecutionException("supported multi-alter operation '" + alter + "'");
        }
        AlterExpression alterExpression = alter.getAlterExpressions().get(0);
        AlterOperation operation = alterExpression.getOperation();
        Boolean changeAutoIncrement = null;
        switch (operation) {
            case ADD: {
                List<AlterExpression.ColumnDataType> cols = alterExpression.getColDataTypeList();
                for (AlterExpression.ColumnDataType cl : cols) {
                    List<String> columnSpecs = decodeColumnSpecs(cl.getColumnSpecs());
                    int type = sqlDataTypeToColumnType(
                            cl.getColDataType().getDataType(),
                            cl.getColDataType().getArgumentsStringList(),
                            columnSpecs
                    );
                    Column newColumn = Column.column(fixMySqlBackTicks(cl.getColumnName()), type, decodeDefaultValue(cl, type));
                    addColumns.add(newColumn);
                }
            }
            break;
            case DROP:
                dropColumns.add(fixMySqlBackTicks(alterExpression.getColumnName()));
                break;
            case MODIFY: {
                TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
                if (tableSpaceManager == null) {
                    throw new StatementExecutionException("bad tablespace '" + tableSpace + "'");
                }
                AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
                if (tableManager == null) {
                    throw new StatementExecutionException(
                            "bad table " + tableName + " in tablespace '" + tableSpace + "'");
                }
                Table table = tableManager.getTable();
                List<AlterExpression.ColumnDataType> cols = alterExpression.getColDataTypeList();
                for (AlterExpression.ColumnDataType cl : cols) {
                    String columnName = fixMySqlBackTicks(cl.getColumnName().toLowerCase());
                    Column oldColumn = table.getColumn(columnName);
                    if (oldColumn == null) {
                        throw new StatementExecutionException(
                                "bad column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                    }
                    Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(tableName);
                    if (indexes != null) {
                        for (AbstractIndexManager am : indexes.values()) {
                            for (String indexedColumn : am.getColumnNames()) {
                                indexedColumn = fixMySqlBackTicks(indexedColumn);
                                if (indexedColumn.equalsIgnoreCase(oldColumn.name)) {
                                    throw new StatementExecutionException(
                                            "cannot alter indexed " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "',"
                                            + "index name is " + am.getIndexName());
                                }
                            }
                        }
                    }
                    List<String> columnSpecs = decodeColumnSpecs(cl.getColumnSpecs());
                    int newType = sqlDataTypeToColumnType(
                            cl.getColDataType().getDataType(),
                            cl.getColDataType().getArgumentsStringList(),
                            columnSpecs
                    );

                    if (oldColumn.type != newType) {
                        if (ColumnTypes.isNotNullToNullConversion(oldColumn.type, newType)) {
                            // allow change from "STRING NOT NULL" to "STRING NULL"
                        } else if (ColumnTypes.isNullToNotNullConversion(oldColumn.type, newType)) {
                            // allow change from "STRING NULL" to "STRING NOT NULL"
                            // it will require a check on table at execution time
                        } else {
                            throw new StatementExecutionException("cannot change datatype to " + ColumnTypes.typeToString(newType)
                                    + " for column " + columnName + " (" + ColumnTypes.typeToString(oldColumn.type) + ") in table " + tableName + " in tablespace '" + tableSpace + "'");
                        }
                    }
                    if (table.isPrimaryKeyColumn(columnName)) {
                        boolean new_auto_increment = decodeAutoIncrement(columnSpecs);
                        if (new_auto_increment && table.primaryKey.length > 1) {
                            throw new StatementExecutionException("cannot add auto_increment flag to " + cl.
                                    getColDataType().getDataType()
                                    + " for column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                        }
                        if (table.auto_increment != new_auto_increment) {
                            changeAutoIncrement = new_auto_increment;
                        }
                    }
                    Bytes newDefault = oldColumn.defaultValue;
                    if (containsDefaultClause(cl)) {
                        newDefault = decodeDefaultValue(cl, newType);
                    }
                    Column newColumnDef = Column.column(columnName, newType, oldColumn.serialPosition, newDefault);
                    modifyColumns.add(newColumnDef);
                }
            }
            break;
            case CHANGE: {
                TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
                if (tableSpaceManager == null) {
                    throw new StatementExecutionException("bad tablespace '" + tableSpace + "'");
                }
                AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
                if (tableManager == null) {
                    throw new StatementExecutionException(
                            "bad table " + tableName + " in tablespace '" + tableSpace + "'");
                }
                Table table = tableManager.getTable();
                String columnName = alterExpression.getColOldName();
                List<AlterExpression.ColumnDataType> cols = alterExpression.getColDataTypeList();
                if (cols.size() != 1) {
                    throw new StatementExecutionException(
                            "bad CHANGE column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                }
                AlterExpression.ColumnDataType cl = cols.get(0);
                Column oldColumn = table.getColumn(columnName);
                if (oldColumn == null) {
                    throw new StatementExecutionException(
                            "bad column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                }
                Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(tableName);
                if (indexes != null) {
                    for (AbstractIndexManager am : indexes.values()) {
                        for (String indexedColumn : am.getColumnNames()) {
                            indexedColumn = fixMySqlBackTicks(indexedColumn);
                            if (indexedColumn.equalsIgnoreCase(oldColumn.name)) {
                                throw new StatementExecutionException(
                                        "cannot alter indexed " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "',"
                                        + "index name is " + am.getIndexName());
                            }
                        }
                    }
                }

                List<String> columnSpecs = decodeColumnSpecs(cl.getColumnSpecs());
                int newType = sqlDataTypeToColumnType(
                        cl.getColDataType().getDataType(),
                        cl.getColDataType().getArgumentsStringList(),
                        columnSpecs
                );

                if (oldColumn.type != newType) {
                    throw new StatementExecutionException("cannot change datatype to " + ColumnTypes.typeToString(newType)
                            + " for column " + columnName + " (" + ColumnTypes.typeToString(oldColumn.type) + ") in table " + tableName + " in tablespace '" + tableSpace + "'");
                }
                if (table.isPrimaryKeyColumn(columnName)) {
                    boolean new_auto_increment = decodeAutoIncrement(columnSpecs);
                    if (new_auto_increment && table.primaryKey.length > 1) {
                        throw new StatementExecutionException(
                                "cannot add auto_increment flag to " + cl.getColDataType().getDataType()
                                + " for column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                    }
                    if (table.auto_increment != new_auto_increment) {
                        changeAutoIncrement = new_auto_increment;
                    }
                }
                String renameTo = fixMySqlBackTicks(cl.getColumnName().toLowerCase());
                if (renameTo != null) {
                    columnName = renameTo;
                }
                Column newColumnDef = Column.column(columnName, newType, oldColumn.serialPosition, oldColumn.defaultValue);
                modifyColumns.add(newColumnDef);
            }

            break;
            default:
                throw new StatementExecutionException("supported alter operation '" + alter + "'");
        }
        return new AlterTableStatement(addColumns, modifyColumns, dropColumns,
                changeAutoIncrement, tableName.toLowerCase(), tableSpace, null);
    }

    private Statement buildDropStatement(String defaultTableSpace, Drop drop) throws StatementExecutionException {
        if (drop.getType().equalsIgnoreCase("table")) {
            if (drop.getName() == null) {
                throw new StatementExecutionException("missing table name");
            }

            String tableSpace = fixMySqlBackTicks(drop.getName().getSchemaName());
            if (tableSpace == null) {
                tableSpace = defaultTableSpace;
            }
            String tableName = fixMySqlBackTicks(drop.getName().getName());
            return new DropTableStatement(tableSpace, tableName, drop.isIfExists());
        }
        if (drop.getType().equalsIgnoreCase("index")) {
            if (drop.getName() == null) {
                throw new StatementExecutionException("missing index name");
            }
            String tableSpace = fixMySqlBackTicks(drop.getName().getSchemaName());
            if (tableSpace == null) {
                tableSpace = defaultTableSpace;
            }
            String indexName = fixMySqlBackTicks(drop.getName().getName());
            return new DropIndexStatement(tableSpace, indexName, drop.isIfExists());
        }
        throw new StatementExecutionException(
                "only DROP TABLE and TABLESPACE is supported, drop type=" + drop.getType() + " is not implemented");
    }

    private Statement buildTruncateStatement(String defaultTableSpace, Truncate truncate) throws StatementExecutionException {

        if (truncate.getTable() == null) {
            throw new StatementExecutionException("missing table name");
        }

        String tableSpace = truncate.getTable().getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        String tableName = fixMySqlBackTicks(truncate.getTable().getName().toLowerCase());
        return new TruncateTableStatement(tableSpace, tableName);
    }

    private ExecutionPlan buildSelectStatement(String defaultTableSpace, int maxRows, Select select, boolean forceScan) throws StatementExecutionException {
        checkSupported(select.getWithItemsList() == null);
        SelectBody selectBody = select.getSelectBody();
        checkSupported(selectBody instanceof PlainSelect);
        PlainSelect plainSelect = (PlainSelect) selectBody;

        checkSupported(!plainSelect.getMySqlHintStraightJoin());
        checkSupported(!plainSelect.getMySqlSqlCalcFoundRows());
        checkSupported(!plainSelect.getMySqlSqlNoCache());
        checkSupported(plainSelect.getDistinct() == null);
        checkSupported(plainSelect.getFetch() == null);
        checkSupported(plainSelect.getFirst() == null);
        checkSupported(plainSelect.getForUpdateTable() == null);
        checkSupported(plainSelect.getForXmlPath() == null);
        checkSupported(plainSelect.getHaving() == null);
        checkSupported(plainSelect.getIntoTables() == null);
        checkSupported(plainSelect.getJoins() == null);
        checkSupported(plainSelect.getOffset() == null);
        checkSupported(plainSelect.getOptimizeFor() == null);
        checkSupported(plainSelect.getOracleHierarchical() == null);
        checkSupported(plainSelect.getOracleHint() == null);
        checkSupported(plainSelect.getSkip() == null);
        checkSupported(plainSelect.getWait() == null);
        checkSupported(plainSelect.getKsqlWindow() == null);

        FromItem fromItem = plainSelect.getFromItem();
        checkSupported(fromItem instanceof net.sf.jsqlparser.schema.Table);


        net.sf.jsqlparser.schema.Table table = (net.sf.jsqlparser.schema.Table) fromItem;
        String tableSpace = table.getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = this.manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no tablespace " + tableSpace + " here");
        }
        String tableName = fixMySqlBackTicks(table.getName().toLowerCase());
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new TableDoesNotExistException("no table " + tableName + " here for " + tableSpace);
        }
        Table tableImpl = tableManager.getTable();

        List<SelectItem> selectItems = plainSelect.getSelectItems();
        checkSupported(!selectItems.isEmpty());
        Predicate predicate = new FullTableScanPredicate();
        TupleComparator comparator = null;
        ScanLimits limits = null;
        ScanStatement scan;
        Projection projection;
        List<SelectExpressionItem> selectedFields = new ArrayList<>(selectItems.size());
        boolean containsAggregatedFunctions = false;
        if (selectItems.size() == 1 && selectItems.get(0) instanceof AllColumns) {
            projection = Projection.IDENTITY(tableImpl.columnNames, tableImpl.columns);
        } else {
            checkSupported(!selectItems.isEmpty());
            for (SelectItem item : selectItems) {
                if (item instanceof SelectExpressionItem) {
                    SelectExpressionItem selectExpressionItem = (SelectExpressionItem) item;
                    selectedFields.add(selectExpressionItem);
                    if (SQLParserExpressionCompiler.detectAggregatedFunction(selectExpressionItem.getExpression()) != null) {
                        containsAggregatedFunctions = true;
                    }
                } else {
                    checkSupported(false);
                }
            }
            if (!containsAggregatedFunctions) {
                // building the projection
                // we have the current phisical schema, we can create references by position (as Calcite does)
                // in order to not need accessing columns by name and also making it easier to support
                // ZeroCopyProjections
                projection = buildProjection(selectedFields, true, tableImpl.getColumns());
            } else {
                // start by full table scan, the AggregateOp operator will create the final projection
                projection = Projection.IDENTITY(tableImpl.columnNames, tableImpl.columns);
            }
        }
        if (plainSelect.getWhere() != null) {
            CompiledSQLExpression whereExpression = SQLParserExpressionCompiler.compileExpression(plainSelect.getWhere(), tableImpl.getColumns());
            SQLRecordPredicate sqlWhere = new SQLRecordPredicate(tableImpl, null, whereExpression);
            IndexUtils.discoverIndexOperations(tableSpace, whereExpression, tableImpl, sqlWhere, select, tableSpaceManager);
            predicate = sqlWhere;
        }

        // start with a TableScan + filters
        scan = new ScanStatement(tableSpace, tableName, Projection.IDENTITY(tableImpl.columnNames, tableImpl.columns), predicate, comparator, limits);
        scan.setTableDef(tableImpl);
        PlannerOp op = new BindableTableScanOp(scan);

        // add aggregations
        if (containsAggregatedFunctions) {
            op = planAggregate(selectedFields, op.getOutputSchema(), (BindableTableScanOp) op, plainSelect.getGroupBy());
        }

        // TODO: add having

        // add order by
        if (plainSelect.getOrderByElements() != null) {
            op = planSort(op, op.getOutputSchema(), plainSelect.getOrderByElements());
        }

        // add limit
        if (plainSelect.getLimit() != null) {
            // cannot mix LIMIT and TOP
            checkSupported(plainSelect.getTop() == null);
            Limit limit = plainSelect.getLimit();
            CompiledSQLExpression offset;
            if (limit.getOffset() != null) {
                offset = SQLParserExpressionCompiler.compileExpression(limit.getOffset(), tableImpl.getColumns());
            } else {
                offset = new ConstantExpression(0);
            }
            CompiledSQLExpression rowCount = null;
            if (limit.getRowCount() != null) {
                rowCount = SQLParserExpressionCompiler.compileExpression(limit.getRowCount(), tableImpl.getColumns());
            }
            op = new LimitOp(op, rowCount, offset);
        }

        if (plainSelect.getTop() != null) {
            // cannot mix LIMIT and TOP
            checkSupported(plainSelect.getLimit() == null);
            Top limit = plainSelect.getTop();
            CompiledSQLExpression rowCount = null;
            if (limit.getExpression() != null) {
                rowCount = SQLParserExpressionCompiler.compileExpression(limit.getExpression(), tableImpl.getColumns());
            }
            op = new LimitOp(op, rowCount, new ConstantExpression(0));
        }

        if (!containsAggregatedFunctions) {
            // add projection
            op = new ProjectOp(projection, op);
        }

        // Simplify Scan to Get
        if (!forceScan) {
            ScanStatement scanStatement = op.unwrap(ScanStatement.class);
            if (scanStatement != null) {
                Table tableDef = scanStatement.getTableDef();
                CompiledSQLExpression where = scanStatement.getPredicate().unwrap(CompiledSQLExpression.class);
                SQLRecordKeyFunction keyFunction = IndexUtils.findIndexAccess(where, tableDef.getPrimaryKey(),
                        tableDef, "=", tableDef);
                if (keyFunction == null || !keyFunction.isFullPrimaryKey()) {
                    throw new StatementExecutionException("unsupported GET not on PK (" + keyFunction + ")");
                }
                GetStatement get = new GetStatement(scanStatement.getTableSpace(),
                        scanStatement.getTable(), keyFunction, scanStatement.getPredicate(), true);
                return ExecutionPlan.simple(get);
            }
        }

        // additional maxrows from JDBC PreparedStatement
        if (maxRows > 0) {
            op = new LimitOp(op,
                    new ConstantExpression(maxRows), new ConstantExpression(0))
                    .optimize();
        }

        return optimizePlan(op);
    }

    private PlannerOp planAggregate(List<SelectExpressionItem> fieldList, Column[] inputSchema, BindableTableScanOp input,
                                                                                                GroupByElement groupBy) {


        List<Column> outputSchemaKeysInGroupBy = new ArrayList<>();
        List<String> fieldnamesKeysInGroupBy = new ArrayList<>();

        List<Column> outputSchemaAggregationResults = new ArrayList<>();
        List<String> fieldnamesAggregationResults = new ArrayList<>();
        List<Integer> projectionAggregationResults = new ArrayList<>();
        List<List<Integer>> argLists = new ArrayList<>();
        List<String> aggtypes = new ArrayList<>();

        List<Column> originalOutputSchema = new ArrayList<>();
        List<String> originalFieldNames = new ArrayList<>();

        int k = 0;
        for (SelectExpressionItem sel : fieldList) {
            Alias alias = sel.getAlias();
            String fieldName = null;
            if (alias != null) {
                checkSupported(alias.getAliasColumns() == null);
                fieldName = fixMySqlBackTicks(alias.getName().toLowerCase());
            }
            Expression exp = sel.getExpression();

            Function fn = SQLParserExpressionCompiler.detectAggregatedFunction(exp);
            if (fn != null) {
                int type = SQLParserExpressionCompiler.getAggregateFunctionType(exp, inputSchema);
                Column additionalColumn = SQLParserExpressionCompiler.getAggregateFunctionArgument(fn, inputSchema);
                aggtypes.add(fixMySqlBackTicks(fn.getName().toLowerCase()));
                if (additionalColumn != null) { // SELECT min(col) -> we have to add "col" to the scan
                    IntHolder pos = new IntHolder();
                    findColumnInSchema(fixMySqlBackTicks(additionalColumn.getName()), input.getStatement().getTableDef().getColumns(), pos);
                    checkSupported(pos.value >= 0);
                    argLists.add(Collections.singletonList(pos.value));
                } else {
                    argLists.add(Collections.emptyList());
                }
                if (fieldName == null) {
                    fieldName = "agg" + k;
                }
                Column col = Column.column(fieldName, type);
                outputSchemaAggregationResults.add(col);
                originalFieldNames.add(fieldName);
                originalOutputSchema.add(col);
                fieldnamesAggregationResults.add(fieldName);
                projectionAggregationResults.add(k);
            } else if (exp instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column colRef = (net.sf.jsqlparser.schema.Column) exp;
                Column colInSchema = findColumnInSchema(fixMySqlBackTicks(colRef.getColumnName()), inputSchema, new IntHolder());
                checkSupported(colInSchema != null);
                if (fieldName == null) {
                    fieldName = colInSchema.getName();
                }
                Column col = Column.column(fieldName, colInSchema.type);
                originalFieldNames.add(fieldName);
                originalOutputSchema.add(col);
            } else {
                checkSupported(false);
            }
            k++;
        }
        List<Integer> groupedFieldsIndexes = new ArrayList<>(); // GROUP BY
        List<Integer> projectionForGroupByFields = new ArrayList<>();
        if (groupBy != null) {
            checkSupported(groupBy.getGroupingSets() == null || groupBy.getGroupingSets().isEmpty());
            int posInGroupBy = 0;
            for (Expression exp : groupBy.getGroupByExpressions()) {
                if (exp instanceof net.sf.jsqlparser.schema.Column) {
                    net.sf.jsqlparser.schema.Column colRef = (net.sf.jsqlparser.schema.Column) exp;
                    IntHolder pos = new IntHolder();
                    Column colInSchema = findColumnInSchema(fixMySqlBackTicks(colRef.getColumnName()), inputSchema, pos);
                    checkSupported(colInSchema != null);
                    groupedFieldsIndexes.add(pos.value);
                    fieldnamesKeysInGroupBy.add(fixMySqlBackTicks(colRef.getColumnName()));
                    outputSchemaKeysInGroupBy.add(colInSchema);
                    projectionForGroupByFields.add(posInGroupBy);
                } else {
                    checkSupported(false);
                }
                posInGroupBy++;
            }
        }

        PlannerOp op;
        if (!groupedFieldsIndexes.isEmpty()) {
            // this is tricky,
            // the AggregateOp always create a result in the form of
            // FIELD1,FIELD2......AGG1,AGG2
            // Basically it puts all of the results of the aggregation to the right

            // So we have to add a projection that makes the resultset appear
            // as it is defined in the SELECT clause

            List<Column> outputSchema = new ArrayList<>();
            outputSchema.addAll(outputSchemaKeysInGroupBy);
            outputSchema.addAll(outputSchemaAggregationResults);
            Column[] outputSchemaArray = outputSchema.toArray(new Column[0]);

            List<String> aggreateFieldNames = new ArrayList<>();
            aggreateFieldNames.addAll(fieldnamesKeysInGroupBy);
            aggreateFieldNames.addAll(fieldnamesAggregationResults);

            op = new AggregateOp(input,
                    aggreateFieldNames.toArray(new String[0]),
                    outputSchemaArray,
                    aggtypes.toArray(new String[0]),
                    argLists,
                    groupedFieldsIndexes);
            String[] reodereded = originalFieldNames.toArray(new String[0]);
            int[] projections = new int[originalFieldNames.size()];
            int i = 0;
            for (int pos : projectionForGroupByFields) {
                projections[pos] = i++;
            }
            for (int pos : projectionAggregationResults) {
                projections[pos] = i++;
            }
            ProjectOp.ZeroCopyProjection projection = new ProjectOp.ZeroCopyProjection(reodereded,
                    originalOutputSchema.toArray(new Column[0]),
                    projections);
            op = new ProjectOp(projection, op);
        } else {
            // no "GROUP BY", so no need for an additional projection
            // this is the SELECT COUNT(*) FROM TABLE case
            op = new AggregateOp(input,
                    originalFieldNames.toArray(new String[0]),
                    originalOutputSchema.toArray(new Column[0]),
                    aggtypes.toArray(new String[0]),
                    argLists,
                    groupedFieldsIndexes);
        }
        return op;
    }

    private static ExecutionPlan optimizePlan(PlannerOp op) {
        op = op.optimize();
        return ExecutionPlan.simple(new SQLPlannedOperationStatement(op), op);
    }

    private PlannerOp planSort(PlannerOp input, Column[] columns, List<OrderByElement> fieldCollations) {
        boolean[] directions = new boolean[fieldCollations.size()];
        boolean[] nullLastdirections = new boolean[fieldCollations.size()];
        int[] fields = new int[fieldCollations.size()];
        int i = 0;
        for (OrderByElement col : fieldCollations) {
            OrderByElement.NullOrdering nullDirection = col.getNullOrdering();
            CompiledSQLExpression columnPos = SQLParserExpressionCompiler.compileExpression(col.getExpression(), columns);
            checkSupported(columnPos instanceof AccessCurrentRowExpression);
            AccessCurrentRowExpression pos = (AccessCurrentRowExpression) columnPos;
            int index = pos.getIndex();
            directions[i] = col.isAsc();
            // default is NULL LAST
            nullLastdirections[i] = nullDirection == OrderByElement.NullOrdering.NULLS_LAST
                    || nullDirection == null;
            fields[i++] = index;
        }
        return new SortOp(input, directions, fields, nullLastdirections);

    }

    private Projection buildProjection(
            final List<SelectExpressionItem> projects,
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
        for (SelectExpressionItem node : projects) {
            int type = ColumnTypes.ANYTYPE;
            CompiledSQLExpression exp;
            String alias = null;
            if (node.getAlias() != null) {
                alias = fixMySqlBackTicks(node.getAlias().getName().toLowerCase());
                checkSupported(node.getAlias().getAliasColumns() == null);
            }
            if (node.getExpression() instanceof net.sf.jsqlparser.schema.Column
                    && !isBooleanLiteral((net.sf.jsqlparser.schema.Column) node.getExpression())) {
                net.sf.jsqlparser.schema.Column col = (net.sf.jsqlparser.schema.Column) node.getExpression();
//                checkSupported(col.getTable() == null);
                String columnName = fixMySqlBackTicks(col.getColumnName());

                if (alias == null) {
                    alias = columnName;
                }
                IntHolder indexInSchema = new IntHolder(-1);
                Column found = findColumnInSchema(columnName, tableSchema, indexInSchema);
                if (indexInSchema.value == -1 || found == null) {
                    throw new StatementExecutionException("Column " + columnName + " not found in target table");
                }
                exp = new AccessCurrentRowExpression(indexInSchema.value);
                type = found.type;
            } else {
                exp = SQLParserExpressionCompiler.compileExpression(node.getExpression(), tableSchema);
                if (alias == null) {
                    alias = "col" + i;
                }
            }

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
            Column col = Column.column(alias, type);
            identity = identity && col.name.equals(tableSchema[i].name);
            fieldNames[i] = alias;
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

    private ExecutionPlan buildInsertStatement(String defaultTableSpace, Insert insert, boolean returnValues) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Table table = insert.getTable();
        String tableSpace = table.getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = this.manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no tablespace " + tableSpace + " here");
        }
        String tableName = fixMySqlBackTicks(table.getName().toLowerCase());
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new TableDoesNotExistException("no table " + tableName + " here for " + tableSpace);
        }
        Table tableImpl = tableManager.getTable();
        List<CompiledSQLExpression> keyValueExpression = new ArrayList<>();
        List<String> keyExpressionToColumn = new ArrayList<>();
        List<CompiledSQLExpression> valuesExpressions = new ArrayList<>();
        List<String> valuesColumns = new ArrayList<>();
            boolean invalid = false;
        checkSupported(insert.getSelect() == null);

        ItemsList itemsList = insert.getItemsList();
        checkSupported(itemsList instanceof ExpressionList);

        List<Expression> projects = ((ExpressionList) itemsList).getExpressions();
        int index = 0;
        List<net.sf.jsqlparser.schema.Column> columns = insert.getColumns();
        if (columns == null) { // INSERT INTO TABLE VALUES (xxxx) (no column list)
            columns = new ArrayList<>();
            for (Column c : tableImpl.getColumns()) {
                columns.add(new net.sf.jsqlparser.schema.Column(c.name));
            }
        }
        for (net.sf.jsqlparser.schema.Column column : columns) {
            CompiledSQLExpression exp =
                    SQLParserExpressionCompiler.compileExpression(projects.get(index), tableImpl.getColumns());
            String columnName = fixMySqlBackTicks(column.getColumnName());
            if (exp instanceof ConstantExpression
                    || exp instanceof JdbcParameterExpression
                    || exp instanceof TypedJdbcParameterExpression
                    || exp instanceof CompiledFunction) {
                boolean isAlwaysNull = (exp instanceof ConstantExpression)
                        && ((ConstantExpression) exp).isNull();
                if (!isAlwaysNull) {
                    if (tableImpl.isPrimaryKeyColumn(columnName)) {
                        keyExpressionToColumn.add(columnName);
                        keyValueExpression.add(exp);
                    }
                    valuesColumns.add(columnName);
                    valuesExpressions.add(exp);
                }
                index++;
            } else {
                invalid = true;
                break;
            }
        }
        // handle default values
        for (Column col : tableImpl.getColumns()) {
            if (col.defaultValue != null && !valuesColumns.contains(col.name)) {
                valuesColumns.add(col.name);
                CompiledSQLExpression defaultValueExpression = makeDefaultValue(col);
                valuesExpressions.add(defaultValueExpression);
            }
        }
        DMLStatement statement;
        if (!invalid) {
            RecordFunction keyfunction;
            if (keyValueExpression.isEmpty()
                    && tableImpl.auto_increment) {
                keyfunction = new AutoIncrementPrimaryKeyRecordFunction();
            } else {
                if (keyValueExpression.size() != tableImpl.primaryKey.length) {
                    throw new StatementExecutionException("you must set a value for the primary key (expressions=" + keyValueExpression.size() + ")");
                }
                keyfunction = new SQLRecordKeyFunction(keyExpressionToColumn, keyValueExpression, tableImpl);
            }
            RecordFunction valuesfunction = new SQLRecordFunction(valuesColumns, tableImpl, valuesExpressions);
            statement = new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction, false).setReturnValues(returnValues);
        } else {
            throw new StatementNotSupportedException();
        }

        PlannerOp op = new SimpleInsertOp(statement.setReturnValues(returnValues));
        return optimizePlan(op);
    }

    private ExecutionPlan buildUpsertStatement(String defaultTableSpace, Upsert upsert, boolean returnValues) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Table table = upsert.getTable();
        String tableSpace = table.getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = this.manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no tablespace " + tableSpace + " here");
        }
        String tableName = fixMySqlBackTicks(table.getName().toLowerCase());
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new TableDoesNotExistException("no table " + tableName + " here for " + tableSpace);
        }
        Table tableImpl = tableManager.getTable();
        List<CompiledSQLExpression> keyValueExpression = new ArrayList<>();
        List<String> keyExpressionToColumn = new ArrayList<>();
        List<CompiledSQLExpression> valuesExpressions = new ArrayList<>();
        List<String> valuesColumns = new ArrayList<>();
            boolean invalid = false;
        checkSupported(upsert.getSelect() == null);
        checkSupported(upsert.getDuplicateUpdateColumns() == null);
        checkSupported(upsert.getDuplicateUpdateExpressionList() == null);

        ItemsList itemsList = upsert.getItemsList();
        checkSupported(itemsList instanceof ExpressionList);

        List<Expression> projects = ((ExpressionList) itemsList).getExpressions();
        int index = 0;
        List<net.sf.jsqlparser.schema.Column> columns = upsert.getColumns();
        if (columns == null) { // INSERT INTO TABLE VALUES (xxxx) (no column list)
            columns = new ArrayList<>();
            for (Column c : tableImpl.getColumns()) {
                columns.add(new net.sf.jsqlparser.schema.Column(c.name));
            }
        }
        for (net.sf.jsqlparser.schema.Column column : columns) {
            CompiledSQLExpression exp =
                    SQLParserExpressionCompiler.compileExpression(projects.get(index), tableImpl.getColumns());
            String columnName = fixMySqlBackTicks(column.getColumnName());
            if (exp instanceof ConstantExpression
                    || exp instanceof JdbcParameterExpression
                    || exp instanceof TypedJdbcParameterExpression
                    || exp instanceof CompiledFunction) {
                boolean isAlwaysNull = (exp instanceof ConstantExpression)
                        && ((ConstantExpression) exp).isNull();
                if (!isAlwaysNull) {
                    if (tableImpl.isPrimaryKeyColumn(columnName)) {
                        keyExpressionToColumn.add(columnName);
                        keyValueExpression.add(exp);
                    }
                    valuesColumns.add(columnName);
                    valuesExpressions.add(exp);
                }
                index++;
            } else {
                invalid = true;
                break;
            }
        }
        // handle default values
        for (Column col : tableImpl.getColumns()) {
            if (col.defaultValue != null && !valuesColumns.contains(col.name)) {
                valuesColumns.add(col.name);
                CompiledSQLExpression defaultValueExpression = makeDefaultValue(col);
                valuesExpressions.add(defaultValueExpression);
            }
        }
        DMLStatement statement;
        if (!invalid) {
            RecordFunction keyfunction;
            if (keyValueExpression.isEmpty()
                    && tableImpl.auto_increment) {
                keyfunction = new AutoIncrementPrimaryKeyRecordFunction();
            } else {
                if (keyValueExpression.size() != tableImpl.primaryKey.length) {
                    throw new StatementExecutionException("you must set a value for the primary key (expressions=" + keyValueExpression.size() + ")");
                }
                keyfunction = new SQLRecordKeyFunction(keyExpressionToColumn, keyValueExpression, tableImpl);
            }
            RecordFunction valuesfunction = new SQLRecordFunction(valuesColumns, tableImpl, valuesExpressions);
            statement = new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction, true).setReturnValues(returnValues);
        } else {
            throw new StatementNotSupportedException();
        }

        PlannerOp op = new SimpleInsertOp(statement.setReturnValues(returnValues));
        return optimizePlan(op);
    }

    private ExecutionPlan buildUpdateStatement(String defaultTableSpace, Update update, boolean returnValues) throws StatementExecutionException {
        final boolean upsert = false;
        net.sf.jsqlparser.schema.Table table = update.getTable();
        String tableSpace = table.getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = this.manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no tablespace " + tableSpace + " here");
        }
        String tableName = fixMySqlBackTicks(table.getName().toLowerCase());
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new TableDoesNotExistException("no table " + tableName + " here for " + tableSpace);
        }
        Table tableImpl = tableManager.getTable();
        checkSupported(update.getSelect() == null);
        checkSupported(update.getJoins() == null);
        checkSupported(update.getOrderByElements() == null);
        checkSupported(update.getReturningExpressionList() == null);
        checkSupported(update.getStartJoins() == null || update.getStartJoins().isEmpty());
        List<Expression> projects = update.getExpressions();
        List<CompiledSQLExpression> expressions = new ArrayList<>(projects.size());
        int index = 0;
        List<String> updateColumnList = new ArrayList<>(projects.size());
        for (net.sf.jsqlparser.schema.Column column : update.getColumns()) {
            checkSupported(column.getTable() == null);
            updateColumnList.add(fixMySqlBackTicks(column.getColumnName()));
            CompiledSQLExpression exp =
                    SQLParserExpressionCompiler.compileExpression(projects.get(index), tableImpl.getColumns());
            expressions.add(exp);
            index++;
        }

        RecordFunction function = new SQLRecordFunction(updateColumnList, tableImpl, expressions);
        Predicate where = new FullTableScanPredicate();
        if (update.getWhere() != null) {
            CompiledSQLExpression whereExpression = SQLParserExpressionCompiler.compileExpression(update.getWhere(), tableImpl.getColumns());
            SQLRecordPredicate sqlWhere = new SQLRecordPredicate(tableImpl, null, whereExpression);
            IndexUtils.discoverIndexOperations(tableSpace, whereExpression, tableImpl, sqlWhere, update, tableSpaceManager);
            where = sqlWhere;
        }

        PlannerOp op = new SimpleUpdateOp(new UpdateStatement(tableSpace, tableName, null, function, where)
                                                    .setReturnValues(returnValues));
        return optimizePlan(op);
    }

    private ExecutionPlan buildDeleteStatement(String defaultTableSpace, Delete delete) throws StatementExecutionException {
        final boolean upsert = false;
        net.sf.jsqlparser.schema.Table table = delete.getTable();
        String tableSpace = table.getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = this.manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no tablespace " + tableSpace + " here");
        }
        String tableName = fixMySqlBackTicks(table.getName().toLowerCase());
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new TableDoesNotExistException("no table " + tableName + " here for " + tableSpace);
        }
        Table tableImpl = tableManager.getTable();
        checkSupported(delete.getLimit() == null);
        checkSupported(delete.getJoins() == null);
        checkSupported(delete.getOrderByElements() == null);
        checkSupported(delete.getTables() == null || delete.getTables().isEmpty());

        Predicate where = new FullTableScanPredicate();
        if (delete.getWhere() != null) {
            CompiledSQLExpression whereExpression = SQLParserExpressionCompiler.compileExpression(delete.getWhere(), tableImpl.getColumns());
            SQLRecordPredicate sqlWhere = new SQLRecordPredicate(tableImpl, null, SQLParserExpressionCompiler.compileExpression(delete.getWhere(), tableImpl.getColumns()));
            IndexUtils.discoverIndexOperations(tableSpace, whereExpression, tableImpl, sqlWhere, delete, tableSpaceManager);
            where = sqlWhere;
        }

        PlannerOp op = new SimpleDeleteOp(new DeleteStatement(tableSpace, tableName, null, where));
        return optimizePlan(op);
    }


    private static boolean containsDefaultClause(ColumnDefinition cf) {
        List<String> specs = cf.getColumnSpecs();
        if (specs == null || specs.isEmpty()) {
            return false;
        }
        for (String spec : specs) {
            if (spec.equalsIgnoreCase("default")) {
                return true;
            }
        }
        return false;
    }

    private static Bytes decodeDefaultValue(ColumnDefinition cf, int type) {
        List<String> specs = cf.getColumnSpecs();
        if (specs == null || specs.isEmpty()) {
            return null;
        }
        int defaultKeyWordPos = -1;
        int i = 0;
        for (String spec : specs) {
            if (spec.equalsIgnoreCase("default")) {
                defaultKeyWordPos = i;
                break;
            }
            i++;
        }
        if (defaultKeyWordPos < 0) {
            return null;
        }
        if (defaultKeyWordPos == specs.size() - 1) {
            throw new StatementExecutionException("Bad default constraint specs: " + specs);
        }
        String defaultRepresentation = specs.get(defaultKeyWordPos + 1);
        if (defaultRepresentation.isEmpty()) { // not possible
            throw new StatementExecutionException("Bad default constraint specs: " + specs);
        }
        if (defaultRepresentation.equalsIgnoreCase("null")) {
            return null;
        }
        try {
            switch (type) {
                case ColumnTypes.STRING:
                case ColumnTypes.NOTNULL_STRING:
                    if (defaultRepresentation.length() <= 1) {
                        throw new StatementExecutionException("Bad default constraint specs: " + specs);
                    }
                    if (!defaultRepresentation.startsWith("'") || !defaultRepresentation.endsWith("'")) {
                        throw new StatementExecutionException("Bad default constraint specs: " + specs);
                    }
                    // TODO: unescape values
                    return Bytes.from_string(defaultRepresentation.substring(1, defaultRepresentation.length() - 1));
                case ColumnTypes.INTEGER:
                case ColumnTypes.NOTNULL_INTEGER:
                    return Bytes.from_int(Integer.parseInt(defaultRepresentation));
                case ColumnTypes.LONG:
                case ColumnTypes.NOTNULL_LONG:
                    return Bytes.from_long(Long.parseLong(defaultRepresentation));
                case ColumnTypes.DOUBLE:
                case ColumnTypes.NOTNULL_DOUBLE:
                    // TODO: deal with Java Locale
                    return Bytes.from_double(Double.parseDouble(defaultRepresentation));
                case ColumnTypes.BOOLEAN:
                case ColumnTypes.NOTNULL_BOOLEAN:
                    if (defaultRepresentation.length() <= 1) {
                        throw new StatementExecutionException("Bad default constraint specs: " + specs);
                    }
                    if (!defaultRepresentation.startsWith("'") || !defaultRepresentation.endsWith("'")) {
                        throw new StatementExecutionException("Bad default constraint specs: " + specs);
                    }
                    defaultRepresentation = defaultRepresentation.substring(1, defaultRepresentation.length() - 1);
                    if (!defaultRepresentation.equalsIgnoreCase("true")
                            && !defaultRepresentation.equalsIgnoreCase("false")) {
                        throw new StatementExecutionException("Bad default constraint specs: " + specs);
                    }
                    return Bytes.from_boolean(Boolean.parseBoolean(defaultRepresentation));
                case ColumnTypes.TIMESTAMP:
                case ColumnTypes.NOTNULL_TIMESTAMP:
                    if (!defaultRepresentation.equalsIgnoreCase("current_timestamp")) {
                        throw new StatementExecutionException("Bad default constraint specs: " + specs);
                    }
                    return Bytes.from_string("CURRENT_TIMESTAMP");
                default:
                    throw new StatementExecutionException("Default not yet supported for columns of type " + ColumnTypes.typeToString(type));
            }
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("Bad default constraint specs: " + specs, err);
        }

    }

    private static CompiledSQLExpression makeDefaultValue(Column col) {
        switch (col.type) {
            case ColumnTypes.NOTNULL_STRING:
            case ColumnTypes.STRING:
                return new ConstantExpression(col.defaultValue.to_string());
            case ColumnTypes.NOTNULL_INTEGER:
            case ColumnTypes.INTEGER:
                return new ConstantExpression(col.defaultValue.to_int());
            case ColumnTypes.NOTNULL_LONG:
            case ColumnTypes.LONG:
                return new ConstantExpression(col.defaultValue.to_long());
            case ColumnTypes.NOTNULL_DOUBLE:
            case ColumnTypes.DOUBLE:
                return new ConstantExpression(col.defaultValue.to_double());
            case ColumnTypes.NOTNULL_BOOLEAN:
            case ColumnTypes.BOOLEAN:
                return new ConstantExpression(col.defaultValue.to_boolean());
            case ColumnTypes.NOTNULL_TIMESTAMP:
            case ColumnTypes.TIMESTAMP:
                return new CompiledFunction(BuiltinFunctions.CURRENT_TIMESTAMP, Collections.emptyList());
            default:
                throw new UnsupportedOperationException("Not supported for type " + ColumnTypes.typeToString(col.type));
        }
    }

    private static class StatementNotSupportedException extends HerdDBInternalException {

        public StatementNotSupportedException() {
        }

        public StatementNotSupportedException(String message) {
            super(message);
        }

    }

    public static void checkSupported(boolean condition) {
        if (!condition) {
            throw new StatementNotSupportedException();
        }
    }
    public static void checkSupported(boolean condition, Object message) {
        if (!condition) {
            throw new StatementNotSupportedException(message + "");
        }
    }

}
