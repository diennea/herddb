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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
import herddb.model.Aggregator;
import herddb.model.AutoIncrementPrimaryKeyRecordFunction;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.ColumnsList;
import herddb.model.DMLStatement;
import herddb.model.ExecutionPlan;
import herddb.model.Predicate;
import herddb.model.Projection;
import herddb.model.RecordFunction;
import herddb.model.ScanLimitsImpl;
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableDoesNotExistException;
import herddb.model.TableSpace;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TupleComparator;
import herddb.model.TuplePredicate;
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
import herddb.model.commands.ScanStatement;
import herddb.model.commands.TruncateTableStatement;
import herddb.model.commands.UpdateStatement;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.sql.expressions.SQLExpressionCompiler;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.IntHolder;
import herddb.utils.SQLUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

/**
 * Translates SQL to Internal API
 *
 * @author enrico.olivelli
 */
public class SQLPlanner implements AbstractSQLPlanner {

    private final DBManager manager;
    private final PlansCache cache;

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
    }

    public SQLPlanner(DBManager manager, long maxPlanCacheSize) {
        this.manager = manager;
        this.cache = new PlansCache(maxPlanCacheSize);
    }

    public static String rewriteExecuteSyntax(String query) {
        char ch = query.charAt(0);

        /* "empty" data skipped now we must recognize instructions to rewrite */
        switch (ch) {
            /* ALTER */
            case 'A':
            case 'a':
                if (query.regionMatches(true, 0, "ALTER TABLESPACE ", 0, 17)) {
                    return "EXECUTE altertablespace " + query.substring(17);
                }

                return query;

            /* BEGIN */
            case 'B':
            case 'b':
                if (query.regionMatches(true, 0, "BEGIN TRANSACTION", 0, 17)) {
                    return "EXECUTE begintransaction" + query.substring(17);
                }

                return query;

            /* COMMIT / CREATE */
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

            /* DROP */
            case 'D':
            case 'd':
                if (query.regionMatches(true, 0, "DROP TABLESPACE ", 0, 16)) {
                    return "EXECUTE droptablespace " + query.substring(16);
                }

                return query;

            /* ROLLBACK */
            case 'R':
            case 'r':
                if (query.regionMatches(true, 0, "ROLLBACK TRANSACTION", 0, 20)) {
                    return "EXECUTE rollbacktransaction" + query.substring(20);
                }
                return query;

            /* TRUNCATE */
            case 'T':
            case 't':
                if (query.regionMatches(true, 0, "TRUNCATE", 0, 8)) {
                    return "TRUNCATE" + query.substring(8);
                }
                return query;
            default:
                return query;
        }
    }

    @Override
    public TranslatedQuery translate(String defaultTableSpace, String query, List<Object> parameters,
        boolean scan, boolean allowCache, boolean returnValues, int maxRows) throws StatementExecutionException {
        if (parameters == null) {
            parameters = Collections.emptyList();
        }

        /* Strips out leading comments */
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
        if (allowCache) {
            ExecutionPlan cached = cache.get(cacheKey);
            if (cached != null) {
                return new TranslatedQuery(cached, new SQLStatementEvaluationContext(query, parameters));
            }
        }
        net.sf.jsqlparser.statement.Statement stmt = parseStatement(query);
        if (!isCachable(stmt)) {
            allowCache = false;
        }
        ExecutionPlan executionPlan = plan(defaultTableSpace, stmt, scan, returnValues, maxRows);
        if (allowCache) {
            cache.put(cacheKey, executionPlan);
        }
        return new TranslatedQuery(executionPlan, new SQLStatementEvaluationContext(query, parameters));

    }

    private net.sf.jsqlparser.statement.Statement parseStatement(String query) throws StatementExecutionException {
        net.sf.jsqlparser.statement.Statement stmt;
        try {
            stmt = CCJSqlParserUtil.parse(query);
        } catch (JSQLParserException | net.sf.jsqlparser.parser.TokenMgrError err) {
            throw new StatementExecutionException("unable to parse query " + query, err);
        }
        return stmt;
    }

    @Override
    public ExecutionPlan plan(String defaultTableSpace, net.sf.jsqlparser.statement.Statement stmt,
        boolean scan, boolean returnValues, int maxRows) {
        verifyJdbcParametersIndexes(stmt);
        ExecutionPlan result;
        if (stmt instanceof CreateTable) {
            result = ExecutionPlan.simple(buildCreateTableStatement(defaultTableSpace, (CreateTable) stmt));
        } else if (stmt instanceof CreateIndex) {
            result = ExecutionPlan.simple(buildCreateIndexStatement(defaultTableSpace, (CreateIndex) stmt));
        } else if (stmt instanceof Insert) {
            result = buildInsertStatement(defaultTableSpace, (Insert) stmt, returnValues);
        } else if (stmt instanceof Delete) {
            result = buildDeleteStatement(defaultTableSpace, (Delete) stmt, returnValues);
        } else if (stmt instanceof Update) {
            result = buildUpdateStatement(defaultTableSpace, (Update) stmt, returnValues);
        } else if (stmt instanceof Select) {
            result = buildSelectStatement(defaultTableSpace, (Select) stmt, scan, maxRows);
        } else if (stmt instanceof Execute) {
            result = ExecutionPlan.simple(buildExecuteStatement(defaultTableSpace, (Execute) stmt));
        } else if (stmt instanceof Alter) {
            result = ExecutionPlan.simple(buildAlterStatement(defaultTableSpace, (Alter) stmt));
        } else if (stmt instanceof Drop) {
            result = ExecutionPlan.simple(buildDropStatement(defaultTableSpace, (Drop) stmt));
        } else if (stmt instanceof Truncate) {
            result = ExecutionPlan.simple(buildTruncateStatement(defaultTableSpace, (Truncate) stmt));
        } else {
            return null;
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
        } else if (stmt instanceof Truncate) {
            return false;
        }
        return true;
    }

    private Statement buildCreateTableStatement(String defaultTableSpace, CreateTable s) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        try {
            boolean foundPk = false;
            Table.Builder tablebuilder = Table.builder()
                .uuid(UUID.randomUUID().toString())
                .name(tableName)
                .tablespace(tableSpace);
            Set<String> primaryKey = new HashSet<>();

            if (s.getIndexes() != null) {
                for (Index index : s.getIndexes()) {
                    if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {
                        for (String n : index.getColumnsNames()) {
                            n = n.toLowerCase();
                            tablebuilder.primaryKey(n);
                            primaryKey.add(n);
                            foundPk = true;
                        }
                    }
                }
            }

            int position = 0;
            for (ColumnDefinition cf : s.getColumnDefinitions()) {
                String columnName = cf.getColumnName().toLowerCase();
                int type;
                String dataType = cf.getColDataType().getDataType();
                type = sqlDataTypeToColumnType(dataType, cf.getColDataType().getArgumentsStringList());
                tablebuilder.column(columnName, type, position++);

                if (cf.getColumnSpecStrings() != null) {
                    List<String> columnSpecs = decodeColumnSpecs(cf.getColumnSpecStrings());
                    boolean auto_increment = decodeAutoIncrement(columnSpecs);
                    if (columnSpecs.contains("PRIMARY")) {
                        foundPk = true;
                        tablebuilder.primaryKey(columnName, auto_increment);
                    }
                    if (auto_increment && primaryKey.contains(cf.getColumnName())) {
                        tablebuilder.primaryKey(columnName, auto_increment);
                    }
                }
            }

            if (!foundPk) {
                tablebuilder.column("_pk", ColumnTypes.LONG, position++);
                tablebuilder.primaryKey("_pk", true);
            }

            Table table = tablebuilder.build();
            List<herddb.model.Index> otherIndexes = new ArrayList<>();
            if (s.getIndexes() != null) {
                for (Index index : s.getIndexes()) {
                    if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {

                    } else if (index.getType().equalsIgnoreCase("INDEX")) {
                        String indexName = index.getName().toLowerCase();
                        String indexType = convertIndexType(null);

                        herddb.model.Index.Builder builder = herddb.model.Index
                            .builder()
                            .name(indexName)
                            .type(indexType)
                            .uuid(UUID.randomUUID().toString())
                            .table(tableName)
                            .tablespace(tableSpace);

                        for (String columnName : index.getColumnsNames()) {
                            columnName = columnName.toLowerCase();
                            Column column = table.getColumn(columnName);
                            if (column == null) {
                                throw new StatementExecutionException("no such column " + columnName + " on table " + tableName + " in tablespace " + tableSpace);
                            }
                            builder.column(column.name, column.type);
                        }

                        otherIndexes.add(builder.build());
                    }
                }
            }

            CreateTableStatement statement = new CreateTableStatement(table, otherIndexes);
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad table definition: " + err.getMessage(), err);
        }
    }

    private boolean decodeAutoIncrement(List<String> columnSpecs) {
        boolean auto_increment = columnSpecs.contains("AUTO_INCREMENT");
        return auto_increment;
    }

    private String decodeRenameTo(List<String> columnSpecs) {
        if (columnSpecs.size() != 3) {
            return null;
        }
        if (!"RENAME".equals(columnSpecs.get(0))) {
            return null;
        }
        if (!"TO".equals(columnSpecs.get(1))) {
            return null;
        }
        return columnSpecs.get(2).toLowerCase();

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
            String tableName = s.getTable().getName();

            String indexName = s.getIndex().getName().toLowerCase();
            String indexType = convertIndexType(s.getIndex().getType());

            herddb.model.Index.Builder builder = herddb.model.Index
                .builder()
                .name(indexName)
                .uuid(UUID.randomUUID().toString())
                .type(indexType)
                .table(tableName)
                .tablespace(tableSpace);

            AbstractTableManager tableDefinition = manager.getTableSpaceManager(tableSpace).getTableManager(tableName);
            if (tableDefinition == null) {
                throw new TableDoesNotExistException("no such table " + tableName + " in tablespace " + tableSpace);
            }
            for (String columnName : s.getIndex().getColumnsNames()) {
                columnName = columnName.toLowerCase();
                Column column = tableDefinition.getTable().getColumn(columnName);
                if (column == null) {
                    throw new StatementExecutionException("no such column " + columnName + " on table " + tableName + " in tablespace " + tableSpace);
                }
                builder.column(column.name, column.type);
            }

            CreateIndexStatement statement = new CreateIndexStatement(builder.build());
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad index definition: " + err.getMessage(), err);
        }
    }

    private String convertIndexType(String indexType) throws StatementExecutionException {
        if (indexType == null) {
            indexType = herddb.model.Index.TYPE_BRIN;
        } else {
            indexType = indexType.toLowerCase();
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

    private int sqlDataTypeToColumnType(String dataType, List<String> arguments) throws StatementExecutionException {
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
            case "datetime":
                type = ColumnTypes.TIMESTAMP;
                break;
            case "boolean":
            case "bool":
                type = ColumnTypes.BOOLEAN;
                break;
            case "double":
            case "float":
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
        return type;
    }

    private ExecutionPlan buildInsertStatement(String defaultTableSpace, Insert s, boolean returnValues) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here at " + manager.getNodeId());
        }
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablespace " + tableSpace);
        }
        Table table = tableManager.getTable();

        ItemsList itemlist = s.getItemsList();
        if (itemlist instanceof ExpressionList) {
            int index = 0;
            List<Expression> keyValueExpression = new ArrayList<>();
            List<String> keyExpressionToColumn = new ArrayList<>();

            List<CompiledSQLExpression> valuesExpressions = new ArrayList<>();
            List<net.sf.jsqlparser.schema.Column> valuesColumns = new ArrayList<>();

            ExpressionList list = (ExpressionList) itemlist;
            if (s.getColumns() != null) {
                for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
                    Column column = table.getColumn(c.getColumnName());
                    if (column == null) {
                        throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablespace " + tableSpace);
                    }
                    Expression expression;
                    try {
                        expression = list.getExpressions().get(index++);
                    } catch (IndexOutOfBoundsException badQuery) {
                        throw new StatementExecutionException("bad number of VALUES in INSERT clause");
                    }

                    if (table.isPrimaryKeyColumn(column.name)) {
                        keyExpressionToColumn.add(column.name);
                        keyValueExpression.add(expression);

                    }
                    valuesColumns.add(c);
                    valuesExpressions.add(SQLExpressionCompiler.compileExpression(null, expression));
                }
            } else {
                for (Column column : table.columns) {

                    Expression expression = list.getExpressions().get(index++);
                    if (table.isPrimaryKeyColumn(column.name)) {
                        keyExpressionToColumn.add(column.name);
                        keyValueExpression.add(expression);
                    }
                    valuesColumns.add(new net.sf.jsqlparser.schema.Column(column.name));
                    valuesExpressions.add(SQLExpressionCompiler.compileExpression(null, expression));
                }
            }

            RecordFunction keyfunction;
            if (keyValueExpression.isEmpty() && table.auto_increment) {
                keyfunction = new AutoIncrementPrimaryKeyRecordFunction();
            } else {
                if (keyValueExpression.size() != table.primaryKey.length) {
                    throw new StatementExecutionException("you must set a value for the primary key (expressions=" + keyValueExpression.size() + ")");
                }
                keyfunction = new SQLRecordKeyFunction(table, keyExpressionToColumn, keyValueExpression);
            }
            RecordFunction valuesfunction = new SQLRecordFunction(table, valuesColumns, valuesExpressions);

            try {
                return ExecutionPlan.simple(new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction).setReturnValues(returnValues));
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        } else if (itemlist instanceof MultiExpressionList) {
            if (returnValues) {
                throw new StatementExecutionException("cannot 'return values' on multi-values insert");
            }
            MultiExpressionList multilist = (MultiExpressionList) itemlist;

            List<InsertStatement> inserts = new ArrayList<>();
            for (ExpressionList list : multilist.getExprList()) {
                List<Expression> keyValueExpression = new ArrayList<>();
                List<String> keyExpressionToColumn = new ArrayList<>();

                List<CompiledSQLExpression> valuesExpressions = new ArrayList<>();
                List<net.sf.jsqlparser.schema.Column> valuesColumns = new ArrayList<>();

                int index = 0;
                if (s.getColumns() != null) {
                    for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
                        Column column = table.getColumn(c.getColumnName());
                        if (column == null) {
                            throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablespace " + tableSpace);
                        }
                        Expression expression;
                        try {
                            expression = list.getExpressions().get(index++);
                        } catch (IndexOutOfBoundsException badQuery) {
                            throw new StatementExecutionException("bad number of VALUES in INSERT clause");
                        }

                        if (table.isPrimaryKeyColumn(column.name)) {
                            keyExpressionToColumn.add(column.name);
                            keyValueExpression.add(expression);

                        }
                        valuesColumns.add(c);
                        valuesExpressions.add(SQLExpressionCompiler.compileExpression(null, expression));
                    }
                } else {
                    for (Column column : table.columns) {

                        Expression expression = list.getExpressions().get(index++);
                        if (table.isPrimaryKeyColumn(column.name)) {
                            keyExpressionToColumn.add(column.name);
                            keyValueExpression.add(expression);
                        }
                        valuesColumns.add(new net.sf.jsqlparser.schema.Column(column.name));
                        valuesExpressions.add(SQLExpressionCompiler.compileExpression(null, expression));
                    }
                }

                RecordFunction keyfunction;
                if (keyValueExpression.isEmpty() && table.auto_increment) {
                    keyfunction = new AutoIncrementPrimaryKeyRecordFunction();
                } else {
                    if (keyValueExpression.size() != table.primaryKey.length) {
                        throw new StatementExecutionException("you must set a value for the primary key (expressions=" + keyValueExpression.size() + ")");
                    }
                    keyfunction = new SQLRecordKeyFunction(table, keyExpressionToColumn, keyValueExpression);
                }
                RecordFunction valuesfunction = new SQLRecordFunction(table, valuesColumns, valuesExpressions);
                InsertStatement insert = new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction);
                inserts.add(insert);
            }
            try {
                return ExecutionPlan.multiInsert(inserts);
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        } else {
            List<Expression> keyValueExpression = new ArrayList<>();
            List<String> keyExpressionToColumn = new ArrayList<>();

            List<CompiledSQLExpression> valuesExpressions = new ArrayList<>();
            List<net.sf.jsqlparser.schema.Column> valuesColumns = new ArrayList<>();

            Select select = s.getSelect();
            ExecutionPlan datasource = buildSelectStatement(defaultTableSpace, select, true, -1);
            if (s.getColumns() == null) {
                throw new StatementExecutionException("for INSERT ... SELECT you have to declare the columns to be filled in (use INSERT INTO TABLE(c,c,c,) SELECT .....)");
            }
            IntHolder holder = new IntHolder(1);
            for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
                Column column = table.getColumn(c.getColumnName());
                if (column == null) {
                    throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablespace " + tableSpace);
                }
                JdbcParameter readFromResultSetAsJdbcParameter = new JdbcParameter();
                readFromResultSetAsJdbcParameter.setIndex(holder.value++);

                if (table.isPrimaryKeyColumn(column.name)) {
                    keyExpressionToColumn.add(column.name);
                    keyValueExpression.add(readFromResultSetAsJdbcParameter);
                }
                valuesColumns.add(c);
                valuesExpressions.add(SQLExpressionCompiler.compileExpression(null, readFromResultSetAsJdbcParameter));
            }

            RecordFunction keyfunction;
            if (keyValueExpression.isEmpty() && table.auto_increment) {
                keyfunction = new AutoIncrementPrimaryKeyRecordFunction();
            } else {
                if (keyValueExpression.size() != table.primaryKey.length) {
                    throw new StatementExecutionException("you must set a value for the primary key (expressions=" + keyValueExpression.size() + ")");
                }
                keyfunction = new SQLRecordKeyFunction(table, keyExpressionToColumn, keyValueExpression);
            }
            RecordFunction valuesfunction = new SQLRecordFunction(table, valuesColumns, valuesExpressions);

            try {
                return ExecutionPlan.dataManipulationFromSelect(
                    new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction)
                        .setReturnValues(returnValues),
                    datasource);
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        }
    }

    private ExecutionPlan buildDeleteStatement(String defaultTableSpace, Delete s, boolean returnValues) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Table fromTable = s.getTable();
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here at " + manager.getNodeId());
        }
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablespace " + tableSpace);
        }
        Table table = tableManager.getTable();

        // Perform a scan and then delete each row
        SQLRecordPredicate where = s.getWhere() != null ? new SQLRecordPredicate(table, table.name, s.getWhere()) : null;
        if (where != null) {
            Expression expressionWhere = s.getWhere();
            discoverIndexOperations(expressionWhere, table, table.name, where, tableSpaceManager);
        }

        DMLStatement st = new DeleteStatement(tableSpace, tableName, null, where).setReturnValues(returnValues);
        return ExecutionPlan.simple(st);

    }

    private void discoverIndexOperations(Expression expressionWhere, Table table, String mainTableAlias, SQLRecordPredicate where, TableSpaceManager tableSpaceManager) throws StatementExecutionException {
        SQLRecordKeyFunction keyFunction = findIndexAccess(expressionWhere, table.primaryKey, table, mainTableAlias, EqualsTo.class);
        IndexOperation result = null;
        if (keyFunction != null) {
            if (keyFunction.isFullPrimaryKey()) {
                result = new PrimaryIndexSeek(keyFunction);
            } else {
                result = new PrimaryIndexPrefixScan(keyFunction);
            }
        } else {
            SQLRecordKeyFunction rangeMin = findIndexAccess(expressionWhere, table.primaryKey,
                table, mainTableAlias, GreaterThanEquals.class
            );
            if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                rangeMin = null;
            }
            if (rangeMin == null) {
                rangeMin = findIndexAccess(expressionWhere, table.primaryKey, table, mainTableAlias, GreaterThan.class);
                if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                    rangeMin = null;
                }
            }

            SQLRecordKeyFunction rangeMax = findIndexAccess(expressionWhere, table.primaryKey, table, mainTableAlias, MinorThanEquals.class
            );
            if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                rangeMax = null;

            }
            if (rangeMax == null) {
                rangeMax = findIndexAccess(expressionWhere, table.primaryKey, table, mainTableAlias, MinorThan.class
                );
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
        where.setIndexOperation(result);
        Expression filterPk = findFiltersOnPrimaryKey(table, table.name, expressionWhere);
        where.setPrimaryKeyFilter(filterPk);
    }

    private ExecutionPlan buildUpdateStatement(String defaultTableSpace, Update s,
        boolean returnValues) throws StatementExecutionException {
        if (s.getTables().size() != 1) {
            throw new StatementExecutionException("unsupported multi-table update " + s);
        }
        net.sf.jsqlparser.schema.Table fromTable = s.getTables().get(0);
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here at " + manager.getNodeId());
        }
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablespace " + tableSpace);
        }
        Table table = tableManager.getTable();
        for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
            Column column = table.getColumn(c.getColumnName());
            if (column == null) {
                throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablespace " + tableSpace);
            }
            if (table.isPrimaryKeyColumn(c.getColumnName())) {
                throw new StatementExecutionException("updates of fields on the PK (" + Arrays.toString(table.primaryKey) + ") are not supported. Please perform a DELETE and than an INSERT");
            }
        }

        List<CompiledSQLExpression> compiledSQLExpressions = new ArrayList<>();
        for (Expression e : s.getExpressions()) {
            compiledSQLExpressions.add(SQLExpressionCompiler.compileExpression(null, e));
        }
        RecordFunction function = new SQLRecordFunction(table, s.getColumns(), compiledSQLExpressions);

        // Perform a scan and then update each row
        SQLRecordPredicate where = s.getWhere() != null ? new SQLRecordPredicate(table, table.name, s.getWhere()) : null;
        if (where != null) {
            Expression expressionWhere = s.getWhere();
            discoverIndexOperations(expressionWhere, table, table.name, where, tableSpaceManager);
        }
        DMLStatement st = new UpdateStatement(tableSpace, tableName, null, function, where)
            .setReturnValues(returnValues);
        return ExecutionPlan.simple(st);

    }

    private Predicate buildSimplePredicate(Expression where, Table table, String tableAlias) {
        if (where instanceof EqualsTo || where == null) {
            // surely this is the only predicate on the PK, we can skip it
            return null;
        }
        return new SQLRecordPredicate(table, tableAlias, where);

    }

    private static Expression findConstraintOnColumn(Expression where, String columnName, String tableAlias, Class<? extends BinaryExpression> expressionType) throws StatementExecutionException {
        if (where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            Expression keyOnLeft = findConstraintOnColumn(and.getLeftExpression(), columnName, tableAlias, expressionType);
            if (keyOnLeft != null) {
                return keyOnLeft;
            }

            Expression keyOnRight = findConstraintOnColumn(and.getRightExpression(), columnName, tableAlias,
                expressionType);
            if (keyOnRight != null) {
                return keyOnRight;
            }
        } else if (expressionType.isAssignableFrom(where.getClass())) {
            Expression keyDirect = validateColumnConstaintToExpression(where, columnName, tableAlias, expressionType);
            if (keyDirect != null) {
                return keyDirect;
            }
        }

        return null;

    }

    private Expression findConstraintExpressionOnColumn(Expression where, String columnName, String tableAlias, Class<? extends BinaryExpression> expressionType) throws StatementExecutionException {
        if (where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            Expression keyOnLeft = findConstraintExpressionOnColumn(and.getLeftExpression(), columnName, tableAlias, expressionType);
            if (keyOnLeft != null) {
                return keyOnLeft;
            }

            Expression keyOnRight = findConstraintExpressionOnColumn(and.getRightExpression(), columnName, tableAlias,
                expressionType);
            if (keyOnRight != null) {
                return keyOnRight;
            }
        } else if (expressionType.isAssignableFrom(where.getClass())) {
            Expression keyDirect = validateColumnConstaintExpressionToExpression(where, columnName, tableAlias, expressionType);
            if (keyDirect != null) {
                return keyDirect;
            }
        }

        return null;

    }

    private SQLRecordKeyFunction findPrimaryKeyIndexSeek(Expression where, Table table, String tableAlias) throws StatementExecutionException {
        return findIndexAccess(where, table.primaryKey, table, tableAlias, EqualsTo.class
        );
    }

    private static SQLRecordKeyFunction findIndexAccess(Expression where, String[] columnsToMatch, ColumnsList table, String tableAlias, Class<? extends BinaryExpression> expressionType) throws StatementExecutionException {
        List<Expression> expressions = new ArrayList<>();
        List<String> columns = new ArrayList<>();

        for (String pk : columnsToMatch) {
            Expression condition = findConstraintOnColumn(where, pk, tableAlias, expressionType);
            if (condition == null) {
                break;
            }
            columns.add(pk);
            expressions.add(condition);

        }
        if (expressions.isEmpty()) {
            // no match at all, there is no direct constraint on PK
            return null;
        }
        return new SQLRecordKeyFunction(table, columns, expressions);
    }

    private static Object resolveValue(Expression expression, boolean allowColumn) throws StatementExecutionException {
        if (expression instanceof JdbcParameter) {
            throw new StatementExecutionException("jdbcparameter expression not usable in this query");
        } else if (allowColumn && expression instanceof net.sf.jsqlparser.schema.Column) {
            // this is only for supporting back ticks in DDL
            return ((net.sf.jsqlparser.schema.Column) expression).getColumnName();
        } else if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        } else if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
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
                        throw new StatementExecutionException("unsupported value type " + expression.getClass() + " with sign " + se.getSign() + " on value " + value + " of type " + value.getClass());
                    }
                }
                default:
                    throw new StatementExecutionException("unsupported value type " + expression.getClass() + " with sign " + se.getSign());
            }

        } else {
            throw new StatementExecutionException("unsupported value type " + expression.getClass());
        }
    }

    private static Expression validateColumnConstaintToExpression(Expression testExpression, String columnName, String tableAlias, Class<? extends BinaryExpression> expressionType) throws StatementExecutionException {
        Expression result = null;
        if (expressionType.isAssignableFrom(testExpression.getClass())) {
            BinaryExpression e = (BinaryExpression) testExpression;
            if (e.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) e.getLeftExpression();
                boolean okAlias = true;
                if (c.getTable() != null && c.getTable().getName() != null && !c.getTable().getName().equals(tableAlias)) {
                    okAlias = false;
                }
                if (okAlias && columnName.equalsIgnoreCase(c.getColumnName())
                    && SQLRecordPredicate.isConstant(e.getRightExpression())) {
                    return e.getRightExpression();
                }
            } else if (e.getLeftExpression() instanceof AndExpression) {
                result = findConstraintOnColumn(e.getLeftExpression(), columnName, tableAlias, expressionType);
                if (result != null) {
                    return result;
                }
            } else if (e.getRightExpression() instanceof AndExpression) {
                result = findConstraintOnColumn(e.getRightExpression(), columnName, tableAlias, expressionType);
                if (result != null) {
                    return result;
                }
            }
        }
        return result;
    }

    private Expression validateColumnConstaintExpressionToExpression(Expression testExpression, String columnName, String tableAlias, Class<? extends BinaryExpression> expressionType) throws StatementExecutionException {
        Expression result = null;
        if (expressionType.isAssignableFrom(testExpression.getClass())) {
            BinaryExpression e = (BinaryExpression) testExpression;
            if (e.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) e.getLeftExpression();
                boolean okAlias = true;
                if (c.getTable() != null && c.getTable().getName() != null && !c.getTable().getName().equals(tableAlias)) {
                    okAlias = false;
                }
                if (okAlias && columnName.equalsIgnoreCase(c.getColumnName())
                    && SQLRecordPredicate.isConstant(e.getRightExpression())) {
                    return e;
                }
            } else if (e.getLeftExpression() instanceof AndExpression) {
                result = findConstraintExpressionOnColumn(e.getLeftExpression(), columnName, tableAlias, expressionType);
                if (result != null) {
                    return result;
                }
            } else if (e.getRightExpression() instanceof AndExpression) {
                result = findConstraintExpressionOnColumn(e.getRightExpression(), columnName, tableAlias, expressionType);
                if (result != null) {
                    return result;
                }
            }
        }
        return result;
    }

    private ColumnReferencesDiscovery discoverMainTableAlias(Expression expression) throws StatementExecutionException {
        ColumnReferencesDiscovery discovery = new ColumnReferencesDiscovery(expression);
        expression.accept(discovery);
        return discovery;
    }

    private Expression collectConditionsForAlias(String alias, Expression expression,
        List<ColumnReferencesDiscovery> conditionsOnJoinedResult, String mainTableName) throws StatementExecutionException {
        if (expression == null) {
            // no constraint on table
            return null;
        }
        ColumnReferencesDiscovery discoveredMainAlias = discoverMainTableAlias(expression);
        String mainAlias = discoveredMainAlias.getMainTableAlias();
        if (!discoveredMainAlias.isContainsMixedAliases() && alias.equals(mainAlias)) {
            return expression;
        } else if (expression instanceof AndExpression) {
            AndExpression be = (AndExpression) expression;
            ColumnReferencesDiscovery discoveredMainAliasLeft = discoverMainTableAlias(be.getLeftExpression());
            String mainAliasLeft = discoveredMainAliasLeft.isContainsMixedAliases()
                ? null : discoveredMainAliasLeft.getMainTableAlias();

            ColumnReferencesDiscovery discoveredMainAliasright = discoverMainTableAlias(be.getRightExpression());
            String mainAliasRight = discoveredMainAliasright.isContainsMixedAliases()
                ? null : discoveredMainAliasright.getMainTableAlias();
            if (alias.equals(mainAliasLeft)) {
                if (alias.equals(mainAliasRight)) {
                    return expression;
                } else {
                    return be.getLeftExpression();
                }
            } else if (alias.equals(mainAliasRight)) {
                return be.getRightExpression();
            } else {
                // no constraint on table
                return null;
            }
        } else {
            conditionsOnJoinedResult.add(discoveredMainAlias);
            return null;
        }
    }

    private Expression composeAndExpression(List<ColumnReferencesDiscovery> conditionsOnJoinedResult) {
        if (conditionsOnJoinedResult.size() == 1) {
            return conditionsOnJoinedResult.get(0).getExpression();
        }
        AndExpression result = new AndExpression(conditionsOnJoinedResult.get(0).getExpression(),
            conditionsOnJoinedResult.get(1).getExpression());
        for (int i = 2; i < conditionsOnJoinedResult.size(); i++) {
            result = new AndExpression(result, conditionsOnJoinedResult.get(i).getExpression());
        }
        return result;
    }

    private Expression composeSimpleAndExpressions(List<Expression> expressions) {
        if (expressions.size() == 1) {
            return expressions.get(0);
        }
        AndExpression result = new AndExpression(expressions.get(0),
            expressions.get(1));
        for (int i = 2; i < expressions.size(); i++) {
            result = new AndExpression(result, expressions.get(i));
        }
        return result;
    }

    private Expression findFiltersOnPrimaryKey(Table table, String tableAlias, Expression where) throws StatementExecutionException {
        List<Expression> expressions = new ArrayList<>();
        for (String pk : table.primaryKey) {
            Expression condition = findConstraintExpressionOnColumn(where, pk, tableAlias, BinaryExpression.class);
            if (condition == null) {
                break;
            }
            expressions.add(condition);
        }
        if (expressions.isEmpty()) {
            // no match at all, there is no direct constraint on PK
            return null;
        } else {
            return composeSimpleAndExpressions(expressions);
        }
    }

    private static class JoinSupport {

        final TableRef tableRef;
        final AbstractTableManager tableManager;
        final Table table;
        Projection projection;
        List<SelectItem> selectItems = new ArrayList<>();
        boolean allColumns;
        Predicate predicate;

        public JoinSupport(TableRef tableRef, AbstractTableManager tableManager) {
            this.tableRef = tableRef;
            this.tableManager = tableManager;
            this.table = tableManager.getTable();
        }

    }

    private ExecutionPlan buildSelectStatement(String defaultTableSpace, Select s, boolean scan, int maxRows) throws StatementExecutionException {
        PlainSelect selectBody = (PlainSelect) s.getSelectBody();
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) selectBody.getFromItem();

        TableRef mainTable = TableRef.buildFrom(fromTable, defaultTableSpace);
        String mainTableAlias = mainTable.tableAlias;
        String tableSpace = mainTable.tableSpace;
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new TableSpaceDoesNotExistException("no such tablespace " + tableSpace + " here at " + manager.getNodeId());
        }
        AbstractTableManager tableManager = tableSpaceManager.getTableManager(mainTable.tableName);
        if (tableManager == null) {
            throw new TableDoesNotExistException("no such table " + mainTable.tableName + " in tablespace " + tableSpace);
        }

        // linked hash map retains the order of insertions
        LinkedHashMap<String, JoinSupport> joins = new LinkedHashMap<>();
        boolean joinPresent = false;
        joins.put(mainTable.tableAlias, new JoinSupport(mainTable, tableManager));

        if (selectBody.getJoins() != null) {
            for (Join join : selectBody.getJoins()) {
                joinPresent = true;
                if (join.isLeft()
                    || join.isCross()
                    || join.isRight()
                    || join.isOuter()
                    || join.isSimple()) {
                    throw new StatementExecutionException("unsupported JOIN type: " + join);
                }
                net.sf.jsqlparser.schema.Table joinedTable = (net.sf.jsqlparser.schema.Table) join.getRightItem();
                TableRef joinedTableRef = TableRef.buildFrom(joinedTable, defaultTableSpace);
                if (!joinedTableRef.tableSpace.equalsIgnoreCase(mainTable.tableSpace)) {
                    throw new TableDoesNotExistException("unsupported cross-tablespace JOIN "
                        + "between" + mainTable.tableSpace + "." + mainTable.tableName
                        + " and " + joinedTableRef.tableSpace + "." + joinedTableRef.tableName);
                }
                AbstractTableManager joinedTableManager = tableSpaceManager.getTableManager(joinedTableRef.tableName);
                if (joinedTableManager == null) {
                    throw new TableDoesNotExistException("no such table " + joinedTableRef.tableName + " in tablespace " + tableSpace);
                }
                JoinSupport joinSupport = new JoinSupport(joinedTableRef, joinedTableManager);
                joins.put(joinedTableRef.tableAlias, joinSupport);
            }
        }

        Projection mainTableProjection;
        Table table = tableManager.getTable();
        boolean allColumns = false;
        boolean containsAggregateFunctions = false;
        for (SelectItem c : selectBody.getSelectItems()) {
            if (c instanceof AllColumns) {
                allColumns = true;
                break;
            } else if (c instanceof AllTableColumns) {
                AllTableColumns allTableColumns = (AllTableColumns) c;
                TableRef ref = TableRef.buildFrom(allTableColumns.getTable(), defaultTableSpace);
                if (!joinPresent && ref.tableAlias.equals(mainTable.tableAlias)) {
                    // select a.*  FROM table a
                    allColumns = true;
                } else {
                    // select a.*, b.* FROM table a JOIN table b
                    joins.get(ref.tableAlias).allColumns = true;
                }
            } else if (c instanceof SelectExpressionItem) {
                SelectExpressionItem se = (SelectExpressionItem) c;
                if (isAggregateFunction(se.getExpression())) {
                    containsAggregateFunctions = true;
                }
                if (!joinPresent) {
                    joins.get(mainTable.tableAlias).selectItems.add(c);
                } else {
                    ColumnReferencesDiscovery discoverMainTableAlias = discoverMainTableAlias(se.getExpression());
                    String mainTableAliasForItem = discoverMainTableAlias.getMainTableAlias();
                    if (discoverMainTableAlias.isContainsMixedAliases()) {
                        throw new StatementExecutionException("unsupported single SELECT ITEM with mixed aliases: " + c);
                    }
                    if (mainTableAliasForItem == null) {
                        mainTableAliasForItem = mainTable.tableAlias;
                    }
                    joins.get(mainTableAliasForItem).selectItems.add(c);
                }
            } else {
                throw new StatementExecutionException("unsupported SELECT ITEM type: " + c);
            }
        }

        if (allColumns) {
            mainTableProjection = Projection.IDENTITY(table.columnNames, table.columns);
            for (Map.Entry<String, JoinSupport> join : joins.entrySet()) {
                JoinSupport support = join.getValue();
                support.projection = Projection.IDENTITY(support.table.columnNames, support.table.columns);
                support.allColumns = true;
            }
        } else {
            if (!joinPresent) {
                mainTableProjection = new SQLProjection(table, mainTableAlias, selectBody.getSelectItems());
            } else {
                for (JoinSupport support : joins.values()) {
                    if (support.allColumns) {
                        support.projection = Projection.IDENTITY(support.table.columnNames, support.table.columns);
                    } else {
                        support.projection = new SQLProjection(support.table, support.tableRef.tableAlias, support.selectItems);
                    }
                }
                mainTableProjection = joins.get(mainTableAlias).projection;
            }
        }
        if (scan) {
            if (!joinPresent) {
                SQLRecordPredicate where = selectBody.getWhere() != null ? new SQLRecordPredicate(table, mainTableAlias, selectBody.getWhere()) : null;
                if (where != null) {
                    discoverIndexOperations(selectBody.getWhere(), table, mainTableAlias, where, tableSpaceManager);
                }

                Aggregator aggregator = null;
                ScanLimitsImpl scanLimits = null;
                if (containsAggregateFunctions || (selectBody.getGroupByColumnReferences() != null && !selectBody.getGroupByColumnReferences().isEmpty())) {
                    aggregator = new SQLAggregator(selectBody.getSelectItems(), selectBody.getGroupByColumnReferences(), manager.getRecordSetFactory());
                }

                TupleComparator comparatorOnScan = null;
                TupleComparator comparatorOnPlan = null;
                if (selectBody.getOrderByElements() != null && !selectBody.getOrderByElements().isEmpty()) {
                    if (aggregator != null) {
                        comparatorOnPlan = SingleColumnSQLTupleComparator.make(mainTableAlias,
                            selectBody.getOrderByElements(), null);
                    } else {
                        comparatorOnScan = SingleColumnSQLTupleComparator.make(mainTableAlias,
                            selectBody.getOrderByElements(),
                            table.primaryKey);
                    }
                }

                Limit limit = selectBody.getLimit();
                Top top = selectBody.getTop();
                if (limit != null && top != null) {
                    throw new StatementExecutionException("LIMIT and TOP cannot be used on the same query");
                }
                if (limit != null) {
                    if (limit.isLimitAll() || limit.isLimitNull() || limit.getOffset() instanceof JdbcParameter) {
                        throw new StatementExecutionException("Invalid LIMIT clause (limit=" + limit + ")");
                    }
                    if (maxRows > 0 && limit.getRowCount() instanceof JdbcParameter) {
                        throw new StatementExecutionException("Invalid LIMIT clause (limit=" + limit + ") and JDBC setMaxRows=" + maxRows);
                    }
                    int rowCount;
                    int rowCountJdbcParameter = -1;
                    if (limit.getRowCount() instanceof JdbcParameter) {
                        rowCount = -1;
                        rowCountJdbcParameter = ((JdbcParameter) limit.getRowCount()).getIndex() - 1;
                    } else {
                        rowCount = ((Number) resolveValue(limit.getRowCount(), false)).intValue();
                    }
                    int offset = limit.getOffset() != null ? ((Number) resolveValue(limit.getOffset(), false)).intValue() : 0;
                    scanLimits = new ScanLimitsImpl(rowCount, offset, rowCountJdbcParameter + 1);
                } else if (top != null) {
                    if (top.isPercentage() || top.getExpression() == null) {
                        throw new StatementExecutionException("Invalid TOP clause (top=" + top + ")");
                    }
                    try {
                        int rowCount = Integer.parseInt(resolveValue(top.getExpression(), false) + "");
                        scanLimits = new ScanLimitsImpl(rowCount, 0);
                    } catch (NumberFormatException error) {
                        throw new StatementExecutionException("Invalid TOP clause: " + error, error);
                    }
                }
                if (maxRows > 0) {
                    if (scanLimits == null) {
                        scanLimits = new ScanLimitsImpl(maxRows, 0);
                    } else if (scanLimits.getMaxRows() <= 0 || scanLimits.getMaxRows() > maxRows) {
                        scanLimits = new ScanLimitsImpl(maxRows, scanLimits.getOffset());
                    }
                }

                ScanLimitsImpl limitsOnScan = null;
                ScanLimitsImpl limitsOnPlan = null;
                if (aggregator != null) {
                    limitsOnPlan = scanLimits;
                } else {
                    limitsOnScan = scanLimits;
                }
                try {
                    ScanStatement statement = new ScanStatement(tableSpace, mainTable.tableName, mainTableProjection, where, comparatorOnScan, limitsOnScan);
                    return ExecutionPlan.make(statement, aggregator, limitsOnPlan, comparatorOnPlan);
                } catch (IllegalArgumentException err) {
                    throw new StatementExecutionException(err);
                }
            } else {
                if (containsAggregateFunctions || (selectBody.getGroupByColumnReferences() != null && !selectBody.getGroupByColumnReferences().isEmpty())) {
                    throw new StatementExecutionException("AGGREGATEs are not yet supported with JOIN");
                }
                Limit limit = selectBody.getLimit();
                Top top = selectBody.getTop();
                if (limit != null && top != null) {
                    throw new StatementExecutionException("LIMIT and TOP cannot be used on the same query");
                }
                ScanLimitsImpl scanLimits = null;
                if (limit != null) {
                    if (limit.isLimitAll() || limit.isLimitNull() || limit.getOffset() instanceof JdbcParameter) {
                        throw new StatementExecutionException("Invalid LIMIT clause (limit=" + limit + ")");
                    }
                    if (maxRows > 0 && limit.getRowCount() instanceof JdbcParameter) {
                        throw new StatementExecutionException("Invalid LIMIT clause (limit=" + limit + ") and JDBC setMaxRows=" + maxRows);
                    }
                    int rowCount;
                    int rowCountJdbcParameter = -1;
                    if (limit.getRowCount() instanceof JdbcParameter) {
                        rowCount = -1;
                        rowCountJdbcParameter = ((JdbcParameter) limit.getRowCount()).getIndex() - 1;
                    } else {
                        rowCount = ((Number) resolveValue(limit.getRowCount(), false)).intValue();
                    }
                    int offset = limit.getOffset() != null ? ((Number) resolveValue(limit.getOffset(), false)).intValue() : 0;
                    scanLimits = new ScanLimitsImpl(rowCount, offset, rowCountJdbcParameter + 1);

                } else if (top != null) {
                    if (top.isPercentage() || top.getExpression() == null) {
                        throw new StatementExecutionException("Invalid TOP clause");
                    }
                    try {
                        int rowCount = Integer.parseInt(resolveValue(top.getExpression(), false) + "");
                        scanLimits = new ScanLimitsImpl(rowCount, 0);
                    } catch (NumberFormatException error) {
                        throw new StatementExecutionException("Invalid TOP clause: " + error, error);
                    }
                }
                if (maxRows > 0) {
                    if (scanLimits == null) {
                        scanLimits = new ScanLimitsImpl(maxRows, 0);
                    } else if (scanLimits.getMaxRows() <= 0 || scanLimits.getMaxRows() > maxRows) {
                        scanLimits = new ScanLimitsImpl(maxRows, scanLimits.getOffset());
                    }
                }

                List<ColumnReferencesDiscovery> conditionsOnJoinedResult = new ArrayList<>();
                List<ScanStatement> scans = new ArrayList<>();
                for (Map.Entry<String, JoinSupport> join : joins.entrySet()) {
                    String alias = join.getKey();
                    JoinSupport joinSupport = join.getValue();
                    Expression collectedConditionsForAlias = collectConditionsForAlias(alias, selectBody.getWhere(),
                        conditionsOnJoinedResult, mainTableAlias);
                    LOG.severe("Collected WHERE for alias " + alias + ": " + collectedConditionsForAlias);

                    if (collectedConditionsForAlias == null) {
                        joinSupport.predicate = null;
                    } else {
                        joinSupport.predicate = new SQLRecordPredicate(
                            join.getValue().table, alias, collectedConditionsForAlias);
                    }

                }
                for (Join join : selectBody.getJoins()) {
                    if (join.getOnExpression() != null) {
                        ColumnReferencesDiscovery discoverMainTableAliasForJoinCondition
                            = discoverMainTableAlias(join.getOnExpression());
                        conditionsOnJoinedResult.add(discoverMainTableAliasForJoinCondition);
                        LOG.severe("Collected ON-condition on final JOIN result: " + join.getOnExpression());
                    }
                }
                for (ColumnReferencesDiscovery e : conditionsOnJoinedResult) {
                    LOG.severe("Collected WHERE on final JOIN result: " + e.getExpression());
                    for (Map.Entry<String, List<net.sf.jsqlparser.schema.Column>> entry : e.getColumnsByTable().entrySet()) {
                        String tableAlias = entry.getKey();
                        List<net.sf.jsqlparser.schema.Column> filteredColumnsOnJoin = entry.getValue();
                        LOG.severe("for  TABLE " + tableAlias + " we need to load " + filteredColumnsOnJoin);
                        JoinSupport support = joins.get(tableAlias);
                        if (support == null) {
                            throw new StatementExecutionException("invalid table alias " + tableAlias);
                        }
                        if (!support.allColumns) {
                            for (net.sf.jsqlparser.schema.Column c : filteredColumnsOnJoin) {
                                support.selectItems.add(new SelectExpressionItem(c));
                            }
                            support.projection = new SQLProjection(support.table, support.tableRef.tableAlias, support.selectItems);
                        }
                    }

                }
                Map<String, Table> tables = new HashMap<>();
                for (Map.Entry<String, JoinSupport> join : joins.entrySet()) {
                    JoinSupport joinSupport = join.getValue();
                    tables.put(join.getKey(), joinSupport.table);
                    ScanStatement statement = new ScanStatement(tableSpace,
                        joinSupport.table.name,
                        joinSupport.projection, joinSupport.predicate, null, null);
                    scans.add(statement);
                }
                TuplePredicate joinFilter = null;
                if (!conditionsOnJoinedResult.isEmpty()) {
                    joinFilter = new SQLRecordPredicate(null, null, composeAndExpression(conditionsOnJoinedResult));
                }
                Projection joinProjection = null;
                if (!allColumns) {
                    joinProjection = new SQLProjection(tableSpace, tables, selectBody.getSelectItems());
                }
                TupleComparator comparatorOnPlan = null;
                if (selectBody.getOrderByElements() != null && !selectBody.getOrderByElements().isEmpty()) {
                    comparatorOnPlan = SingleColumnSQLTupleComparator.make(mainTableAlias,
                        selectBody.getOrderByElements(), null);
                }

                try {
                    return ExecutionPlan.joinedScan(scans, joinFilter, joinProjection, scanLimits, comparatorOnPlan);
                } catch (IllegalArgumentException err) {
                    throw new StatementExecutionException(err);
                }

            }
        } else {
            if (selectBody.getWhere() == null) {
                throw new StatementExecutionException("unsupported GET without WHERE");
            }
            if (joinPresent) {
                throw new StatementExecutionException("unsupported GET with JOIN");
            }

            // SELECT * FROM WHERE KEY=? AND ....
            SQLRecordKeyFunction keyFunction = findPrimaryKeyIndexSeek(selectBody.getWhere(), table, mainTableAlias);
            if (keyFunction == null || !keyFunction.isFullPrimaryKey()) {
                throw new StatementExecutionException("unsupported GET not on PK, bad where clause: " + selectBody.getWhere() + " (" + selectBody.getWhere().getClass() + ")");
            }
            Predicate where = buildSimplePredicate(selectBody.getWhere(), table, mainTableAlias);
            try {
                return ExecutionPlan.simple(new GetStatement(tableSpace, mainTable.tableName, keyFunction, where, false));
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        }
    }

    private static IndexOperation findSecondaryIndexOperation(AbstractIndexManager index, Expression where, Table table) throws StatementExecutionException {
        IndexOperation secondaryIndexOperation = null;
        String[] columnsToMatch = index.getColumnNames();
        SQLRecordKeyFunction indexSeekFunction = findIndexAccess(where, columnsToMatch,
            index.getIndex(),
            table.name,
            EqualsTo.class
        );
        if (indexSeekFunction != null) {
            if (indexSeekFunction.isFullPrimaryKey()) {
                secondaryIndexOperation = new SecondaryIndexSeek(index.getIndexName(), columnsToMatch, indexSeekFunction);
            } else {
                secondaryIndexOperation = new SecondaryIndexPrefixScan(index.getIndexName(), columnsToMatch, indexSeekFunction);
            }
        } else {
            SQLRecordKeyFunction rangeMin = findIndexAccess(where, columnsToMatch,
                index.getIndex(),
                table.name, GreaterThanEquals.class
            );
            if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                rangeMin = null;

            }
            if (rangeMin == null) {
                rangeMin = findIndexAccess(where, columnsToMatch,
                    index.getIndex(),
                    table.name, GreaterThan.class
                );
                if (rangeMin != null && !rangeMin.isFullPrimaryKey()) {
                    rangeMin = null;

                }
            }

            SQLRecordKeyFunction rangeMax = findIndexAccess(where, columnsToMatch,
                index.getIndex(),
                table.name, MinorThanEquals.class
            );
            if (rangeMax != null && !rangeMax.isFullPrimaryKey()) {
                rangeMax = null;

            }
            if (rangeMax == null) {
                rangeMax = findIndexAccess(where, columnsToMatch,
                    index.getIndex(),
                    table.name, MinorThan.class
                );
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
    private static final Logger LOG = Logger.getLogger(SQLPlanner.class.getName());

    private Statement buildExecuteStatement(String defaultTableSpace, Execute execute) throws StatementExecutionException {
        switch (execute.getName().toUpperCase()) {
            case "BEGINTRANSACTION": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName)");
                }
                return new BeginTransactionStatement(tableSpaceName.toString());
            }
            case "COMMITTRANSACTION": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1), true);
                if (transactionId == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                try {
                    return new CommitTransactionStatement(tableSpaceName.toString(), Long.parseLong(transactionId.toString()));
                } catch (NumberFormatException err) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }

            }
            case "ROLLBACKTRANSACTION": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1), true);
                if (transactionId == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                try {
                    return new RollbackTransactionStatement(tableSpaceName.toString(), Long.parseLong(transactionId.toString()));
                } catch (NumberFormatException err) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
            }
            case "CREATETABLESPACE": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() < 1) {
                    throw new StatementExecutionException("CREATETABLESPACE syntax (EXECUTE CREATETABLESPACE tableSpaceName ['leader:LEADERID'],['wait:TIMEOUT'] )");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0), true);
                String leader = null;
                Set<String> replica = new HashSet<>();
                int expectedreplicacount = 1;
                long maxleaderinactivitytime = 0;
                int wait = 0;
                for (int i = 1; i < execute.getExprList().getExpressions().size(); i++) {
                    String property = (String) resolveValue(execute.getExprList().getExpressions().get(i), true);
                    int colon = property.indexOf(':');
                    if (colon <= 0) {
                        throw new StatementExecutionException("bad property " + property + " in " + execute + " statement");
                    }
                    String pName = property.substring(0, colon);
                    String value = property.substring(colon + 1);
                    switch (pName.toLowerCase()) {
                        case "leader":
                            leader = value;
                            break;
                        case "replica":
                            replica = Arrays.asList(value.split(",")).stream().map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
                            break;
                        case "wait":
                            wait = Integer.parseInt(value);
                            break;
                        case "expectedreplicacount":
                            try {
                                expectedreplicacount = Integer.parseInt(value.trim());
                                if (expectedreplicacount <= 0) {
                                    throw new StatementExecutionException("invalid expectedreplicacount " + value + " must be positive");
                                }
                            } catch (NumberFormatException err) {
                                throw new StatementExecutionException("invalid expectedreplicacount " + value + ": " + err);
                            }
                            break;
                        case "maxleaderinactivitytime":
                            try {
                                maxleaderinactivitytime = Long.parseLong(value.trim());
                                if (maxleaderinactivitytime < 0) {
                                    throw new StatementExecutionException("invalid maxleaderinactivitytime " + value + " must be positive or zero");
                                }
                            } catch (NumberFormatException err) {
                                throw new StatementExecutionException("invalid maxleaderinactivitytime " + value + ": " + err);
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
                return new CreateTableSpaceStatement(tableSpaceName + "", replica, leader, expectedreplicacount, wait, maxleaderinactivitytime);
            }
            case "ALTERTABLESPACE": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() < 2) {
                    throw new StatementExecutionException("ALTERTABLESPACE syntax (EXECUTE ALTERTABLESPACE tableSpaceName,'property:value','property2:value2')");
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
                            throw new StatementExecutionException("bad property " + property + " in " + execute + " statement");
                        }
                        String pName = property.substring(0, colon);
                        String value = property.substring(colon + 1);
                        switch (pName.toLowerCase()) {
                            case "leader":
                                leader = value;
                                break;
                            case "replica":
                                replica = Arrays.asList(value.split(",")).stream().map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
                                break;
                            case "expectedreplicacount":
                                try {
                                    expectedreplicacount = Integer.parseInt(value.trim());
                                    if (expectedreplicacount <= 0) {
                                        throw new StatementExecutionException("invalid expectedreplicacount " + value + " must be positive");
                                    }
                                } catch (NumberFormatException err) {
                                    throw new StatementExecutionException("invalid expectedreplicacount " + value + ": " + err);
                                }
                                break;
                            case "maxleaderinactivitytime":
                                try {
                                    maxleaderinactivitytime = Long.parseLong(value.trim());
                                    if (maxleaderinactivitytime < 0) {
                                        throw new StatementExecutionException("invalid maxleaderinactivitytime " + value + " must be positive or zero");
                                    }
                                } catch (NumberFormatException err) {
                                    throw new StatementExecutionException("invalid maxleaderinactivitytime " + value + ": " + err);
                                }
                                break;
                            default:
                                throw new StatementExecutionException("bad property " + pName);
                        }
                    }
                    return new AlterTableSpaceStatement(tableSpaceName + "", replica, leader, expectedreplicacount, maxleaderinactivitytime);
                } catch (MetadataStorageManagerException err) {
                    throw new StatementExecutionException(err);
                }
            }
            case "DROPTABLESPACE": {
                if (execute.getExprList() == null || execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException("DROPTABLESPACE syntax (EXECUTE DROPTABLESPACE tableSpaceName)");
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
                    throw new StatementExecutionException("RENAMETABLE syntax (EXECUTE RENAMETABLE 'tableSpaceName','tablename','nametablename')");
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
                        null, oldTableName, tableSpaceName, newTableName);
                } catch (MetadataStorageManagerException err) {
                    throw new StatementExecutionException(err);
                }
            }
            default:
                throw new StatementExecutionException("Unsupported command " + execute.getName());
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
        String tableName = alter.getTable().getName();
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
                    Column newColumn = Column.column(cl.getColumnName(), sqlDataTypeToColumnType(
                        cl.getColDataType().getDataType(),
                        cl.getColDataType().getArgumentsStringList()
                    ));
                    addColumns.add(newColumn);
                }
            }
            break;
            case DROP:
                dropColumns.add(alterExpression.getColumnName());
                break;
            case MODIFY: {
                TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
                if (tableSpaceManager == null) {
                    throw new StatementExecutionException("bad tablespace '" + tableSpace + "'");
                }
                AbstractTableManager tableManager = tableSpaceManager.getTableManager(tableName);
                if (tableManager == null) {
                    throw new StatementExecutionException("bad table " + tableName + " in tablespace '" + tableSpace + "'");
                }
                Table table = tableManager.getTable();
                List<AlterExpression.ColumnDataType> cols = alterExpression.getColDataTypeList();
                for (AlterExpression.ColumnDataType cl : cols) {
                    String columnName = cl.getColumnName().toLowerCase();
                    Column oldColumn = table.getColumn(columnName);
                    if (oldColumn == null) {
                        throw new StatementExecutionException("bad column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                    }
                    Map<String, AbstractIndexManager> indexes = tableSpaceManager.getIndexesOnTable(tableName);
                    if (indexes != null) {
                        for (AbstractIndexManager am : indexes.values()) {
                            for (String indexedColumn : am.getColumnNames()) {
                                if (indexedColumn.equalsIgnoreCase(oldColumn.name)) {
                                    throw new StatementExecutionException(
                                        "cannot alter indexed " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "',"
                                        + "index name is " + am.getIndexName());
                                }
                            }
                        }
                    }
                    int newType = sqlDataTypeToColumnType(
                        cl.getColDataType().getDataType(),
                        cl.getColDataType().getArgumentsStringList()
                    );

                    if (oldColumn.type != newType) {
                        throw new StatementExecutionException("cannot change datatype to " + cl.getColDataType().getDataType()
                            + " for column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                    }
                    List<String> columnSpecs = decodeColumnSpecs(cl.getColumnSpecs());
                    if (table.isPrimaryKeyColumn(columnName)) {
                        boolean new_auto_increment = decodeAutoIncrement(columnSpecs);
                        if (new_auto_increment && table.primaryKey.length > 1) {
                            throw new StatementExecutionException("cannot add auto_increment flag to " + cl.getColDataType().getDataType()
                                + " for column " + columnName + " in table " + tableName + " in tablespace '" + tableSpace + "'");
                        }
                        if (table.auto_increment != new_auto_increment) {
                            changeAutoIncrement = new_auto_increment;
                        }
                    }
                    String renameTo = decodeRenameTo(columnSpecs);
                    if (renameTo != null) {
                        columnName = renameTo;
                    }
                    Column newColumnDef = Column.column(columnName, newType, oldColumn.serialPosition);
                    modifyColumns.add(newColumnDef);
                }
            }
            break;
            default:
                throw new StatementExecutionException("supported alter operation '" + alter + "'");
        }
        return new AlterTableStatement(addColumns, modifyColumns, dropColumns,
            changeAutoIncrement, tableName, tableSpace, null);
    }

    private Statement buildDropStatement(String defaultTableSpace, Drop drop) throws StatementExecutionException {
        if (drop.getType().equalsIgnoreCase("table")) {
            if (drop.getName() == null) {
                throw new StatementExecutionException("missing table name");
            }

            String tableSpace = drop.getName().getSchemaName();
            if (tableSpace == null) {
                tableSpace = defaultTableSpace;
            }
            String tableName = drop.getName().getName();
            return new DropTableStatement(tableSpace, tableName, drop.isIfExists());
        }
        if (drop.getType().equalsIgnoreCase("index")) {
            if (drop.getName() == null) {
                throw new StatementExecutionException("missing index name");
            }
            String tableSpace = drop.getName().getSchemaName();
            if (tableSpace == null) {
                tableSpace = defaultTableSpace;
            }
            String indexName = drop.getName().getName();
            return new DropIndexStatement(tableSpace, indexName, drop.isIfExists());
        }
        throw new StatementExecutionException("only DROP TABLE and TABLESPACE is supported, drop type=" + drop.getType() + " is not implemented");
    }

    private Statement buildTruncateStatement(String defaultTableSpace, Truncate truncate) throws StatementExecutionException {

        if (truncate.getTable() == null) {
            throw new StatementExecutionException("missing table name");
        }

        String tableSpace = truncate.getTable().getSchemaName();
        if (tableSpace == null) {
            tableSpace = defaultTableSpace;
        }
        String tableName = truncate.getTable().getName();
        return new TruncateTableStatement(tableSpace, tableName);
    }

    private boolean isAggregateFunction(Expression expression) throws StatementExecutionException {
        if (!(expression instanceof Function)) {
            return false;
        }
        Function function = (Function) expression;
        String name = function.getName().toLowerCase();
        if (BuiltinFunctions.isAggregateFunction(function.getName())) {
            return true;
        }
        if (BuiltinFunctions.isScalarFunction(function.getName())) {
            return false;
        }
        throw new StatementExecutionException("unsupported function " + name);
    }

    private void verifyJdbcParametersIndexes(net.sf.jsqlparser.statement.Statement stmt) {
        JdbcQueryRewriter assigner = new JdbcQueryRewriter();
        stmt.accept(assigner);

    }

}
