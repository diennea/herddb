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

import herddb.core.DBManager;
import herddb.core.TableManager;
import herddb.core.TableSpaceManager;
import herddb.model.Aggregator;
import herddb.model.AutoIncrementPrimaryKeyRecordFunction;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.DataScannerException;
import herddb.model.ExecutionPlan;
import herddb.model.Predicate;
import herddb.model.PrimaryKeyIndexSeekPredicate;
import herddb.model.Projection;
import herddb.model.RecordFunction;
import herddb.model.ScanLimits;
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TupleComparator;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.GetStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.model.commands.ScanStatement;
import herddb.model.commands.UpdateStatement;
import herddb.sql.functions.BuiltinFunctions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.statement.update.Update;

/**
 * Translates SQL to Internal API
 *
 * @author enrico.olivelli
 */
public class SQLTranslator {

    private final DBManager manager;
    private final PlansCache cache;

    public void clearCache() {
        cache.clear();
    }

    public SQLTranslator(DBManager manager) {
        this.manager = manager;
        this.cache = new PlansCache();
    }

    public TranslatedQuery translate(String query, List<Object> parameters, boolean scan, boolean allowCache) throws StatementExecutionException {
        String cacheKey = "scan:" + scan + ",query:" + query;
        if (allowCache) {
            ExecutionPlan cached = cache.get(cacheKey);
            if (cached != null) {
                return new TranslatedQuery(cached, new SQLStatementEvaluationContext(query, parameters));
            }
        }
        try {
            ExecutionPlan result;
            net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(query);
            if (stmt instanceof CreateTable) {
                result = ExecutionPlan.simple(buildCreateTableStatement((CreateTable) stmt));
            } else if (stmt instanceof Insert) {
                result = ExecutionPlan.simple(buildInsertStatement((Insert) stmt));
            } else if (stmt instanceof Delete) {
                result = ExecutionPlan.simple(buildDeleteStatement((Delete) stmt));
            } else if (stmt instanceof Update) {
                result = ExecutionPlan.simple(buildUpdateStatement((Update) stmt));
            } else if (stmt instanceof Select) {
                result = buildSelectStatement((Select) stmt, scan);
            } else if (stmt instanceof Execute) {
                result = ExecutionPlan.simple(buildExecuteStatement((Execute) stmt));
            } else {
                throw new StatementExecutionException("unable to parse query " + query + ", type " + stmt.getClass());
            }
            if (allowCache) {
                cache.put(cacheKey, result);
            }
            return new TranslatedQuery(result, new SQLStatementEvaluationContext(query, parameters));
        } catch (JSQLParserException | DataScannerException err) {
            throw new StatementExecutionException("unable to parse query " + query, err);
        }

    }

    private Statement buildCreateTableStatement(CreateTable s) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName().toLowerCase();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        boolean foundPk = false;
        List<String> allColumnNames = new ArrayList<>();
        Table.Builder tablebuilder = Table.builder().name(tableName).tablespace(tableSpace);
        for (ColumnDefinition cf : s.getColumnDefinitions()) {
            int type;
            allColumnNames.add(cf.getColumnName());
            switch (cf.getColDataType().getDataType().toLowerCase()) {
                case "string":
                case "varchar":
                case "nvarchar":
                case "nvarchar2":
                case "nclob":
                case "clob":
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
                default:
                    throw new StatementExecutionException("bad type " + cf.getColDataType().getDataType());
            }
            tablebuilder.column(cf.getColumnName(), type);

            if (cf.getColumnSpecStrings() != null) {
                List<String> columnSpecs = cf.getColumnSpecStrings().stream().map(String::toUpperCase).collect(Collectors.toList());
                if (columnSpecs.contains("PRIMARY")) {
                    foundPk = true;
                    boolean auto_increment = columnSpecs.contains("AUTO_INCREMENT");
                    tablebuilder.primaryKey(cf.getColumnName(), auto_increment);
                }
            }
        }

        if (s.getIndexes() != null) {
            for (Index index : s.getIndexes()) {
                if (index.getType().equalsIgnoreCase("PRIMARY KEY")) {
                    for (String n : index.getColumnsNames()) {
                        tablebuilder.primaryKey(n);
                        foundPk = true;
                    }
                }
            }
        }
        if (!foundPk) {
            // HEAP TABLE
            allColumnNames.forEach(tablebuilder::primaryKey);
        }

        try {
            CreateTableStatement statement = new CreateTableStatement(tablebuilder.build());
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad table definition: " + err.getMessage(), err);
        }
    }

    private Statement buildInsertStatement(Insert s) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName().toLowerCase();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here");
        }
        TableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablepace " + tableSpace);
        }
        Table table = tableManager.getTable();

        List<Expression> keyValueExpression = new ArrayList<>();
        List<String> keyExpressionToColumn = new ArrayList<>();

        List<Expression> valuesExpressions = new ArrayList<>();
        List<net.sf.jsqlparser.schema.Column> valuesColumns = new ArrayList<>();

        int index = 0;
        int countJdbcParametersBeforeKey = -1;
        int countJdbcParameters = 0;
        ExpressionList list = (ExpressionList) s.getItemsList();
        if (s.getColumns() != null) {
            for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
                Column column = table.getColumn(c.getColumnName());
                if (column == null) {
                    throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablepace " + tableSpace);
                }
                Expression expression = list.getExpressions().get(index++);
                if (table.isPrimaryKeyColumn(column.name)) {
                    keyExpressionToColumn.add(column.name);
                    keyValueExpression.add(expression);
                    if (countJdbcParametersBeforeKey < 0) {
                        countJdbcParametersBeforeKey = countJdbcParameters;
                    }
                }
                valuesColumns.add(c);
                valuesExpressions.add(expression);
                countJdbcParameters += countJdbcParametersUsedByExpression(expression);
            }
        } else {
            for (Column column : table.columns) {

                Expression expression = list.getExpressions().get(index++);
                if (table.isPrimaryKeyColumn(column.name)) {
                    keyExpressionToColumn.add(column.name);
                    keyValueExpression.add(expression);
                    if (countJdbcParametersBeforeKey < 0) {
                        countJdbcParametersBeforeKey = countJdbcParameters;
                    }
                }
                valuesColumns.add(new net.sf.jsqlparser.schema.Column(column.name));
                valuesExpressions.add(expression);
                countJdbcParameters += countJdbcParametersUsedByExpression(expression);
            }
        }
        if (countJdbcParametersBeforeKey < 0) {
            countJdbcParametersBeforeKey = 0;
        }

        RecordFunction keyfunction;
        if (keyValueExpression.isEmpty() && table.auto_increment) {
            keyfunction = new AutoIncrementPrimaryKeyRecordFunction();
        } else {
            if (keyValueExpression.size() != table.primaryKey.length) {
                throw new StatementExecutionException("you must set a value for the primary key");
            }
            keyfunction = new SQLRecordKeyFunction(table, keyExpressionToColumn, keyValueExpression, countJdbcParametersBeforeKey);
        }
        RecordFunction valuesfunction = new SQLRecordFunction(table, valuesColumns, valuesExpressions, 0);

        try {
            return new InsertStatement(tableSpace, tableName, keyfunction, valuesfunction);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private Statement buildDeleteStatement(Delete s) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) s.getTable();
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName().toLowerCase();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here");
        }
        TableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablepace " + tableSpace);
        }
        Table table = tableManager.getTable();

        SQLRecordKeyFunction keyFunction = findPrimaryKeyIndexSeek(s.getWhere(), table, new AtomicInteger());
        if (keyFunction == null) {
            // DELETE FROM TABLE WHERE KEY=? AND ....
            throw new StatementExecutionException("unsupported where " + s.getWhere());
        }

        Predicate where = buildPredicate(s.getWhere(), table, 0);
        try {
            return new DeleteStatement(tableSpace, tableName, keyFunction, where);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private Statement buildUpdateStatement(Update s) throws StatementExecutionException {
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) s.getTables().get(0);
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName().toLowerCase();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here");
        }
        TableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablepace " + tableSpace);
        }
        Table table = tableManager.getTable();
        for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
            Column column = table.getColumn(c.getColumnName());
            if (column == null) {
                throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablepace " + tableSpace);
            }
        }

        RecordFunction function = new SQLRecordFunction(table, s.getColumns(), s.getExpressions(), 0);
        int setClauseParamters = (int) s.getExpressions().stream().filter(e -> e instanceof JdbcParameter).count();
        RecordFunction keyFunction = findPrimaryKeyIndexSeek(s.getWhere(), table, new AtomicInteger(setClauseParamters));
        if (keyFunction == null) {
            // UPDATE TABLE SET XXX WHERE KEY=? AND ....
            throw new StatementExecutionException("unsupported where " + s.getWhere());
        }

        Predicate where = buildPredicate(s.getWhere(), table, setClauseParamters);

        try {
            return new UpdateStatement(tableSpace, tableName, keyFunction, function, where);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    private Predicate buildPredicate(Expression where, Table table, int parameterPos) {
        if (where instanceof EqualsTo) {
            // surely this is the only predicate on the PK, we can skip it
            return null;
        }
        return new SQLRecordPredicate(table, where, parameterPos);

    }

    private int countJdbcParametersUsedByExpression(Expression e) {
        if (e instanceof net.sf.jsqlparser.schema.Column
                || e instanceof StringValue
                || e instanceof LongValue) {
            return 0;
        }
        if (e instanceof BinaryExpression) {
            BinaryExpression bi = (BinaryExpression) e;
            return countJdbcParametersUsedByExpression(bi.getLeftExpression()) + countJdbcParametersUsedByExpression(bi.getRightExpression());
        }
        if (e instanceof JdbcParameter) {
            return 1;
        }
        if (e instanceof Parenthesis) {
            return countJdbcParametersUsedByExpression(((Parenthesis) e).getExpression());
        }
        if (e instanceof Function) {
            int count = 0;
            Function f = (Function) e;
            if (f.getParameters() != null && f.getParameters().getExpressions() != null) {
                for (Expression ex : f.getParameters().getExpressions()) {
                    count += countJdbcParametersUsedByExpression(ex);
                }
            }
            return count;
        }
        throw new UnsupportedOperationException("unsupported expression type " + e.getClass() + " (" + e + ")");
    }

    private Expression findColumnEqualsTo(Expression where, String columnName, Table table, AtomicInteger jdbcParameterPos) throws StatementExecutionException {
        if (where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            Expression keyOnLeft = validatePrimaryKeyEqualsToExpression(and.getLeftExpression(), columnName, table, jdbcParameterPos);
            if (keyOnLeft != null) {
                return keyOnLeft;
            }
            int countJdbcParametersUsedByLeft = countJdbcParametersUsedByExpression(and.getLeftExpression());

            Expression keyOnRight = validatePrimaryKeyEqualsToExpression(and.getRightExpression(), columnName, table, new AtomicInteger(jdbcParameterPos.get() + countJdbcParametersUsedByLeft));
            if (keyOnRight != null) {
                return keyOnRight;
            }
        } else if (where instanceof EqualsTo) {
            Expression keyDirect = validatePrimaryKeyEqualsToExpression(where, columnName, table, jdbcParameterPos);
            if (keyDirect != null) {
                return keyDirect;
            }
        }

        return null;
    }

    private SQLRecordKeyFunction findPrimaryKeyIndexSeek(Expression where, Table table, AtomicInteger jdbcParameterPos) throws StatementExecutionException {
        if (table.primaryKey.length == 1) {
            Expression key = findColumnEqualsTo(where, table.primaryKey[0], table, jdbcParameterPos);
            if (key == null) {
                return null;
            }
            return new SQLRecordKeyFunction(table, Arrays.asList(table.primaryKey[0]), Collections.singletonList(key), jdbcParameterPos.get());
        }

        List<Expression> expressions = new ArrayList<>();
        for (String pk : table.primaryKey) {
            Expression condition = findColumnEqualsTo(where, pk, table, jdbcParameterPos);
            if (condition == null) {
                return null;
            }
            expressions.add(condition);
        }
        return new SQLRecordKeyFunction(table, Arrays.asList(table.primaryKey), expressions, jdbcParameterPos.get());

    }

    private Object resolveValue(Expression expression) throws StatementExecutionException {
        if (expression instanceof JdbcParameter) {
            throw new StatementExecutionException("jdbcparameter expression not usable in this query");
        } else if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        } else if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        } else {
            throw new StatementExecutionException("unsupported value type " + expression.getClass());
        }
    }

    private Expression validatePrimaryKeyEqualsToExpression(Expression testExpression, String columnName, Table table1, AtomicInteger jdbcParameterPos) throws StatementExecutionException {
        Expression result = null;
        if (testExpression instanceof EqualsTo) {
            EqualsTo e = (EqualsTo) testExpression;
            if (e.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) e.getLeftExpression();
                if (columnName.equals(column.getColumnName())) {
                    return e.getRightExpression();
                }
            } else if (e.getLeftExpression() instanceof AndExpression) {
                result = findColumnEqualsTo((AndExpression) e.getLeftExpression(), columnName, table1, jdbcParameterPos);
                if (result != null) {
                    return result;
                }
            } else if (e.getRightExpression() instanceof AndExpression) {
                result = findColumnEqualsTo((AndExpression) e.getRightExpression(), columnName, table1, jdbcParameterPos);
                if (result != null) {
                    return result;
                }
            }
        }
        return result;
    }

    private ExecutionPlan buildSelectStatement(Select s, boolean scan) throws StatementExecutionException, DataScannerException {
        PlainSelect selectBody = (PlainSelect) s.getSelectBody();
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) selectBody.getFromItem();
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName().toLowerCase();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        TableSpaceManager tableSpaceManager = manager.getTableSpaceManager(tableSpace);
        if (tableSpaceManager == null) {
            throw new StatementExecutionException("no such tablespace " + tableSpace + " here");
        }
        TableManager tableManager = tableSpaceManager.getTableManager(tableName);
        if (tableManager == null) {
            throw new StatementExecutionException("no such table " + tableName + " in tablepace " + tableSpace);
        }
        int countJdbcParameters = 0;
        Table table = tableManager.getTable();
        boolean allColumns = false;
        boolean containsAggregateFunctions = false;
        for (SelectItem c : selectBody.getSelectItems()) {

            if (c instanceof AllColumns) {
                allColumns = true;
                break;
            } else if (c instanceof SelectExpressionItem) {
                SelectExpressionItem se = (SelectExpressionItem) c;
                countJdbcParameters += countJdbcParametersUsedByExpression(se.getExpression());
                if (isAggregateFunction(se.getExpression())) {
                    containsAggregateFunctions = true;
                }
            }
        }
        Projection projection;
        if (allColumns) {
            projection = Projection.IDENTITY(table.columns);
        } else {
            projection = new SQLProjection(table, selectBody.getSelectItems());
        }
        if (scan) {
            Predicate where = null;
            if (selectBody.getWhere() != null) {
                SQLRecordKeyFunction keyFunction = findPrimaryKeyIndexSeek(selectBody.getWhere(), table, new AtomicInteger());
                if (keyFunction != null) {
                    // optimize PrimaryKeyIndexSeek case                    
                    where = new PrimaryKeyIndexSeekPredicate(keyFunction);
                }
            }
            if (where == null) {
                where = selectBody.getWhere() != null ? new SQLRecordPredicate(table, selectBody.getWhere(), 0) : null;
            }

            Aggregator aggregator = null;
            ScanLimits limitsOnScan = null;
            ScanLimits limitsOnPlan = null;
            if (containsAggregateFunctions || (selectBody.getGroupByColumnReferences() != null && !selectBody.getGroupByColumnReferences().isEmpty())) {
                aggregator = new SQLAggregator(selectBody.getSelectItems(), selectBody.getGroupByColumnReferences());
            }

            TupleComparator comparatorOnScan = null;
            TupleComparator comparatorOnPlan = null;
            if (selectBody.getOrderByElements() != null && !selectBody.getOrderByElements().isEmpty()) {
                if (aggregator != null) {
                    comparatorOnPlan = new SQLTupleComparator(selectBody.getOrderByElements());;
                } else {
                    comparatorOnScan = new SQLTupleComparator(selectBody.getOrderByElements());
                }
            }

            Limit limit = selectBody.getLimit();
            Top top = selectBody.getTop();
            if (limit != null && top != null) {
                throw new StatementExecutionException("LIMIT and TOP cannot be used on the same query");
            }
            if (limit != null) {
                if (limit.isLimitAll() || limit.isLimitNull() || limit.isOffsetJdbcParameter() || limit.isRowCountJdbcParameter()) {
                    throw new StatementExecutionException("Invalid LIMIT clause");
                }
                if (aggregator != null) {
                    limitsOnPlan = new ScanLimits((int) limit.getRowCount(), (int) limit.getOffset());
                } else {
                    limitsOnScan = new ScanLimits((int) limit.getRowCount(), (int) limit.getOffset());
                }
            } else if (top != null) {
                if (top.isPercentage() || top.isRowCountJdbcParameter()) {
                    throw new StatementExecutionException("Invalid TOP clause");
                }
                if (aggregator != null) {
                    limitsOnPlan = new ScanLimits((int) top.getRowCount(), 0);
                } else {
                    limitsOnScan = new ScanLimits((int) top.getRowCount(), 0);
                }
            }

            try {
                ScanStatement statement = new ScanStatement(tableSpace, tableName, projection, where, comparatorOnScan, limitsOnScan);
                return ExecutionPlan.make(statement, aggregator, limitsOnPlan, comparatorOnPlan);
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        } else {
            if (selectBody.getWhere() == null) {
                throw new StatementExecutionException("unsupported GET without WHERE");
            }
            // SELECT * FROM WHERE KEY=? AND ....
            RecordFunction keyFunction = findPrimaryKeyIndexSeek(selectBody.getWhere(), table, new AtomicInteger());
            if (keyFunction == null) {
                throw new StatementExecutionException("unsupported where " + selectBody.getWhere() + " " + selectBody.getWhere().getClass());
            }

            Predicate where = buildPredicate(selectBody.getWhere(), table, 0);

            try {
                return ExecutionPlan.simple(new GetStatement(tableSpace, tableName, keyFunction, where));
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        }
    }

    private Statement buildExecuteStatement(Execute execute) throws StatementExecutionException {
        switch (execute.getName()) {
            case "BEGINTRANSACTION": {
                if (execute.getExprList().getExpressions().size() != 1) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName");
                }
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0));
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("BEGINTRANSACTION requires one parameter (EXECUTE BEGINTRANSACTION tableSpaceName");
                }
                return new BeginTransactionStatement(tableSpaceName.toString());
            }
            case "COMMITTRANSACTION": {
                if (execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                AtomicInteger pos = new AtomicInteger();
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0));
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE COMMITTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1));
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
                if (execute.getExprList().getExpressions().size() != 2) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                AtomicInteger pos = new AtomicInteger();
                Object tableSpaceName = resolveValue(execute.getExprList().getExpressions().get(0));
                if (tableSpaceName == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                Object transactionId = resolveValue(execute.getExprList().getExpressions().get(1));
                if (transactionId == null) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
                try {
                    return new RollbackTransactionStatement(tableSpaceName.toString(), Long.parseLong(transactionId.toString()));
                } catch (NumberFormatException err) {
                    throw new StatementExecutionException("COMMITTRANSACTION requires two parameters (EXECUTE ROLLBACKTRANSACTION tableSpaceName transactionId)");
                }
            }
            default:
                throw new StatementExecutionException("Unsupported command " + execute.getName());
        }
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
}
