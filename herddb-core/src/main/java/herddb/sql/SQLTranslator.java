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
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.ExecutionPlan;
import herddb.model.Predicate;
import herddb.model.PrimaryKeyIndexSeekPredicate;
import herddb.model.Projection;
import herddb.model.RecordFunction;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
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
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
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
        } catch (JSQLParserException err) {
            throw new StatementExecutionException("unable to parse query " + query, err);
        }

    }

    private Statement buildCreateTableStatement(CreateTable s) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName();
        if (tableSpace == null) {
            tableSpace = TableSpace.DEFAULT;
        }
        Table.Builder tablebuilder = Table.builder().name(tableName).tablespace(tableSpace);
        for (ColumnDefinition cf : s.getColumnDefinitions()) {
            int type;
            switch (cf.getColDataType().getDataType()) {
                case "string":
                case "varchar":
                case "nvarchar":
                case "nvarchar2":
                    type = ColumnTypes.STRING;
                    break;
                case "long":
                case "bigint":
                    type = ColumnTypes.LONG;
                    break;
                case "int":
                case "integer":
                case "smallint":
                    type = ColumnTypes.INTEGER;
                    break;
                case "bytea":
                case "blob":
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
            if (cf.getColumnSpecStrings() != null && cf.getColumnSpecStrings().contains("primary")) {
                tablebuilder.primaryKey(cf.getColumnName());
            }

        }
        try {
            CreateTableStatement statement = new CreateTableStatement(tablebuilder.build());
            return statement;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException("bad table definition", err);
        }
    }

    private Statement buildInsertStatement(Insert s) throws StatementExecutionException {
        String tableSpace = s.getTable().getSchemaName();
        String tableName = s.getTable().getName();
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

        net.sf.jsqlparser.schema.Column keyColumn = null;
        Expression keyValueExpression = null;
        List<Expression> valuesExpressions = new ArrayList<>();
        List<net.sf.jsqlparser.schema.Column> valuesColumns = new ArrayList<>();

        int index = 0;
        int countJdbcParametersBeforeKey = 0;
        int countJdbcParameters = 0;
        ExpressionList list = (ExpressionList) s.getItemsList();
        for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
            Column column = table.getColumn(c.getColumnName());
            if (column == null) {
                throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablepace " + tableSpace);
            }
            Expression expression = list.getExpressions().get(index++);
            if (table.primaryKeyColumn.equals(column.name)) {
                keyColumn = c;
                keyValueExpression = expression;
                countJdbcParametersBeforeKey = countJdbcParameters;
            }
            valuesColumns.add(c);
            valuesExpressions.add(expression);
            countJdbcParameters += countJdbcParametersUsedByExpression(expression);
        }
        if (keyValueExpression == null) {
            throw new StatementExecutionException("you must set a value for the primary key");
        }
        RecordFunction keyfunction = new SQLRecordKeyFunction(table, keyValueExpression, countJdbcParametersBeforeKey);
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
        String tableName = fromTable.getName();
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

        Expression key = findPrimaryKeyEqualsTo(s.getWhere(), table, new AtomicInteger());
        if (key == null) {
            // DELETE FROM TABLE WHERE KEY=? AND ....
            throw new StatementExecutionException("unsupported where " + s.getWhere());
        }
        RecordFunction keyFunction = new SQLRecordKeyFunction(table, key, 0);

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
        String tableName = fromTable.getName();
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
        Map<String, Object> record = new HashMap<>();

        for (net.sf.jsqlparser.schema.Column c : s.getColumns()) {
            Column column = table.getColumn(c.getColumnName());
            if (column == null) {
                throw new StatementExecutionException("no such column " + c.getColumnName() + " in table " + tableName + " in tablepace " + tableSpace);
            }
        }

        RecordFunction function = new SQLRecordFunction(table, s.getColumns(), s.getExpressions(), 0);
        int setClauseParamters = (int) s.getExpressions().stream().filter(e -> e instanceof JdbcParameter).count();
        Expression key = findPrimaryKeyEqualsTo(s.getWhere(), table, new AtomicInteger(setClauseParamters));
        if (key == null) {
            // UPDATE TABLE SET XXX WHERE KEY=? AND ....
            throw new StatementExecutionException("unsupported where " + s.getWhere());
        }

        RecordFunction keyFunction = new SQLRecordKeyFunction(table, key, setClauseParamters);
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
        throw new UnsupportedOperationException("unsupported expression type " + e.getClass() + " (" + e + ")");
    }

    private Expression findPrimaryKeyEqualsTo(Expression where, Table table, AtomicInteger jdbcParameterPos) throws StatementExecutionException {
        if (where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            Expression keyOnLeft = validatePrimaryKeyEqualsToExpression(and.getLeftExpression(), table, jdbcParameterPos);
            if (keyOnLeft != null) {
                return keyOnLeft;
            }
            int countJdbcParametersUsedByLeft = countJdbcParametersUsedByExpression(and.getLeftExpression());

            Expression keyOnRight = validatePrimaryKeyEqualsToExpression(and.getRightExpression(), table, new AtomicInteger(jdbcParameterPos.get() + countJdbcParametersUsedByLeft));
            if (keyOnRight != null) {
                return keyOnRight;
            }
        } else if (where instanceof EqualsTo) {
            Expression keyDirect = validatePrimaryKeyEqualsToExpression(where, table, jdbcParameterPos);
            if (keyDirect != null) {
                return keyDirect;
            }
        }

        return null;
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

    private Expression validatePrimaryKeyEqualsToExpression(Expression testExpression, Table table1, AtomicInteger jdbcParameterPos) throws StatementExecutionException {
        Expression result = null;
        if (testExpression instanceof EqualsTo) {
            EqualsTo e = (EqualsTo) testExpression;
            if (e.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) e.getLeftExpression();
                if (column.getColumnName().equals(table1.primaryKeyColumn)) {
                    return e.getRightExpression();
                }
            } else if (e.getLeftExpression() instanceof AndExpression) {
                result = findPrimaryKeyEqualsTo((AndExpression) e.getLeftExpression(), table1, jdbcParameterPos);
            }
        }
        return result;
    }

    private ExecutionPlan buildSelectStatement(Select s, boolean scan) throws StatementExecutionException {
        PlainSelect selectBody = (PlainSelect) s.getSelectBody();
        net.sf.jsqlparser.schema.Table fromTable = (net.sf.jsqlparser.schema.Table) selectBody.getFromItem();
        String tableSpace = fromTable.getSchemaName();
        String tableName = fromTable.getName();
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
        for (SelectItem c : selectBody.getSelectItems()) {

            if (c instanceof AllColumns) {
                allColumns = true;
                break;
            } else if (c instanceof SelectExpressionItem) {
                SelectExpressionItem se = (SelectExpressionItem) c;
                countJdbcParameters += countJdbcParametersUsedByExpression(se.getExpression());
            }
        }
        Projection projection;
        if (allColumns) {
            projection = Projection.IDENTITY(table.columns);
        } else {
            projection = new SQLProjection(selectBody.getSelectItems());
        }
        if (scan) {
            Predicate where = null;
            if (selectBody.getWhere() != null && selectBody.getWhere() instanceof EqualsTo) {
                Expression key = findPrimaryKeyEqualsTo(selectBody.getWhere(), table, new AtomicInteger());
                if (key != null) {
                    // optimize PrimaryKeyIndexSeek case
                    RecordFunction keyFunction = new SQLRecordKeyFunction(table, key, countJdbcParameters);
                    where = new PrimaryKeyIndexSeekPredicate(keyFunction);
                }
            }
            if (where == null) {
                where = selectBody.getWhere() != null ? new SQLRecordPredicate(table, selectBody.getWhere(), 0) : null;
            }
            TupleComparator comparator = null;
            if (selectBody.getOrderByElements() != null && !selectBody.getOrderByElements().isEmpty()) {
                comparator = new SQLTupleComparator(selectBody.getOrderByElements());
            }
            try {
                ScanStatement statement = new ScanStatement(tableSpace, tableName, projection, where, comparator);
                return ExecutionPlan.simple(statement);
            } catch (IllegalArgumentException err) {
                throw new StatementExecutionException(err);
            }
        } else {
            if (selectBody.getWhere() == null) {
                throw new StatementExecutionException("unsupported GET without WHERE");
            }
            // SELECT * FROM WHERE KEY=? AND ....
            Expression key = findPrimaryKeyEqualsTo(selectBody.getWhere(), table, new AtomicInteger());
            if (key == null) {
                throw new StatementExecutionException("unsupported where " + selectBody.getWhere() + " " + selectBody.getWhere().getClass());
            }
            RecordFunction keyFunction = new SQLRecordKeyFunction(table, key, countJdbcParameters);

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
}
