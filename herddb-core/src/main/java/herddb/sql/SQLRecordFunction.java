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

import herddb.codec.RecordSerializer;
import herddb.model.Record;
import herddb.model.RecordFunction;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.sql.expressions.CompiledSQLExpression;
import herddb.utils.DataAccessor;
import java.util.Arrays;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.sf.jsqlparser.schema.Column;

/**
 * Record mutator using SQL
 *
 * @author enrico.olivelli
 */
public class SQLRecordFunction extends RecordFunction {

    private final Table table;
    private final List<String> columns;
    private final List<CompiledSQLExpression> expressions;
    private final Map<String, CompiledSQLExpression> expressionsByColumnName;

    public SQLRecordFunction(Table table, List<Column> columns, List<CompiledSQLExpression> expressions) {
        this.table = table;
        this.columns = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
        this.expressions = expressions;
        this.expressionsByColumnName = new HashMap<>();
        final int size = columns.size();
        for (int i = 0; i < size; i++) {
            expressionsByColumnName.put(this.columns.get(i), expressions.get(i));
        }
    }

    public SQLRecordFunction(List<String> columns, Table table, List<CompiledSQLExpression> expressions) {
        this.table = table;
        this.columns = columns;
        this.expressions = expressions;
        this.expressionsByColumnName = new HashMap<>();
        final int size = columns.size();
        for (int i = 0; i < size; i++) {
            expressionsByColumnName.put(this.columns.get(i), expressions.get(i));
        }
    }

    @Override
    public byte[] computeNewValue(Record previous, StatementEvaluationContext context, TableContext tableContext) throws StatementExecutionException {
        try {
            byte[] oldRes = _computeNewValueOld(previous, context);
            byte[] newRes = _computeNewValue(previous, context, tableContext);
            if (!Arrays.equals(oldRes, newRes)) {
                throw new IllegalArgumentException("this is a bug!");
            }
            return newRes;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    byte[] _computeNewValueOld(Record previous, StatementEvaluationContext context) throws StatementExecutionException {
        Map<String, Object> res = previous != null ? new HashMap<>(previous.toBean(table)) : new HashMap<>(table.columns.length);
        DataAccessor bean = previous != null ? previous.getDataAccessor(table) : DataAccessor.NULL;
        final int size = columns.size();
        for (int i = 0; i < size; i++) {
            CompiledSQLExpression e = expressions.get(i);
            String columnName = columns.get(i);
            herddb.model.Column column = table.getColumn(columnName);
            if (column == null) {
                throw new StatementExecutionException("unknown column " + columnName + " in table " + table.name);
            }
            columnName = column.name;
            Object value = RecordSerializer.convert(column.type, e.evaluate(bean, context));
            res.put(columnName, value);
        }
        return RecordSerializer.serializeValueRaw(res, table, previous != null && previous.value != null ? previous.value.data.length : 0);
    }

    private byte[] _computeNewValue(Record previous, StatementEvaluationContext context, TableContext tableContext) throws StatementExecutionException {
        try {
            DataAccessor bean = previous != null ? previous.getDataAccessor(table) : DataAccessor.NULL;
            Function<String, Object> fieldValueComputer = (columnName) -> {
                CompiledSQLExpression e = expressionsByColumnName.get(columnName);
                if (e == null) {
                    return bean.get(columnName);
                } else {
                    herddb.model.Column column = table.getColumn(columnName);
                    if (column == null) {
                        throw new StatementExecutionException("unknown column " + columnName + " in table " + table.name);
                    }
                    return RecordSerializer.convert(column.type, e.evaluate(bean, context));
                }
            };

            byte[] finalRes = RecordSerializer.buildRecord(previous != null && previous.value != null ? previous.value.data.length : 0, table,
                    fieldValueComputer);
            return finalRes;
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    @Override
    public String toString() {
        return "SQLRecordFunction{" + "table=" + table + ", columns=" + columns + ", expressions=" + expressions + '}';
    }

}
