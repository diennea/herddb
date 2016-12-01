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
import herddb.model.Column;
import herddb.model.Record;
import herddb.model.RecordFunction;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import herddb.model.ColumnsList;

/**
 * Record mutator using SQL
 *
 * @author enrico.olivelli
 */
public class SQLRecordKeyFunction extends RecordFunction {

    private final List<Expression> expressions;
    private final herddb.model.Column[] columns;
    private final String[] pkColumnNames;
    private final ColumnsList table;
    private final boolean fullPrimaryKey;
    private final boolean isConstant;

    public SQLRecordKeyFunction(ColumnsList table, List<String> expressionToColumn, List<Expression> expressions) {
        this.table = table;
        this.columns = new Column[expressions.size()];
        this.expressions = new ArrayList<>();
        this.pkColumnNames = new String[expressions.size()];
        int i = 0;
        boolean constant = true;
        for (String cexp : expressionToColumn) {
            Column pkcolumn = table.getColumn(cexp);
            this.columns[i] = pkcolumn;
            Expression exp = expressions.get(i);
            this.expressions.add(exp);
            if (!SQLRecordPredicate.isConstant(exp)) {
                constant = false;
            }
            i++;
        }
        this.isConstant = constant;
        int k = 0;
        String[] primaryKey = table.getPrimaryKey();
        for (String pk : primaryKey) {
            if (expressionToColumn.contains(pk)) {
                this.pkColumnNames[k++] = pk;
            }
        }
        this.fullPrimaryKey = (primaryKey.length == columns.length);
    }

    public boolean isFullPrimaryKey() {
        return fullPrimaryKey;
    }

    @Override
    public byte[] computeNewValue(Record previous, StatementEvaluationContext context, TableContext tableContext) throws StatementExecutionException {
        SQLStatementEvaluationContext statementEvaluationContext = (SQLStatementEvaluationContext) context;

        if (isConstant) {
            byte[] cachedResult = (byte[]) statementEvaluationContext.getConstant(this);
            if (cachedResult != null) {
                return cachedResult;
            }
        }

        Map<String, Object> pk = new HashMap<>();
        for (int i = 0; i < expressions.size(); i++) {
            herddb.model.Column c = columns[i];
            Expression expression = expressions.get(i);
            Object value;
            if (expression instanceof JdbcParameter) {
                int index = ((JdbcParameter) expression).getIndex();
                try {
                    value = statementEvaluationContext.jdbcParameters.get(index);
                } catch (IndexOutOfBoundsException missingParam) {
                    throw new StatementExecutionException("missing JDBC parameter " + index);
                }
            } else if (expression instanceof LongValue) {
                value = ((LongValue) expression).getValue();
            } else if (expression instanceof TimestampValue) {
                value = ((TimestampValue) expression).getValue();
            } else if (expression instanceof StringValue) {
                value = ((StringValue) expression).getValue();
            } else {
                throw new StatementExecutionException("unsupported type " + expression.getClass() + " " + expression);
            }
            pk.put(c.name, value);
        }
        try {
            // maybe this is only a partial primary key
            byte[] result = RecordSerializer.serializePrimaryKey(pk, table, pkColumnNames).data;
            if (isConstant) {
                statementEvaluationContext.cacheConstant(this, result);
            }
            return result;
        } catch (Exception err) {
            throw new StatementExecutionException("error while converting primary key " + pk + ": " + err, err);
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder("SQLRecordKeyFunction (fullPrimaryKey=" + fullPrimaryKey + "):");
        for (int i = 0; i < pkColumnNames.length; i++) {
            if (i > 0) {
                b.append(" AND ");
            }
            b.append(pkColumnNames[i] + "=" + expressions.get(i));
        }
        return b.toString();

    }

}
