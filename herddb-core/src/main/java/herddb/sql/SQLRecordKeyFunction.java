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
import static herddb.model.Column.column;
import herddb.model.Record;
import herddb.model.RecordFunction;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

/**
 * Record mutator using SQL
 *
 * @author enrico.olivelli
 */
public class SQLRecordKeyFunction extends RecordFunction {

    private final List<Expression> expressions;
    private final herddb.model.Column[] columns;
    private final int jdbcParametersStartPos;
    private final Table table;

    public SQLRecordKeyFunction(Table table, List<String> expressionToColumn, List<Expression> expressions, int jdbcParametersStartPos) {
        this.jdbcParametersStartPos = jdbcParametersStartPos;
        this.table = table;
        this.columns = new Column[table.primaryKey.length];
        this.expressions = new ArrayList<>();
        int i = 0;
        for (String cexp : expressionToColumn) {
            Column pkcolumn = table.getColumn(cexp);
            this.columns[i] = pkcolumn;
            this.expressions.add(expressions.get(i));
            i++;
        }
    }

    @Override
    public byte[] computeNewValue(Record previous, StatementEvaluationContext context) throws StatementExecutionException {
        SQLStatementEvaluationContext statementEvaluationContext = (SQLStatementEvaluationContext) context;

        int paramIndex = jdbcParametersStartPos;
        Map<String, Object> pk = new HashMap<>();
        for (int i = 0; i < expressions.size(); i++) {
            herddb.model.Column c = columns[i];
            Expression expression = expressions.get(i);
            Object value;
            if (expression instanceof JdbcParameter) {
                value = statementEvaluationContext.jdbcParameters.get(paramIndex++);
            } else {
                if (expression instanceof LongValue) {
                    value = ((LongValue) expression).getValue();
                } else {
                    if (expression instanceof StringValue) {
                        value = ((StringValue) expression).getValue();
                    } else {
                        throw new StatementExecutionException("unsupported type " + expression.getClass() + " " + expression);
                    }
                }
            }
            pk.put(c.name, value);
        }
        try {
            return RecordSerializer.serializePrimaryKey(pk, table).data;
        } catch (Exception err) {
            throw new StatementExecutionException("error while converting primary key " + pk + ": " + err, err);
        }
    }

}
