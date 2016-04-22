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
import herddb.model.Table;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

/**
 * Record mutator using SQL
 *
 * @author enrico.olivelli
 */
public class SQLRecordKeyFunction extends RecordFunction {

    private final Expression expression;
    private final herddb.model.Column column;
    private final int jdbcParametersStartPos;

    public SQLRecordKeyFunction(Table table, Expression expression, int jdbcParametersStartPos) {
        this.jdbcParametersStartPos = jdbcParametersStartPos;
        this.column = table.getColumn(table.primaryKeyColumn);
        this.expression = expression;
    }

    @Override
    public byte[] computeNewValue(Record previous, StatementEvaluationContext context) {
        SQLStatementEvaluationContext statementEvaluationContext = (SQLStatementEvaluationContext) context;        

        int paramIndex = jdbcParametersStartPos;
        Object value;
        if (expression instanceof JdbcParameter) {
            value = statementEvaluationContext.jdbcParameters.get(paramIndex++);
        } else if (expression instanceof LongValue) {
            value = ((LongValue) expression).getValue();
        } else if (expression instanceof StringValue) {
            value = ((StringValue) expression).getValue();
        } else {
            throw new RuntimeException("unsupported type " + expression.getClass() + " " + expression);
        }
        return RecordSerializer.serialize(value, column.type);
    }

}
