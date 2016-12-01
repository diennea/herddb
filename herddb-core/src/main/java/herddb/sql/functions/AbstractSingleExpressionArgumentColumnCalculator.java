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
package herddb.sql.functions;

import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.sql.AggregatedColumnCalculator;
import java.util.function.Function;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.schema.Column;

/**
 *
 * @author enrico.olivelli
 */
public abstract class AbstractSingleExpressionArgumentColumnCalculator implements AggregatedColumnCalculator {

    final protected String fieldName;
    final protected Expression expression;
    final protected Function<Tuple, ? extends Comparable> valueExtractor;

    protected AbstractSingleExpressionArgumentColumnCalculator(String fieldName, Expression expression,
        StatementEvaluationContext context
    ) throws StatementExecutionException {
        this.fieldName = fieldName;
        this.expression = expression;
        if (expression instanceof Column) {
            Column c = (Column) expression;
            String name = c.getColumnName();
            valueExtractor = (Tuple t) -> {
                Object value = t.get(name);
                if (value == null) {
                    return null;
                }
                if (value instanceof Long) {
                    return (Long) value;
                } else if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else if (value instanceof java.sql.Timestamp) {
                    return ((java.sql.Timestamp) value);
                } else {
                    return Long.parseLong(value.toString());
                }
            };
        } else if (this.expression instanceof LongValue) {
            Long fixed = ((LongValue) expression).getValue();
            valueExtractor = (Tuple t) -> fixed;
        } else if (this.expression instanceof TimestampValue) {
            java.sql.Timestamp fixed = ((TimestampValue) expression).getValue();
            valueExtractor = (Tuple t) -> fixed;
        } else if (this.expression instanceof StringValue) {
            try {
                Long fixed = Long.parseLong(((StringValue) expression).getValue());
                valueExtractor = (Tuple t) -> fixed;
            } catch (NumberFormatException err) {
                throw new StatementExecutionException("cannot SUM expressions of type " + expression);
            }
        } else if (this.expression instanceof JdbcParameter) {
            JdbcParameter param = (JdbcParameter) this.expression;
            int index = param.getIndex();
            Object value = context.getJdbcParameters().get(index);
            valueExtractor = (Tuple t) -> (Comparable) value;
        } else {
            throw new StatementExecutionException("cannot SUM expressions of type " + expression.getClass() + ": bad expression is " + expression);
        }
    }
}
