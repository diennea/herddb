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
import herddb.sql.expressions.CompiledSQLExpression;

/**
 * SQL AVG
 *
 * @author enrico.olivelli
 */
public class FloatingPointAvgColumnCalculator extends AbstractSingleExpressionArgumentColumnCalculator {

    public FloatingPointAvgColumnCalculator(String fieldName, CompiledSQLExpression expression, StatementEvaluationContext context) throws StatementExecutionException {
        super(fieldName, expression, context);
    }

    double result;
    long count;

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public void consume(herddb.utils.DataAccessor tuple) throws StatementExecutionException {
        Comparable value = valueExtractor.apply(tuple);
        if (value != null) {
            result += ((Number) value).doubleValue();
        }
        count++;
    }

    @Override
    public Object getValue() {
        if (count == 0) {
            throw new StatementExecutionException("Division by zero in AVG function");
        }
        return result / count;
    }
}
