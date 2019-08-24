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
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;

/**
 * Returns at most the first value
 *
 * @author enrico.olivelli
 * @see SqlSingleValueAggFunction
 */
public class SingleValueCalculator extends AbstractSingleExpressionArgumentColumnCalculator {

    public SingleValueCalculator(String fieldName, CompiledSQLExpression expression, StatementEvaluationContext context) throws StatementExecutionException {
        super(fieldName, expression, context);
    }

    Object result;

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public void consume(herddb.utils.DataAccessor tuple) throws StatementExecutionException {
        result = valueExtractor.apply(tuple);
    }

    @Override
    public Object getValue() {
        return result;
    }
}
