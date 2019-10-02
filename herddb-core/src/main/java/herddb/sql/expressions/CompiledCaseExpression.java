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

package herddb.sql.expressions;

import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.utils.SQLRecordPredicateFunctions;
import java.util.List;
import java.util.Map.Entry;

public class CompiledCaseExpression implements CompiledSQLExpression {

    private final List<Entry<CompiledSQLExpression, CompiledSQLExpression>> whenExpressions;
    private final CompiledSQLExpression elseExpression;

    public CompiledCaseExpression(List<Entry<CompiledSQLExpression, CompiledSQLExpression>> whenExpressions, CompiledSQLExpression elseExpression) {
        this.whenExpressions = whenExpressions;
        this.elseExpression = elseExpression;
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        if (whenExpressions != null) {
            for (Entry<CompiledSQLExpression, CompiledSQLExpression> entry : whenExpressions) {
                Object whenValue = entry.getKey().evaluate(bean, context);
                if (SQLRecordPredicateFunctions.toBoolean(whenValue)) {
                    return entry.getValue().evaluate(bean, context);
                }
            }
        }
        if (elseExpression != null) {
            return elseExpression.evaluate(bean, context);
        } else {
            return null;
        }
    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        if (whenExpressions != null) {
            for (Entry<CompiledSQLExpression, CompiledSQLExpression> entry : whenExpressions) {
                entry.getKey().validate(context);
                entry.getValue().validate(context);
            }
        }
        if (elseExpression != null) {
            elseExpression.validate(context);
        }
    }


}
