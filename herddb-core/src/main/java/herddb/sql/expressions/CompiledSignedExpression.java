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

public class CompiledSignedExpression implements CompiledSQLExpression {

    private final CompiledSQLExpression inner;
    private final char sign;

    public CompiledSignedExpression(char sign, CompiledSQLExpression inner) {
        this.inner = inner;
        this.sign = sign;
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        Object innerValue = inner.evaluate(bean, context);
        switch (sign) {
            case '-':
                if (innerValue instanceof Integer) {
                    return ((Integer) innerValue) * -1;
                }
                if (innerValue instanceof Long) {
                    return ((Long) innerValue) * -1;
                }
                if (innerValue instanceof Double) {
                    return ((Double) innerValue) * -1;
                }
                if (innerValue instanceof Float) {
                    return ((Float) innerValue) * -1;
                }
                if (innerValue instanceof Short) {
                    return ((Short) innerValue) * -1;
                }
                if (innerValue instanceof Byte) {
                    return ((Byte) innerValue) * -1;
                }
                throw new StatementExecutionException("invalid signed expression, value=" + innerValue + "; class=" + innerValue.getClass() + "; exp=" + inner);
            case '+':
                return innerValue;
            default:
                throw new StatementExecutionException("invalid sign '" + sign + "'");
        }

    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        inner.validate(context);
    }

}
