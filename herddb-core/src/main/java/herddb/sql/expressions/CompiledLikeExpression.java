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

import static herddb.utils.SQLRecordPredicateFunctions.like;
import static herddb.utils.SQLRecordPredicateFunctions.matches;
import herddb.core.HerdDBInternalException;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.utils.DataAccessor;
import herddb.utils.SQLRecordPredicateFunctions;
import java.util.regex.Pattern;

public class CompiledLikeExpression extends CompiledBinarySQLExpression {

    private final Pattern rightConstantPattern;

    public CompiledLikeExpression(CompiledSQLExpression left, CompiledSQLExpression right) throws HerdDBInternalException {
        super(left, right);
        this.rightConstantPattern = compilePattern(right, '\\');
    }

    public CompiledLikeExpression(CompiledSQLExpression left,    CompiledSQLExpression right, CompiledSQLExpression escape) throws HerdDBInternalException {
        super(left, right);
        this.rightConstantPattern = compilePattern(right,
                ((String) escape.cast(ColumnTypes.STRING).evaluate(DataAccessor.NULL, null)).charAt(0)
        );
    }

    private static Pattern compilePattern(CompiledSQLExpression exp, char escapeChar) throws HerdDBInternalException {
        if (exp instanceof ConstantExpression) {
            ConstantExpression ce = (ConstantExpression) exp;
            if (ce.isNull()) {
                return null;
            }
            return SQLRecordPredicateFunctions.compileLikePattern(
                    ce.evaluate(DataAccessor.NULL, null).toString(),
                    escapeChar
            );
        } else {
            return null;
        }
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        Object leftValue = left.evaluate(bean, context);
        boolean ok;
        if (rightConstantPattern != null) {
            ok = matches(leftValue, rightConstantPattern);
        } else {
            Object rightValue = right.evaluate(bean, context);
            ok = like(leftValue, rightValue, '\\');
        }
        return ok;
    }

    @Override
    public CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(int[] projection) {
        return new CompiledLikeExpression(
                left.remapPositionalAccessToToPrimaryKeyAccessor(projection),
                right.remapPositionalAccessToToPrimaryKeyAccessor(projection));
    }

}
