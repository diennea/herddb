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
import static herddb.sql.SQLRecordPredicate.compare;
import static herddb.sql.SQLRecordPredicate.like;
import static herddb.sql.SQLRecordPredicate.objectEquals;
import static herddb.sql.expressions.SQLExpressionCompiler.compileExpression;
import java.util.Map;
import net.sf.jsqlparser.expression.operators.relational.Between;

public class CompiledBetweenExpression implements CompiledSQLExpression {

    private final CompiledSQLExpression left;
    private final CompiledSQLExpression start;
    private final CompiledSQLExpression end;
    private final boolean not;

    public CompiledBetweenExpression(Boolean not, CompiledSQLExpression left, CompiledSQLExpression start, CompiledSQLExpression end) {
        this.left = left;
        this.start = start;
        this.end = end;
        this.not = not;
    }
    
    public static CompiledSQLExpression create(String validatedTableAlias, Between b) {
        CompiledSQLExpression left = compileExpression(validatedTableAlias, b.getLeftExpression());
        if (left == null) {
            return null;
        }
        CompiledSQLExpression start = compileExpression(validatedTableAlias, b.getBetweenExpressionStart());
        if (start == null) {
            return null;
        }
        CompiledSQLExpression end = compileExpression(validatedTableAlias, b.getBetweenExpressionEnd());
        if (end == null) {
            return null;
        }
        return new CompiledBetweenExpression(b.isNot(), left, start, end);
    }

    @Override
    public Object evaluate(Map<String, Object> bean, StatementEvaluationContext context) throws StatementExecutionException {
        
        Object leftValue = left.evaluate(bean, context);
        Object startValue = start.evaluate(bean, context);
        Object endValue = end.evaluate(bean, context);
        boolean result = (objectEquals(startValue, endValue) || compare(startValue, endValue) < 0) // check impossible range
            && (objectEquals(leftValue, startValue)
            || objectEquals(leftValue, endValue)
            || (compare(leftValue, startValue) > 0 && compare(leftValue, endValue) < 0)); // check value in range;

        if (not) {
            return !result;
        } else {
            return result;
        }
    }

}
