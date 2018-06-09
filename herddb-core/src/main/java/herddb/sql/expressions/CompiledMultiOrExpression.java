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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.utils.SQLRecordPredicateFunctions;
import java.util.Arrays;

@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
public class CompiledMultiOrExpression implements CompiledSQLExpression {

    private final CompiledSQLExpression[] operands;

    public CompiledMultiOrExpression(CompiledSQLExpression[] operands) {
        this.operands = operands;
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        for (int i = 0; i < operands.length; i++) {
            boolean ok = SQLRecordPredicateFunctions.toBoolean(operands[i].evaluate(bean, context));
            if (ok) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        for (CompiledSQLExpression op : operands) {
            op.validate(context);
        }
    }

    @Override
    public String toString() {
        return "OR{" + Arrays.toString(operands) + '}';
    }

    @Override
    public CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(int[] projection) {
        CompiledSQLExpression[] ops = new CompiledSQLExpression[operands.length];
        int i = 0;
        for (CompiledSQLExpression exp : operands) {
            ops[i++] = exp.remapPositionalAccessToToPrimaryKeyAccessor(projection);
        }
        return new CompiledMultiOrExpression(ops);
    }

}
