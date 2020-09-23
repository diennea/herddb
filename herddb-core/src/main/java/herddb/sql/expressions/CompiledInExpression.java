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
import java.util.Arrays;

@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
public class CompiledInExpression implements CompiledSQLExpression {

    private final CompiledSQLExpression left;
    private final CompiledSQLExpression[] values;

    public CompiledInExpression(CompiledSQLExpression left, CompiledSQLExpression[] values) {
        this.values = values;
        this.left = left;
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        for (int i = 0; i < values.length; i++) {
            // zero copy
            if (left.opEqualsTo(bean, context, values[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        left.validate(context);
        for (CompiledSQLExpression op : values) {
            op.validate(context);
        }
    }

    @Override
    public String toString() {
        return "IN{" + left + ", " + Arrays.toString(values) + '}';
    }

    @Override
    public CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(int[] projection) {
        CompiledSQLExpression[] remappedValues = new CompiledSQLExpression[values.length];
        int i = 0;
        for (CompiledSQLExpression exp : values) {
            remappedValues[i++] = exp.remapPositionalAccessToToPrimaryKeyAccessor(projection);
        }
        return new CompiledInExpression(left.remapPositionalAccessToToPrimaryKeyAccessor(projection), remappedValues);
    }
}
