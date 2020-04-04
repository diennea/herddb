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
import herddb.utils.DataAccessor;
import java.util.Arrays;

/**
 * reference to downstrean inputs in the pipeline
 *
 * @author eolivelli
 */
public class AccessCurrentRowExpression implements CompiledSQLExpression {

    private final int index;

    public AccessCurrentRowExpression(int index) {
        this.index = index;
    }

    @Override
    public Object evaluate(DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        return bean.get(index);
    }

    @Override
    public boolean opEqualsTo(DataAccessor bean, StatementEvaluationContext context, CompiledSQLExpression right) throws StatementExecutionException {
        Object rightValue = right.evaluate(bean, context);
        return bean.fieldEqualsTo(index, rightValue);
    }

    @Override
    public boolean opNotEqualsTo(DataAccessor bean, StatementEvaluationContext context, CompiledSQLExpression right) throws StatementExecutionException {
        Object rightValue = right.evaluate(bean, context);
        return bean.fieldNotEqualsTo(index, rightValue);
    }

    @Override
    public int opCompareTo(DataAccessor bean, StatementEvaluationContext context, CompiledSQLExpression right) throws StatementExecutionException {
        Object rightValue = right.evaluate(bean, context);
        return bean.fieldCompareTo(index, rightValue);
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "AccessCurrentRow{" + index + '}';
    }

    @Override
    public CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(int[] projection) {

        int _index = projection[index];
        if (_index < 0) {
            throw new IllegalStateException("column " + index + " not in pk, projection:" + Arrays.toString(projection));
        }
        return new AccessCurrentRowExpression(_index);
    }

}
