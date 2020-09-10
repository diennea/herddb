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

import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.sql.SQLRecordPredicate;
import herddb.utils.DataAccessor;
import java.util.List;

/**
 * Generic cast
 *
 * @author eolivelli
 */
public class CastExpression implements CompiledSQLExpression {

    private final CompiledSQLExpression wrapped;
    private final int type;

    public CastExpression(CompiledSQLExpression wrapped, int type) {
        this.wrapped = wrapped;
        this.type = type;
    }

    @Override
    public Object evaluate(DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        try {
            return SQLRecordPredicate.cast(wrapped.evaluate(bean, context), type);
        } catch (IllegalArgumentException err) {
            throw new StatementExecutionException(err);
        }
    }

    @Override
    public void validate(StatementEvaluationContext context) throws StatementExecutionException {
        wrapped.validate(context);
    }

    @Override
    public List<CompiledSQLExpression> scanForConstraintedValueOnColumnWithOperator(String column, String operator, BindableTableScanColumnNameResolver columnNameResolver) {
        return wrapped.scanForConstraintedValueOnColumnWithOperator(column, operator, columnNameResolver);
    }

    @Override
    public List<CompiledSQLExpression> scanForConstraintsOnColumn(String column, BindableTableScanColumnNameResolver columnNameResolver) {
        return wrapped.scanForConstraintsOnColumn(column, columnNameResolver);
    }

    @Override
    public CompiledSQLExpression cast(int type) {
        return wrapped.cast(type);
    }

    @Override
    public CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(int[] projection) {
        return new CastExpression(wrapped.remapPositionalAccessToToPrimaryKeyAccessor(projection),
                type);
    }

    @Override
    public String toString() {
        return "CastExpression{" + "wrapped=" + wrapped + ", type=" + ColumnTypes.typeToString(type) + '}';
    }

}
