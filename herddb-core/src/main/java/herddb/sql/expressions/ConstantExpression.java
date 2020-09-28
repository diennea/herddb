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

public class ConstantExpression implements CompiledSQLExpression {

    private final Object value;
    private final int type;

    public ConstantExpression(Object value, int type) {
        this.value = value;
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public Object evaluate(herddb.utils.DataAccessor bean, StatementEvaluationContext context) throws StatementExecutionException {
        return value;
    }

    @Override
    public String toString() {
        if (value != null) {
            return "ConstantExpression{" + "value=" + value + ", " + value.getClass().getSimpleName() + ", type=" + ColumnTypes.typeToString(type) + '}';
        } else {
            return "ConstantExpression{null}";
        }
    }

    @Override
    public CompiledSQLExpression cast(int type) {
        if (this.type == type) {
            return this;
        }
        return new ConstantExpression(SQLRecordPredicate.cast(value, type), type);
    }

    @Override
    public CompiledSQLExpression remapPositionalAccessToToPrimaryKeyAccessor(int[] projection) {
        return this;
    }

    public boolean isNull() {
        return value == null;
    }

    public int getType() {
        return type;
    }

}
