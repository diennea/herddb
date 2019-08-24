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

package herddb.model.planner;

import herddb.core.TableSpaceManager;
import herddb.model.DMLStatement;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.utils.Wrapper;
import java.util.concurrent.CompletableFuture;

/**
 * DELETE
 *
 * @author eolivelli
 */
public class SimpleDeleteOp implements PlannerOp {

    private final DMLStatement statement;

    public SimpleDeleteOp(DMLStatement delete) {
        this.statement = delete;
    }

    @Override
    public String getTablespace() {
        return statement.getTableSpace();
    }

    @Override
    public CompletableFuture<StatementExecutionResult> executeAsync(
            TableSpaceManager tableSpaceManager,
            TransactionContext transaction, StatementEvaluationContext context, boolean lockRequired, boolean forWrite
    )
            throws StatementExecutionException {

        return tableSpaceManager.executeStatementAsync(statement, context, transaction);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        T unwrapped = statement.unwrap(clazz);
        if (unwrapped != null) {
            return unwrapped;
        }
        return Wrapper.unwrap(this, clazz);
    }

    @Override
    public boolean isSimpleStatementWrapper() {
        return true;
    }

}
