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

import herddb.core.HerdDBInternalException;
import herddb.core.TableSpaceManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionContext;
import herddb.utils.Wrapper;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Generic step of planned operation
 *
 * @author eolivelli
 */
public interface PlannerOp extends Wrapper {

    public String getTablespace();

    public default StatementExecutionResult execute(TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context, boolean lockRequired, boolean forWrite) throws StatementExecutionException {
        CompletableFuture<StatementExecutionResult> res = executeAsync(tableSpaceManager, transactionContext, context, lockRequired, forWrite);
        try {
            return res.get();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new StatementExecutionException(err);
        } catch (ExecutionException err) {
            Throwable cause = err.getCause();
            if (cause instanceof HerdDBInternalException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof StatementExecutionException) {
                throw (StatementExecutionException) cause;
            } else {
                throw new StatementExecutionException(cause);
            }
        }
    }

    public default CompletableFuture<StatementExecutionResult> executeAsync(TableSpaceManager tableSpaceManager,
            TransactionContext transactionContext,
            StatementEvaluationContext context, boolean lockRequired, boolean forWrite) {
        try {
            return CompletableFuture.completedFuture(execute(tableSpaceManager, transactionContext, context, lockRequired, forWrite));
        } catch (StatementExecutionException err) {
            return FutureUtils.exception(err);
        }
    }

    /**
     * Optimize this node, eventually merging this node with its inputs
     *
     * @return
     */
    public default PlannerOp optimize() {
        return this;
    }
}
