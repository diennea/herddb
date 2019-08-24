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

package herddb.model;

import herddb.index.IndexOperation;
import herddb.utils.Bytes;
import herddb.utils.Wrapper;

/**
 * A Condition
 *
 * @author enrico.olivelli
 */
public abstract class Predicate implements Wrapper {

    public abstract boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException;

    /**
     * Perform validation of the predicate on the given context
     *
     * @param context the context of execution
     * @throws StatementExecutionException
     */
    public void validateContext(StatementEvaluationContext context) throws StatementExecutionException {
    }

    private IndexOperation indexOperation;

    public IndexOperation getIndexOperation() {
        return indexOperation;
    }

    public void setIndexOperation(IndexOperation indexOperation) {
        this.indexOperation = indexOperation;
    }

    public PrimaryKeyMatchOutcome matchesRawPrimaryKey(Bytes key, StatementEvaluationContext context) throws StatementExecutionException {
        return PrimaryKeyMatchOutcome.NEED_FULL_RECORD_EVALUATION;
    }

    public enum PrimaryKeyMatchOutcome {
        FAILED,
        NEED_FULL_RECORD_EVALUATION,
        FULL_CONDITION_VERIFIED
    }

}
