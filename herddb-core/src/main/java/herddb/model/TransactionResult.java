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

/**
 * Result of an action on a transaction
 *
 * @author enrico.olivelli
 */
public class TransactionResult extends StatementExecutionResult {

    private final OutcomeType outcome;

    public enum OutcomeType {
        BEGIN,
        COMMIT,
        ROLLBACK
    }

    public TransactionResult(long transactionId, OutcomeType outcome) {
        super(transactionId);
        this.outcome = outcome;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public OutcomeType getOutcome() {
        return outcome;
    }

    @Override
    public String toString() {
        return "TransactionResult{" + "transactionId=" + transactionId + ", outcome=" + outcome + '}';
    }

}
