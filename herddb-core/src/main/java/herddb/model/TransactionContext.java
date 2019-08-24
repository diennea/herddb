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
 * Reference context for Transaction
 *
 * @author enrico.olivelli
 */
public class TransactionContext {

    public final long transactionId;

    public TransactionContext(long transactionId) {
        this.transactionId = transactionId;
    }

    public static final long AUTOTRANSACTION_ID = -1;
    public static final long NOTRANSACTION_ID = 0;

    public static final TransactionContext NO_TRANSACTION = new TransactionContext(NOTRANSACTION_ID);

    public static final TransactionContext AUTOTRANSACTION_TRANSACTION = new TransactionContext(AUTOTRANSACTION_ID);

    @Override
    public String toString() {
        return "TransactionContext{" + "transactionId=" + transactionId + '}';
    }

}
