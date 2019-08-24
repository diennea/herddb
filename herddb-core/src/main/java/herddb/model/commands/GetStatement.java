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

package herddb.model.commands;

import herddb.model.ConstValueRecordFunction;
import herddb.model.Predicate;
import herddb.model.RecordFunction;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableAwareStatement;
import herddb.utils.Bytes;

/**
 * Lookup a sigle record by primary key, eventually with a condition
 *
 * @author enrico.olivelli
 */
public class GetStatement extends TableAwareStatement {

    private final RecordFunction key;
    private final Predicate predicate;
    private final boolean requireLock;

    public GetStatement(String tableSpace, String table, Bytes key, Predicate predicate, boolean requireLock) {
        super(table, tableSpace);
        this.key = new ConstValueRecordFunction(key);
        this.predicate = predicate;
        this.requireLock = requireLock;
    }

    public GetStatement(String tableSpace, String table, RecordFunction key, Predicate predicate, boolean requireLock) {
        super(table, tableSpace);
        this.key = key;
        this.predicate = predicate;
        this.requireLock = requireLock;
    }

    public RecordFunction getKey() {
        return key;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public boolean isRequireLock() {
        return requireLock;
    }

    @Override
    public void validateContext(StatementEvaluationContext context) throws StatementExecutionException {
        if (predicate != null) {
            predicate.validateContext(context);
        }
    }


}
