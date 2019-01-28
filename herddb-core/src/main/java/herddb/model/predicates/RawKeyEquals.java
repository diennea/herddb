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
package herddb.model.predicates;

import herddb.index.PrimaryIndexSeek;
import herddb.model.ConstValueRecordFunction;
import herddb.model.Predicate;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.utils.Bytes;

/**
 * Very raw predicate which acts on serialized key
 *
 * @author enrico.olivelli
 */
public final class RawKeyEquals extends Predicate {

    private final Bytes value;

    public RawKeyEquals(Bytes value) {
        this.value = value;
        setIndexOperation(new PrimaryIndexSeek(new ConstValueRecordFunction(value)));
    }

    @Override
    public boolean evaluate(Record record, StatementEvaluationContext context) throws StatementExecutionException {
        return record.key.equals(value);
    }

    @Override
    public PrimaryKeyMatchOutcome matchesRawPrimaryKey(Bytes key, StatementEvaluationContext context) throws StatementExecutionException {
        if (this.value.equals(key)) {
            return PrimaryKeyMatchOutcome.FULL_CONDITION_VERIFIED;
        } else {
            return PrimaryKeyMatchOutcome.FAILED;
        }
    }

}
