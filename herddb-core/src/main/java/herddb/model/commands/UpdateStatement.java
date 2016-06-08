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

import herddb.model.DMLStatement;
import herddb.model.Predicate;
import herddb.model.Record;
import herddb.model.RecordFunction;
import herddb.model.ConstValueRecordFunction;

/**
 * Update an existing record
 *
 * @author enrico.olivelli
 */
public class UpdateStatement extends DMLStatement {

    private final RecordFunction function;
    private final RecordFunction key;
    private final Predicate predicate;

    public UpdateStatement(String tableSpace, String table, Record record, Predicate predicate) {
        this(tableSpace, table, new ConstValueRecordFunction(record.key.data), new ConstValueRecordFunction(record.value.data), predicate);
    }

    public UpdateStatement(String tableSpace, String table, RecordFunction key, RecordFunction function, Predicate predicate) {
        super(table, tableSpace);
        this.function = function;
        this.predicate = predicate;
        this.key = key;
    }

    public RecordFunction getFunction() {
        return function;
    }

    public RecordFunction getKey() {
        return key;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return "UpdateStatement "+System.identityHashCode(this)+" {" + "function=" + function + ", key=" + key + ", predicate=" + predicate + '}';
    }

}
