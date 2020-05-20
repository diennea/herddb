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
import herddb.model.DMLStatement;
import herddb.model.Record;
import herddb.model.RecordFunction;

/**
 * Insert a new record
 *
 * @author enrico.olivelli
 */
public final class InsertStatement extends DMLStatement {

    private final RecordFunction keyFunction;
    private final RecordFunction valuesFunction;
    private final boolean upsert;

    public InsertStatement(String tableSpace, String table, Record record) {
        super(table, tableSpace);
        this.keyFunction = new ConstValueRecordFunction(record.key);
        this.valuesFunction = new ConstValueRecordFunction(record.value);
        this.upsert = false;
    }

    public InsertStatement(String tableSpace, String table, RecordFunction keyFunction, RecordFunction function, boolean upsert) {
        super(table, tableSpace);
        this.keyFunction = keyFunction;
        this.valuesFunction = function;
        this.upsert = upsert;
    }

    public RecordFunction getKeyFunction() {
        return keyFunction;
    }

    public RecordFunction getValuesFunction() {
        return valuesFunction;
    }

    public boolean isUpsert() {
        return upsert;
    }

    @Override
    public String toString() {
        return String.format("{InsertStatement={ keyfunction=%s valuesFunction=%s}}",
                             keyFunction.toString(), valuesFunction.toString());
    }
}
