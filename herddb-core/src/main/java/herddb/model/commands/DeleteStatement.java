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
import herddb.model.Predicate;
import herddb.model.RecordFunction;
import herddb.utils.Bytes;

/**
 * Delete an existing record, if the record does not exist the updateCount will
 * return 0
 *
 * @author enrico.olivelli
 */
public class DeleteStatement extends DMLStatement {

    private final RecordFunction keyFunction;
    private final Predicate predicate;

    public DeleteStatement(String tableSpace, String table, Bytes key, Predicate predicate) {
        super(table, tableSpace);
        this.keyFunction = new ConstValueRecordFunction(key.data);
        this.predicate = predicate;
    }

    public DeleteStatement(String tableSpace, String table, RecordFunction keyFunction, Predicate predicate) {
        super(table, tableSpace);
        this.keyFunction = keyFunction;
        this.predicate = predicate;
    }

    public RecordFunction getKeyFunction() {
        return keyFunction;
    }

    public Predicate getPredicate() {
        return predicate;
    }

}
