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
 * Result of the lookup of a single record
 *
 * @author enrico.olivelli
 */
public class GetResult extends StatementExecutionResult {

    private final Record record;
    private final Table table;

    public GetResult(long transactionId, Record record, Table table) {
        super(transactionId);
        this.record = record;
        this.table = table;
    }

    public static final GetResult NOT_FOUND(long transactionId) {
        return new GetResult(transactionId, null, null);
    }

    public Table getTable() {
        return table;
    }

    public Record getRecord() {
        return record;
    }

    public boolean found() {
        return record != null;
    }
}
