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

import herddb.utils.Bytes;

/**
 * Result of statement which mutate data
 *
 * @author enrico.olivelli
 */
public class DMLStatementExecutionResult extends StatementExecutionResult {

    private final int updateCount;
    private final Bytes key;
    private final Bytes newvalue;

    public DMLStatementExecutionResult(int updateCount) {
        this.updateCount = updateCount;
        this.key = null;
        this.newvalue = null;
    }

    public DMLStatementExecutionResult(int updateCount, Bytes key, Bytes newvalue) {
        this.updateCount = updateCount;
        this.key = key;
        this.newvalue = newvalue;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public Bytes getNewvalue() {
        return newvalue;
    }

    public Bytes getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "DMLStatementExecutionResult{" + "updateCount=" + updateCount + ", key=" + key + '}';
    }

}
