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

import herddb.model.Statement;
import herddb.model.planner.PlannerOp;

/**
 * Generic adapter for commands planned from SQL
 */
public class SQLPlannedOperationStatement extends Statement {

    private final PlannerOp rootOp;

    public SQLPlannedOperationStatement(PlannerOp rootOp) {
        super(rootOp.getTablespace());
        this.rootOp = rootOp;
    }

    public PlannerOp getRootOp() {
        return rootOp;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        T unwrapped = rootOp.unwrap(clazz);
        if (unwrapped != null) {
            return unwrapped;
        }
        return super.unwrap(clazz);
    }

}
