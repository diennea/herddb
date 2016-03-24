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

import herddb.model.Predicate;
import herddb.model.TableAwareStatement;
import herddb.utils.Bytes;

/**
 * Lookup a sigle record by primary key, eventually with a condition
 *
 * @author enrico.olivelli
 */
public class GetStatement extends TableAwareStatement {

    private final Bytes key;
    private final Predicate predicate;

    public GetStatement(String tableSpace, String table, Bytes key, Predicate predicate) {
        super(table, tableSpace);
        this.key = key;
        this.predicate = predicate;
    }

    public Bytes getKey() {
        return key;
    }

    public Predicate getPredicate() {
        return predicate;
    }

}
