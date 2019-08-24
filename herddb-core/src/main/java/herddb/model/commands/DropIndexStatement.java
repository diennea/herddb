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

import herddb.model.DDLStatement;

/**
 * Drop table
 *
 * @author enrico.olivelli
 */
public class DropIndexStatement extends DDLStatement {

    private final String indexName;
    private final boolean ifExists;

    public DropIndexStatement(String tableSpace, String indexName, boolean ifExists) {
        super(tableSpace);
        this.indexName = indexName;
        this.ifExists = ifExists;
    }

    @Override
    public boolean supportsTransactionAutoCreate() {
        /* This instruction will autocreate a transaction if issued */
        return true;
    }

    public String getIndexName() {
        return indexName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

}
