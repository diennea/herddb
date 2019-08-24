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
import herddb.model.Index;
import herddb.model.Table;
import java.util.Collections;
import java.util.List;

/**
 * Create a Table
 *
 * @author enrico.olivelli
 */
public class CreateTableStatement extends DDLStatement {

    private final Table tableDefinition;
    private final List<Index> additionalIndexes;

    public CreateTableStatement(Table tableDefinition) {
        super(tableDefinition.tablespace);
        this.tableDefinition = tableDefinition;
        this.additionalIndexes = Collections.emptyList();
    }

    public CreateTableStatement(Table tableDefinition, List<Index> additionalIndexes) {
        super(tableDefinition.tablespace);
        this.tableDefinition = tableDefinition;
        this.additionalIndexes = additionalIndexes;
    }

    @Override
    public boolean supportsTransactionAutoCreate() {
        /* This instruction will autocreate a transaction if issued */
        return true;
    }

    public Table getTableDefinition() {
        return tableDefinition;
    }

    public List<Index> getAdditionalIndexes() {
        return additionalIndexes;
    }

}
