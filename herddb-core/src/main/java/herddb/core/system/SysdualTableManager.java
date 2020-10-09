/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.core.system;

import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import java.util.Collections;
import java.util.List;

/**
 * Table Manager for the DUAL virtual table.
 * DUAL is a table automatically created by Oracle Database along with the data dictionary.
 * DUAL is in the schema of the user SYS but is accessible by the name DUAL to all users.
 * It has one column, DUMMY, defined to be VARCHAR2(1), and contains one row with a value X.
 * Selecting from the DUAL table is useful for computing a constant expression with the SELECT statement.
 * Because DUAL has only one row, the constant is returned only once.
 *
 * @author enrico.olivelli
 */
public class SysdualTableManager extends AbstractSystemTableManager {

    private static final Table TABLE = Table
            .builder()
            .name("dual")
            .column("dummy", ColumnTypes.STRING)
            .primaryKey("dummy")
            .build();
    private final List<Record> result;
        
    public SysdualTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
        this.result = Collections.singletonList(RecordSerializer.makeRecord(
                table,
                "dummy", "X"
        ));
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList(Transaction transaction) {
       return result;
    }

}
