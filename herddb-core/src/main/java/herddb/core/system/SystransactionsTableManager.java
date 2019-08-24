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

package herddb.core.system;

import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import java.util.ArrayList;
import java.util.List;

/**
 * Table Manager for the SYSTRANSACTIONS virtual table
 *
 * @author enrico.olivelli
 */
public class SystransactionsTableManager extends AbstractSystemTableManager {

    private static final Table TABLE = Table
            .builder()
            .name("systransactions")
            .column("tablespace", ColumnTypes.STRING)
            .column("txid", ColumnTypes.LONG)
            .column("creationTimestamp", ColumnTypes.TIMESTAMP)
            .primaryKey("txid", false)
            .build();

    public SystransactionsTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList() {
        List<Transaction> transactions = tableSpaceManager.getTransactions();
        List<Record> result = new ArrayList<>();
        for (Transaction tx : transactions) {

            result.add(RecordSerializer.makeRecord(
                    table,
                    "tablespace", tableSpaceManager.getTableSpaceName(),
                    "txid", tx.transactionId,
                    "creationtimestamp", new java.sql.Timestamp(tx.localCreationTimestamp)
            ));
        }
        return result;
    }

}
