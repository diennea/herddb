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

package herddb.log;

import com.google.common.primitives.Longs;
import herddb.model.Index;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.utils.Bytes;

/**
 * Factory for entries
 *
 * @author enrico.olivelli
 */
public class LogEntryFactory {

    public static LogEntry createTable(Table table, Transaction transaction) {
        byte[] payload = table.serialize();
        return new LogEntry(System.currentTimeMillis(), LogEntryType.CREATE_TABLE, transaction != null ? transaction.transactionId : 0, table.name, null, Bytes.from_array(payload));
    }

    public static LogEntry alterTable(Table table, Transaction transaction) {
        byte[] payload = table.serialize();
        return new LogEntry(System.currentTimeMillis(), LogEntryType.ALTER_TABLE, transaction != null ? transaction.transactionId : 0, table.name, null, Bytes.from_array(payload));
    }

    public static LogEntry dropTable(String table, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DROP_TABLE, transaction != null ? transaction.transactionId : 0, table, null, null);
    }

    public static LogEntry dropIndex(String indexName, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DROP_INDEX, transaction != null ? transaction.transactionId : 0, null, null, Bytes.from_string(indexName));
    }

    public static LogEntry beginTransaction(long transactionId) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.BEGINTRANSACTION, transactionId, null, null, null);
    }
    
    public static LogEntry dataIntegrity(String table,long transactionID,Bytes value){
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DATA_INTEGRITY, transactionID,table, null, value);
    }
    
    public static LogEntry commitTransaction(long transactionId) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.COMMITTRANSACTION, transactionId, null, null, null);
    }

    public static LogEntry rollbackTransaction(long transactionId) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.ROLLBACKTRANSACTION, transactionId, null, null, null);
    }

    public static LogEntry insert(Table table, Bytes key, Bytes value, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.INSERT, transaction != null ? transaction.transactionId : 0, table.name, key, value);
    }

    public static LogEntry update(Table table, Bytes key, Bytes value, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.UPDATE, transaction != null ? transaction.transactionId : 0, table.name, key, value);
    }

    public static LogEntry delete(Table table, Bytes key, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DELETE, transaction != null ? transaction.transactionId : 0, table.name, key, null);
    }

    public static LogEntry createIndex(Index index, Transaction transaction) {
        byte[] payload = index.serialize();
        return new LogEntry(System.currentTimeMillis(), LogEntryType.CREATE_INDEX, transaction != null ? transaction.transactionId : 0, index.table, null, Bytes.from_array(payload))
                ;
    }

    public static LogEntry truncate(Table table, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.TRUNCATE_TABLE,
                transaction != null ? transaction.transactionId : 0, table.name, null, null);
    }

    public static LogEntry noop() {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.NOOP,
                -1, null, null, null);
    }

}








