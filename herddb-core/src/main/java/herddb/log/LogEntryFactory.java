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

import herddb.model.Table;
import herddb.model.Transaction;
import java.nio.charset.StandardCharsets;

/**
 * Factory for entries
 *
 * @author enrico.olivelli
 */
public class LogEntryFactory {

    private static byte[] bytes(String name) {
        return name.getBytes(StandardCharsets.UTF_8);
    }

    public static LogEntry createTable(Table table, Transaction transaction) {
        byte[] payload = table.serialize();
        return new LogEntry(System.currentTimeMillis(), LogEntryType.CREATE_TABLE, bytes(table.tablespace), transaction != null ? transaction.transactionId : 0, null, null, payload);
    }

    public static LogEntry beginTransaction(String tablespace, long transactionId) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.BEGINTRANSACTION, bytes(tablespace), transactionId, null, null, null);
    }

    public static LogEntry commitTransaction(String tablespace, long transactionId) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.COMMITTRANSACTION, bytes(tablespace), transactionId, null, null, null);
    }

    public static LogEntry rollbackTransaction(String tablespace, long transactionId) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.ROLLBACKTRANSACTION, bytes(tablespace), transactionId, null, null, null);
    }

    public static LogEntry insert(Table table, byte[] key, byte[] value, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.INSERT, bytes(table.tablespace), transaction != null ? transaction.transactionId : 0, bytes(table.name), key, value);
    }

    public static LogEntry update(Table table, byte[] key, byte[] value, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.UPDATE, bytes(table.tablespace), transaction != null ? transaction.transactionId : 0, bytes(table.name), key, value);
    }

    public static LogEntry delete(Table table, byte[] key, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DELETE, bytes(table.tablespace), transaction != null ? transaction.transactionId : 0, bytes(table.name), key, null);
    }

}
