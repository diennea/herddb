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
package herddb.network;

import herddb.utils.DataAccessor;
import herddb.utils.TuplesList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A message (from broker to worker or from worker to broker)
 *
 * @author enrico.olivelli
 */
public final class Message {

    public static Message ACK() {
        return new Message(TYPE_ACK, Collections.emptyMap());
    }

    public static Message ERROR(Throwable error) {
        Map<String, Object> params = new HashMap<>();
        params.put("error", error + "");
        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));
        params.put("stackTrace", writer.toString());
        return new Message(TYPE_ERROR, params);
    }

    public static Message CLIENT_CONNECTION_REQUEST(String secret) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("challenge", HashUtils.sha1(ts + "#" + secret));
        return new Message(TYPE_CLIENT_CONNECTION_REQUEST, data);
    }

    public static Message CLIENT_SHUTDOWN() {
        return new Message(TYPE_CLIENT_SHUTDOWN, new HashMap<>());
    }

    public static Message EXECUTE_STATEMENT(String tableSpace, String query, long tx,
            boolean returnValues,
            List<Object> params) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("tableSpace", tableSpace);
        if (tx != 0) {
            data.put("tx", tx);
        }
        if (returnValues) {
            data.put("returnValues", Boolean.TRUE);
        }
        data.put("query", query);
        if (params != null && !params.isEmpty()) {
            data.put("params", params);
        }
        return new Message(TYPE_EXECUTE_STATEMENT, data);
    }

    public static Message TX_COMMAND(String tableSpace, int type, long tx) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("tableSpace", tableSpace);
        if (tx > 0) { // begin does not carry a tx id
            data.put("tx", tx);
        }
        data.put("t", type);
        return new Message(TYPE_TX_COMMAND, data);
    }

    public static Message EXECUTE_STATEMENTS(String tableSpace, String query,
            long tx, boolean returnValues, List<List<Object>> params) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("tableSpace", tableSpace);
        if (tx != 0) {
            data.put("tx", tx);
        }
        if (returnValues) {
            data.put("returnValues", Boolean.TRUE);
        }
        data.put("query", query);
        data.put("params", params);
        return new Message(TYPE_EXECUTE_STATEMENTS, data);
    }

    public static Message OPEN_SCANNER(String tableSpace, String query, String scannerId, long tx, List<Object> params, int fetchSize, int maxRows) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("tableSpace", tableSpace);
        data.put("scannerId", scannerId);
        data.put("tx", tx);
        data.put("query", query);
        if (maxRows > 0) {
            data.put("maxRows", maxRows);
        }
        data.put("fetchSize", fetchSize);
        data.put("params", params);
        return new Message(TYPE_OPENSCANNER, data);
    }

    public static Message REQUEST_TABLESPACE_DUMP(String tableSpace, String dumpId, int fetchSize,
            boolean includeTransactionLog) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("dumpId", dumpId);
        data.put("includeTransactionLog", includeTransactionLog);
        data.put("fetchSize", fetchSize);
        data.put("tableSpace", tableSpace);
        return new Message(TYPE_REQUEST_TABLESPACE_DUMP, data);
    }

    public static Message TABLESPACE_DUMP_DATA(String tableSpace, String dumpId, Map<String, Object> values) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("dumpId", dumpId);
        data.put("values", values);
        return new Message(TYPE_TABLESPACE_DUMP_DATA, data);
    }

    public static Message RESULTSET_CHUNK(String scannerId, TuplesList tuplesList, boolean last, long tx) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("data", tuplesList);
        data.put("scannerId", scannerId);
        data.put("last", last);
        data.put("tx", tx);
        return new Message(TYPE_RESULTSET_CHUNK, data);
    }

    public static Message CLOSE_SCANNER(String scannerId) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("scannerId", scannerId);
        return new Message(TYPE_CLOSESCANNER, data);
    }

    public static Message FETCH_SCANNER_DATA(String scannerId, int fetchSize) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("scannerId", scannerId);
        data.put("fetchSize", fetchSize);
        return new Message(TYPE_FETCHSCANNERDATA, data);
    }

    public static Message EXECUTE_STATEMENT_RESULT(long updateCount, Map<String, Object> otherdata, long tx) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("updateCount", updateCount);
        if (otherdata != null && !otherdata.isEmpty()) {
            data.put("data", otherdata);
        }
        data.put("tx", tx);
        return new Message(TYPE_EXECUTE_STATEMENT_RESULT, data);
    }

    public static Message EXECUTE_STATEMENT_RESULTS(List<Long> updateCounts, List<Map<String, Object>> otherdata, long tx) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("updateCount", updateCounts);
        data.put("data", otherdata);
        data.put("tx", tx);
        return new Message(TYPE_EXECUTE_STATEMENTS_RESULT, data);
    }

    public static Message SASL_TOKEN_MESSAGE_REQUEST(String saslMech, byte[] firstToken) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("mech", saslMech);
        data.put("token", firstToken);
        return new Message(TYPE_SASL_TOKEN_MESSAGE_REQUEST, data);
    }

    public static Message SASL_TOKEN_SERVER_RESPONSE(byte[] saslTokenChallenge) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("token", saslTokenChallenge);
        return new Message(TYPE_SASL_TOKEN_SERVER_RESPONSE, data);
    }

    public static Message SASL_TOKEN_MESSAGE_TOKEN(byte[] token) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("token", token);
        return new Message(TYPE_SASL_TOKEN_MESSAGE_TOKEN, data);
    }

    public static Message REQUEST_TABLE_RESTORE(String tableSpace, byte[] table,
            long dumpLedgerId, long dumpOffset) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("table", table);
        data.put("dumpLedgerId", dumpLedgerId);
        data.put("dumpOffset", dumpOffset);
        data.put("tableSpace", tableSpace);
        return new Message(TYPE_REQUEST_TABLE_RESTORE, data);
    }

    public static Message TABLE_RESTORE_FINISHED(String tableSpace, String table,
            List<byte[]> indexes) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("table", table);
        data.put("tableSpace", tableSpace);
        data.put("indexes", indexes);
        return new Message(TYPE_TABLE_RESTORE_FINISHED, data);
    }

    public static Message RESTORE_FINISHED(String tableSpace) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("tableSpace", tableSpace);
        return new Message(TYPE_RESTORE_FINISHED, data);
    }

    public static Message PUSH_TABLE_DATA(String tableSpace, String name, List<KeyValue> chunk) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("table", name);
        data.put("tableSpace", tableSpace);
        data.put("data", chunk);
        return new Message(TYPE_PUSH_TABLE_DATA, data);
    }

    public static Message PUSH_TXLOGCHUNK(String tableSpace, List<KeyValue> chunk) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("tableSpace", tableSpace);
        data.put("data", chunk);
        return new Message(TYPE_PUSH_TXLOGCHUNK, data);
    }

    public static Message PUSH_TRANSACTIONSBLOCK(String tableSpace, List<byte[]> chunk) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("tableSpace", tableSpace);
        data.put("data", chunk);
        return new Message(TYPE_PUSH_TRANSACTIONSBLOCK, data);
    }

    public final int type;
    public final Map<String, Object> parameters;
    public long messageId = -1;
    public long replyMessageId = -1;

    @Override
    public String toString() {
        return typeToString(type) + ", " + parameters;

    }

    public static final int TYPE_ACK = 1;
    public static final int TYPE_CLIENT_CONNECTION_REQUEST = 2;
    public static final int TYPE_CLIENT_SHUTDOWN = 3;
    public static final int TYPE_ERROR = 4;
    public static final int TYPE_EXECUTE_STATEMENT = 5;
    public static final int TYPE_EXECUTE_STATEMENT_RESULT = 6;
    public static final int TYPE_OPENSCANNER = 7;
    public static final int TYPE_RESULTSET_CHUNK = 8;
    public static final int TYPE_CLOSESCANNER = 9;
    public static final int TYPE_FETCHSCANNERDATA = 10;
    public static final int TYPE_REQUEST_TABLESPACE_DUMP = 11;
    public static final int TYPE_TABLESPACE_DUMP_DATA = 12;
    public static final int TYPE_REQUEST_TABLE_RESTORE = 13;
    public static final int TYPE_PUSH_TABLE_DATA = 14;
    public static final int TYPE_EXECUTE_STATEMENTS = 15;
    public static final int TYPE_EXECUTE_STATEMENTS_RESULT = 16;
    public static final int TYPE_PUSH_TXLOGCHUNK = 17;
    public static final int TYPE_TABLE_RESTORE_FINISHED = 19;
    public static final int TYPE_PUSH_TRANSACTIONSBLOCK = 20;
    public static final int TYPE_RESTORE_FINISHED = 23;

    public static final int TYPE_TX_COMMAND = 24;

    public static final int TX_COMMAND_ROLLBACK_TRANSACTION = 1;
    public static final int TX_COMMAND_COMMIT_TRANSACTION = 2;
    public static final int TX_COMMAND_BEGIN_TRANSACTION = 3;

    public static final int TYPE_SASL_TOKEN_MESSAGE_REQUEST = 100;
    public static final int TYPE_SASL_TOKEN_SERVER_RESPONSE = 101;
    public static final int TYPE_SASL_TOKEN_MESSAGE_TOKEN = 102;

    public static String typeToString(int type) {
        switch (type) {
            case TYPE_ACK:
                return "ACK";
            case TYPE_ERROR:
                return "ERROR";
            case TYPE_CLIENT_CONNECTION_REQUEST:
                return "CLIENT_CONNECTION_REQUEST";
            case TYPE_CLIENT_SHUTDOWN:
                return "CLIENT_SHUTDOWN";
            case TYPE_EXECUTE_STATEMENT:
                return "EXECUTE_STATEMENT";
            case TYPE_TX_COMMAND:
                return "TX_COMMAND";
            case TYPE_EXECUTE_STATEMENT_RESULT:
                return "EXECUTE_STATEMENT_RESULT";
            case TYPE_EXECUTE_STATEMENTS:
                return "EXECUTE_STATEMENTS";
            case TYPE_EXECUTE_STATEMENTS_RESULT:
                return "EXECUTE_STATEMENTS_RESULT";
            case TYPE_OPENSCANNER:
                return "OPENSCANNER";
            case TYPE_RESULTSET_CHUNK:
                return "RESULTSET_CHUNK";
            case TYPE_CLOSESCANNER:
                return "CLOSESCANNER";
            case TYPE_FETCHSCANNERDATA:
                return "FETCHSCANNERDATA";
            case TYPE_REQUEST_TABLESPACE_DUMP:
                return "REQUEST_TABLESPACE_DUMP";
            case TYPE_TABLESPACE_DUMP_DATA:
                return "TABLESPACE_DUMP_DATA";
            case TYPE_SASL_TOKEN_MESSAGE_REQUEST:
                return "SASL_TOKEN_MESSAGE_REQUEST";
            case TYPE_SASL_TOKEN_SERVER_RESPONSE:
                return "SASL_TOKEN_SERVER_RESPONSE";
            case TYPE_SASL_TOKEN_MESSAGE_TOKEN:
                return "SASL_TOKEN_MESSAGE_TOKEN";
            default:
                return "?" + type;
        }
    }

    public Message(int type, Map<String, Object> parameters) {
        this.type = type;
        this.parameters = parameters;
    }

    public long getMessageId() {
        return messageId;
    }

    private static final AtomicLong MESSAGE_ID_GENERATOR = new AtomicLong();

    public void assignMessageId() {
        this.messageId = MESSAGE_ID_GENERATOR.incrementAndGet();
    }

    public Message setMessageId(long messageId) {
        this.messageId = messageId;
        return this;
    }

    public long getReplyMessageId() {
        return replyMessageId;
    }

    public Message setReplyMessageId(long replyMessageId) {
        this.replyMessageId = replyMessageId;
        return this;
    }

    public Message setParameter(String key, Object value) {
        this.parameters.put(key, value);
        return this;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + (int) (this.messageId ^ (this.messageId >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Message other = (Message) obj;
        if (this.type != other.type) {
            return false;
        }
        if (this.messageId != other.messageId) {
            return false;
        }
        if (this.replyMessageId != other.replyMessageId) {
            return false;
        }
        if (!Objects.equals(this.parameters, other.parameters)) {
            return false;
        }
        return true;
    }

}
