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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A message (from broker to worker or from worker to broker)
 *
 * @author enrico.olivelli
 */
public final class Message {

    public static Message ACK(String clientId) {
        return new Message(clientId, TYPE_ACK, new HashMap<>());
    }

    public static Message ERROR(String clientId, Throwable error) {
        Map<String, Object> params = new HashMap<>();
        params.put("error", error + "");
        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));
        params.put("stackTrace", writer.toString());
        return new Message(clientId, TYPE_ERROR, params);
    }

    public static Message CLIENT_CONNECTION_REQUEST(String clientId, String secret) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("challenge", HashUtils.sha1(ts + "#" + secret));
        return new Message(clientId, TYPE_CLIENT_CONNECTION_REQUEST, data);
    }

    public static Message CLIENT_SHUTDOWN(String clientId) {
        return new Message(clientId, TYPE_CLIENT_SHUTDOWN, new HashMap<>());
    }

    public static Message EXECUTE_STATEMENT(String clientId, String query, long tx, List<Object> params) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("tx", tx);
        data.put("query", query);
        data.put("params", params);
        return new Message(clientId, TYPE_EXECUTE_STATEMENT, data);
    }

    public static Message OPEN_SCANNER(String clientId, String query, String scannerId, List<Object> params, int fetchSize, int maxRows) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("scannerId", scannerId);
        data.put("query", query);
        if (maxRows > 0) {
            data.put("maxRows", maxRows);
        }
        data.put("fetchSize", fetchSize);
        data.put("params", params);
        return new Message(clientId, TYPE_OPENSCANNER, data);
    }

    public static Message RESULTSET_CHUNK(String clientId, String scannerId, List<Map<String, Object>> records) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("scannerId", scannerId);
        data.put("records", records);
        return new Message(clientId, TYPE_RESULTSET_CHUNK, data);
    }

    public static Message CLOSE_SCANNER(String clientId, String scannerId) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("scannerId", scannerId);
        return new Message(clientId, TYPE_CLOSESCANNER, data);
    }

    public static Message FETCH_SCANNER_DATA(String clientId, String scannerId, int fetchSize) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("scannerId", scannerId);
        data.put("fetchSize", fetchSize);
        return new Message(clientId, TYPE_FETCHSCANNERDATA, data);
    }

    public static Message EXECUTE_STATEMENT_RESULT(long updateCount, Map<String, Object> otherdata) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("updateCount", updateCount);
        data.put("data", otherdata);
        return new Message(null, TYPE_EXECUTE_STATEMENT_RESULT, data);
    }

    public final String clientId;
    public final int type;
    public final Map<String, Object> parameters;
    public String messageId;
    public String replyMessageId;

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
            case TYPE_EXECUTE_STATEMENT_RESULT:
                return "EXECUTE_STATEMENT_RESULT";
            case TYPE_OPENSCANNER:
                return "OPENSCANNER";
            case TYPE_RESULTSET_CHUNK:
                return "RESULTSET_CHUNK";
            case TYPE_CLOSESCANNER:
                return "CLOSESCANNER";
            case TYPE_FETCHSCANNERDATA:
                return "FETCHSCANNERDATA";
            default:
                return "?" + type;
        }
    }

    public Message(String workerProcessId, int type, Map<String, Object> parameters) {
        this.clientId = workerProcessId;
        this.type = type;
        this.parameters = parameters;
    }

    public String getMessageId() {
        return messageId;
    }

    public Message setMessageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    public String getReplyMessageId() {
        return replyMessageId;
    }

    public Message setReplyMessageId(String replyMessageId) {
        this.replyMessageId = replyMessageId;
        return this;
    }

    public Message setParameter(String key, Object value) {
        this.parameters.put(key, value);
        return this;
    }
}
