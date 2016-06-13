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
package herddb.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import herddb.client.impl.RetryRequestException;
import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.model.Table;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.KeyValue;
import herddb.network.Message;
import herddb.network.ServerHostData;
import herddb.network.netty.NettyConnector;
import herddb.security.sasl.SaslNettyClient;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A real connection to a server
 *
 * @author enrico.olivelli
 */
public class RoutedClientSideConnection implements AutoCloseable, ChannelEventListener {

    private Logger LOGGER = Logger.getLogger(RoutedClientSideConnection.class.getName());
    private final NettyConnector connector;
    private final HDBConnection connection;
    private final String nodeId;
    private final long timeout;
    private final String clientId;
    private final ReentrantLock connectionLock = new ReentrantLock(true);
    private volatile Channel channel;

    private final Map<String, TableSpaceDumpReceiver> dumpReceivers = new ConcurrentHashMap<>();

    public RoutedClientSideConnection(HDBConnection connection, String nodeId) throws ClientSideMetadataProviderException {
        this.connection = connection;
        this.nodeId = nodeId;

        this.connector = new NettyConnector(this);

        final ServerHostData server = connection.getClient().getClientSideMetadataProvider().getServerHostData(nodeId);

        this.connector.setHost(server.getHost());
        this.connector.setPort(server.getPort());
        this.connector.setSsl(server.isSsl());

        this.timeout = connection.getClient().getConfiguration().getLong(ClientConfiguration.PROPERTY_TIMEOUT, ClientConfiguration.PROPERTY_TIMEOUT_DEFAULT);
        this.clientId = connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENTID, ClientConfiguration.PROPERTY_CLIENTID_DEFAULT);
    }

    private void performAuthentication() throws Exception {

        SaslNettyClient saslNettyClient = new SaslNettyClient(
                connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT),
                connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT)
        );
        Channel _channel = channel;
        Message saslResponse = _channel.sendMessageWithReply(Message.SASL_TOKEN_MESSAGE_REQUEST(), timeout);

        while (true) {
            byte[] responseToSendToServer;
            if (saslResponse.type == Message.TYPE_SASL_TOKEN_SERVER_RESPONSE) {
                byte[] token = (byte[]) saslResponse.parameters.get("token");
                responseToSendToServer = saslNettyClient.saslResponse(token);
                if (saslNettyClient.isComplete()) {
                    LOGGER.finest("SASL auth completed with success");
                    break;
                }
            } else if (saslResponse.type == Message.TYPE_ERROR) {
                throw new Exception("Server returned ERROR during SASL negotiation, Maybe authentication failure (" + saslResponse.parameters + ")");
            } else {
                throw new Exception("Unexpected server response during SASL negotiation (" + saslResponse + ")");
            }
            saslResponse = _channel.sendMessageWithReply(Message.SASL_TOKEN_MESSAGE_TOKEN(responseToSendToServer), timeout);
        }
    }

    @Override
    public void messageReceived(Message message) {
        LOGGER.log(Level.SEVERE, "{0} - received {1}", new Object[]{nodeId, message.toString()});
        Channel _channel = channel;
        switch (message.type) {
            case Message.TYPE_TABLESPACE_DUMP_DATA: {
                String dumpId = (String) message.parameters.get("dumpId");
                TableSpaceDumpReceiver receiver = dumpReceivers.get(dumpId);
                LOGGER.log(Level.SEVERE, "receiver for " + dumpId + ": " + receiver);
                if (receiver == null) {
                    if (_channel != null) {
                        _channel.sendReplyMessage(message, Message.ERROR(clientId, new Exception("no such dump receiver " + dumpId)));
                    }
                    return;
                }
                try {
                    Map<String, Object> values = (Map<String, Object>) message.parameters.get("values");
                    String command = (String) values.get("command") + "";
                    boolean sendAck = true;
                    switch (command) {
                        case "start": {
                            long ledgerId = (long) values.get("ledgerid");
                            long offset = (long) values.get("offset");
                            receiver.start(new LogSequenceNumber(ledgerId, offset));
                            break;
                        }
                        case "beginTable": {
                            byte[] tableDefinition = (byte[]) values.get("table");
                            Table table = Table.deserialize(tableDefinition);
                            receiver.beginTable(table);
                            break;
                        }
                        case "endTable": {
                            receiver.endTable();
                            break;
                        }
                        case "finish": {
                            receiver.finish();
                            sendAck = false;
                            break;
                        }
                        case "data": {
                            List<KeyValue> data = (List<KeyValue>) values.get("records");
                            List<Record> records = new ArrayList<>(data.size());
                            for (KeyValue kv : data) {
                                records.add(new Record(new Bytes(kv.key), new Bytes(kv.value)));
                            }
                            receiver.receiveTableDataChunk(records);
                            break;
                        }
                    }
                    if (_channel != null && sendAck) {
                        _channel.sendReplyMessage(message, Message.ACK(clientId));
                    }
                } catch (DataStorageManagerException error) {
                    LOGGER.log(Level.SEVERE, "error while handling dump data", error);
                    if (_channel != null) {
                        _channel.sendReplyMessage(message, Message.ERROR(clientId, error));
                    }
                }
            }
            break;

        }
    }

    @Override
    public void channelClosed() {
        channel = null;
    }

    @Override
    public void close() {
        LOGGER.log(Level.SEVERE, "{0} - close", this);
        this.connector.close();
        this.connection.releaseRoute(nodeId);
        connectionLock.lock();
        try {
            if (channel != null) {
                channel.close();
            }
        } finally {
            channel = null;
            connectionLock.unlock();
        }
    }

    private void ensureOpen() throws HDBException {
        connectionLock.lock();
        try {
            if (channel == null) {
                LOGGER.log(Level.SEVERE, "{0} - connect", this);
                channel = connector.connect();
                performAuthentication();
            }
        } catch (Exception err) {
            throw new HDBException(err);
        } finally {
            connectionLock.unlock();
        }
    }

    DMLResult executeUpdate(String tableSpace, String query, long tx, List<Object> params) throws HDBException, ClientSideMetadataProviderException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, tableSpace, query, tx, params);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply + "");
            }
            long updateCount = (Long) reply.parameters.get("updateCount");

            Object key = null;
            Map<String, Object> data = (Map<String, Object>) reply.parameters.get("data");
            if (data != null) {
                key = data.get("key");
            }
            return new DMLResult(updateCount, key);
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    Map<String, Object> executeGet(String tableSpace, String query, long tx, List<Object> params) throws HDBException, ClientSideMetadataProviderException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, tableSpace, query, tx, params);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply + "");
            }
            long found = (Long) reply.parameters.get("updateCount");
            if (found <= 0) {
                return null;
            }
            return (Map<String, Object>) reply.parameters.get("data");
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    long beginTransaction(String tableSpace) throws HDBException, ClientSideMetadataProviderException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, tableSpace, "EXECUTE BEGINTRANSACTION '" + tableSpace + "'", 0, Collections.emptyList());
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply + "");
            }
            Map<String, Object> data = (Map<String, Object>) reply.parameters.get("data");
            return (Long) data.get("tx");
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void commitTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, tableSpace, "EXECUTE COMMITTRANSACTION '" + tableSpace + "'," + tx, 0, Collections.emptyList());
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply + "");
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void rollbackTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, tableSpace, "EXECUTE ROLLBACKTRANSACTION '" + tableSpace + "'," + tx, 0, Collections.emptyList());
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply + "");
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    private static final AtomicLong SCANNERID_GENERATOR = new AtomicLong();

    ScanResultSet executeScan(String tableSpace, String query, List<Object> params, long tx, int maxRows, int fetchSize) throws HDBException, ClientSideMetadataProviderException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            String scannerId = this.clientId + ":" + SCANNERID_GENERATOR.incrementAndGet();
            Message message = Message.OPEN_SCANNER(clientId, tableSpace, query, scannerId, tx, params, fetchSize, maxRows);
            LOGGER.log(Level.FINEST, "open scanner{0} for query {1}, params {2}", new Object[]{scannerId, query, params});
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply + "");
            }
            List<Map<String, Object>> initialFetchBuffer = (List<Map<String, Object>>) reply.parameters.get("records");
            List<String> columnNames = (List<String>) reply.parameters.get("columns");
            boolean last = (Boolean) reply.parameters.get("last");
            //LOGGER.log(Level.SEVERE, "received first " + initialFetchBuffer.size() + " records for query " + query);
            ScanResultSetImpl impl = new ScanResultSetImpl(scannerId, columnNames, initialFetchBuffer, fetchSize, last);

            return impl;
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void dumpTableSpace(String tableSpace, int fetchSize, TableSpaceDumpReceiver receiver) throws HDBException, ClientSideMetadataProviderException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            String dumpId = this.clientId + ":" + SCANNERID_GENERATOR.incrementAndGet();
            Message message = Message.REQUEST_TABLESPACE_DUMP(clientId, tableSpace, dumpId, fetchSize);
            LOGGER.log(Level.SEVERE, "dumpTableSpace id " + dumpId + " for tablespace " + tableSpace);
            dumpReceivers.put(dumpId, receiver);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            LOGGER.log(Level.SEVERE, "dumpTableSpace id " + dumpId + " for tablespace " + tableSpace + ": first reply " + reply.parameters);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply + "");
            }

        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    private class ScanResultSetImpl extends ScanResultSet {

        private final String scannerId;
        private final ScanResultSetMetadata metadata;

        private ScanResultSetImpl(String scannerId, List<String> columns, List<Map<String, Object>> fetchBuffer, int fetchSize, boolean onlyOneChunk) {
            this.scannerId = scannerId;
            this.metadata = new ScanResultSetMetadata(columns);
            this.fetchBuffer.addAll(fetchBuffer);
            this.fetchSize = fetchSize;
            if (fetchBuffer.isEmpty()) {
                // empty result set
                finished = true;
                noMoreData = true;
            }
            if (onlyOneChunk) {
                lastChunk = true;
            }
        }

        @Override
        public ScanResultSetMetadata getMetadata() {
            return metadata;
        }

        final List<Map<String, Object>> fetchBuffer = new ArrayList<>();
        Map<String, Object> next;
        boolean finished;
        boolean noMoreData;
        int bufferPosition;
        int fetchSize;
        boolean lastChunk;

        @Override
        public void close() {
            finished = true;
        }

        @Override
        public boolean hasNext() throws HDBException {
            if (finished) {
                return false;
            }
            return ensureNext();
        }

        private void fillBuffer() throws HDBException {
            if (lastChunk) {
                fetchBuffer.clear();
                noMoreData = true;
                bufferPosition = 0;
                return;
            }
            fetchBuffer.clear();
            ensureOpen();
            Channel _channel = channel;
            if (_channel == null) {
                throw new HDBException("not connected to node " + nodeId);
            }
            try {
                Message result = _channel.sendMessageWithReply(Message.FETCH_SCANNER_DATA(clientId, scannerId, fetchSize), 10000);
                //LOGGER.log(Level.SEVERE, "fillBuffer result " + result);
                if (result.type == Message.TYPE_ERROR) {
                    throw new HDBException("server side scanner error: " + result.parameters);
                }
                if (result.type != Message.TYPE_RESULTSET_CHUNK) {
                    finished = true;
                    throw new HDBException("protocol error: " + result);
                }
                List<Map<String, Object>> records = (List<Map<String, Object>>) result.parameters.get("records");
                lastChunk = (Boolean) result.parameters.get("last");
                if (records.isEmpty()) {
                    noMoreData = true;
                }
                fetchBuffer.addAll(records);
                bufferPosition = 0;
            } catch (InterruptedException | TimeoutException err) {
                throw new HDBException(err);
            }
        }

        private boolean ensureNext() throws HDBException {
            if (next != null) {
                return true;
            }
            if (bufferPosition == fetchBuffer.size()) {
                fillBuffer();
                if (noMoreData) {
                    finished = true;
                    return false;
                }
            }
            next = fetchBuffer.get(bufferPosition++);
            return true;
        }

        @Override
        public Map<String, Object> next() throws HDBException {
            if (finished) {
                throw new HDBException("Scanner is exhausted");
            }
            Map<String, Object> _next = next;
            next = null;
            return _next;
        }

    }

}
