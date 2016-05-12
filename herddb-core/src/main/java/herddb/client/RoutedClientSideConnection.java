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

import herddb.client.impl.ClientSideScannerPeer;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.Message;
import herddb.network.netty.NettyConnector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    private Channel channel;
    private Map<String, ClientSideScannerPeer> scanners = new ConcurrentHashMap<>();
    private final int fetchSize = 10;

    public RoutedClientSideConnection(HDBConnection connection, String nodeId) {
        this.connection = connection;
        this.nodeId = nodeId;
        this.connector = new NettyConnector(this);
        this.timeout = connection.getClient().getConfiguration().getLong(ClientConfiguration.PROPERTY_TIMEOUT, ClientConfiguration.PROPERTY_TIMEOUT_DEFAULT);
        this.clientId = connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENTID, ClientConfiguration.PROPERTY_CLIENTID_DEFAULT);
    }

    @Override
    public void messageReceived(Message message) {
        LOGGER.log(Level.SEVERE, "{0} - received {1}", new Object[]{nodeId, message.toString()});
    }

    @Override
    public void channelClosed() {
        channel = null;
    }

    @Override
    public void close() {
        LOGGER.log(Level.SEVERE, "{0} - close");
        this.connector.close();
        this.connection.releaseRoute(nodeId);
    }

    private void ensureOpen() throws HDBException {
        connectionLock.lock();
        try {
            if (channel == null) {
                channel = connector.connect();
            }
        } catch (Exception err) {
            throw new HDBException(err);
        } finally {
            connectionLock.unlock();
        }
    }

    DMLResult executeUpdate(String query, long tx, List<Object> params) throws HDBException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, query, tx, params);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
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

    Map<String, Object> executeGet(String query, long tx, List<Object> params) throws HDBException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, query, tx, params);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
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

    long beginTransaction(String tableSpace) throws HDBException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, "EXECUTE BEGINTRANSACTION '" + tableSpace + "'", 0, Collections.emptyList());
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                throw new HDBException(reply + "");
            }
            Map<String, Object> data = (Map<String, Object>) reply.parameters.get("data");
            return (Long) data.get("tx");
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void commitTransaction(String tableSpace, long tx) throws HDBException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, "EXECUTE COMMITTRANSACTION '" + tableSpace + "'," + tx, 0, Collections.emptyList());
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                throw new HDBException(reply + "");
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void rollbackTransaction(String tableSpace, long tx) throws HDBException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, "EXECUTE ROLLBACKTRANSACTION '" + tableSpace + "'," + tx, 0, Collections.emptyList());
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                throw new HDBException(reply + "");
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    ScanResultSet executeScan(String query, List<Object> params, int maxRows) throws HDBException {
        ensureOpen();
        Channel _channel = channel;
        if (_channel == null) {
            throw new HDBException("not connected to node " + nodeId);
        }
        try {
            String scannerId = UUID.randomUUID().toString();
            Message message = Message.OPEN_SCANNER(clientId, query, scannerId, params, fetchSize, maxRows);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                throw new HDBException(reply + "");
            }
            List<Map<String, Object>> initialFetchBuffer = (List<Map<String, Object>>) reply.parameters.get("records");
            List<String> columnNames = (List<String>) reply.parameters.get("columns");
            LOGGER.log(Level.SEVERE, "received first " + initialFetchBuffer.size() + " records for query " + query);
            ScanResultSetImpl impl = new ScanResultSetImpl(scannerId, columnNames, initialFetchBuffer);
            ClientSideScannerPeer scanner = new ClientSideScannerPeer(scannerId, impl);
            scanners.put(scannerId, scanner);
            return impl;
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    private class ScanResultSetImpl extends ScanResultSet {

        private final String scannerId;
        private final ScanResultSetMetadata metadata;

        private ScanResultSetImpl(String scannerId, List<String> columns, List<Map<String, Object>> fetchBuffer) {
            this.scannerId = scannerId;
            this.metadata = new ScanResultSetMetadata(columns);
            this.fetchBuffer.addAll(fetchBuffer);
            if (fetchBuffer.isEmpty()) {
                // empty result set
                finished = true;
                noMoreData = true;
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

        @Override
        public void close() {
            finished = true;
        }

        @Override
        public boolean hasNext() throws HDBException {
            LOGGER.log(Level.SEVERE, "hasNext");
            if (finished) {
                return false;
            }
            return ensureNext();
        }

        private void fillBuffer() throws HDBException {
            LOGGER.log(Level.SEVERE, "fillBuffer");
            fetchBuffer.clear();
            ensureOpen();
            Channel _channel = channel;
            if (_channel == null) {
                throw new HDBException("not connected to node " + nodeId);
            }
            try {
                Message result = _channel.sendMessageWithReply(Message.FETCH_SCANNER_DATA(clientId, scannerId, fetchSize), 10000);
                LOGGER.log(Level.SEVERE, "fillBuffer result " + result);
                if (result.type == Message.TYPE_ERROR) {
                    throw new HDBException("server side scanner error: " + result.parameters);
                }
                if (result.type != Message.TYPE_RESULTSET_CHUNK) {
                    finished = true;
                    throw new HDBException("protocol error: " + result);
                }
                List<Map<String, Object>> records = (List<Map<String, Object>>) result.parameters.get("records");
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
            LOGGER.log(Level.SEVERE, "ensureNext " + next);
            if (next != null) {
                return true;
            }

            LOGGER.log(Level.SEVERE, "ensureNext...position " + bufferPosition);
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
            LOGGER.log(Level.SEVERE, "next");
            if (finished) {
                throw new HDBException("Scanner is exhausted");
            }
            Map<String, Object> _next = next;
            next = null;
            return _next;
        }

    }

}
