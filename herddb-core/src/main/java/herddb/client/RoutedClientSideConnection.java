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

import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.Message;
import herddb.network.netty.NettyConnector;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    long executeUpdate(String query, long tx, List<Object> params) throws HDBException {
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
            return (Long) reply.parameters.get("updateCount");
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
            Message message = Message.EXECUTE_STATEMENT(clientId, "EXECUTE BEGINTRANSACTION ?", 0, Arrays.asList(tableSpace));
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
            Message message = Message.EXECUTE_STATEMENT(clientId, "EXECUTE COMMITTRANSACTION ?,?", 0, Arrays.asList(tableSpace, tx));
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
            Message message = Message.EXECUTE_STATEMENT(clientId, "EXECUTE ROLLBACKTRANSACTION ?,?", 0, Arrays.asList(tableSpace, tx));
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                throw new HDBException(reply + "");
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

}
