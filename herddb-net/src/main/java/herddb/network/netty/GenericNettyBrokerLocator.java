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
package herddb.network.netty;

import herddb.network.ServerLocator;
import herddb.network.ServerNotAvailableException;
import herddb.network.ServerRejectedConnectionException;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ConnectionRequestInfo;
import herddb.network.Message;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;
import herddb.network.ServerHostData;

/**
 * Network connection, based on Netty
 *
 * @author enrico.olivelli
 */
public abstract class GenericNettyBrokerLocator implements ServerLocator {

    protected abstract ServerHostData getServer();

    protected int connectTimeout = 60000;
    protected int socketTimeout = 240000;

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @Override
    public Channel connect(ChannelEventListener messageReceiver, ConnectionRequestInfo workerInfo) throws InterruptedException, ServerNotAvailableException, ServerRejectedConnectionException {
        boolean ok = false;
        NettyConnector connector = new NettyConnector(messageReceiver);
        try {
            ServerHostData broker = getServer();
            if (broker == null) {
                throw new ServerNotAvailableException(new Exception("no broker available"));
            }
            InetSocketAddress addre = broker.getSocketAddress();
            connector.setPort(addre.getPort());
            connector.setHost(addre.getAddress().getHostAddress());
            connector.setConnectTimeout(connectTimeout);
            connector.setSocketTimeout(socketTimeout);
            connector.setSsl(broker.isSsl());
            NettyChannel channel;
            try {
                channel = connector.connect();
            } catch (final Exception e) {
                throw new ServerNotAvailableException(e);
            }

            Message acceptMessage = Message.CLIENT_CONNECTION_REQUEST(workerInfo.getClientId(), workerInfo.getSharedSecret());
            try {
                Message connectionResponse = channel.sendMessageWithReply(acceptMessage, 10000);
                if (connectionResponse.type == Message.TYPE_ACK) {
                    ok = true;
                    return channel;
                } else {
                    throw new ServerRejectedConnectionException("Server rejected connection, response message:" + connectionResponse);
                }
            } catch (TimeoutException err) {
                throw new ServerNotAvailableException(err);
            }
        } finally {
            if (!ok && connector != null) {
                connector.close();
            }
        }
    }
}
