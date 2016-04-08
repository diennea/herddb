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
package herddb.server;

import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.Message;
import herddb.network.ServerSideConnection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles a client Connection
 *
 * @author enrico.olivelli
 */
public class ServerSideConnectionPeer implements ServerSideConnection, ChannelEventListener {

    private static final AtomicLong IDGENERATOR = new AtomicLong();
    private final long id = IDGENERATOR.incrementAndGet();
    private final Channel channel;
    private final Server server;

    public ServerSideConnectionPeer(Channel channel, Server server) {
        this.channel = channel;
        this.channel.setMessagesReceiver(this);
        this.server = server;
    }

    @Override
    public long getConnectionId() {
        return id;
    }

    @Override
    public void messageReceived(Message message) {

    }

    @Override
    public void channelClosed() {
        this.server.connectionClosed(this);
    }

}
