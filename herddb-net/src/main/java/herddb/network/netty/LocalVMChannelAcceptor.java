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

import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ServerSideConnectionAcceptor;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;

/**
 * Accepts in-process client connections.
 */
public class LocalVMChannelAcceptor {
    private ServerSideConnectionAcceptor acceptor;
    private Set<LocalVMChannel> channels = new ConcurrentSkipListSet<>();

    public ServerSideConnectionAcceptor getAcceptor() {
        return acceptor;
    }

    public void setAcceptor(ServerSideConnectionAcceptor acceptor) {
        this.acceptor = acceptor;
    }

    public void close() {
        for (LocalVMChannel channel : channels) {
            channel.close();
        }
    }

    public Channel connect(String name, ChannelEventListener clientSidePeer, ExecutorService executorService) {
        LocalVMChannel channel = new LocalVMChannel(name, clientSidePeer, executorService, this);
        acceptor.createConnection(channel.getServerSideChannel());
        channels.add(channel);
        return channel;
    }

    void deregister(LocalVMChannel channel) {
        channels.remove(channel);
    }

    // Visible for testing
    Set<LocalVMChannel> channels() {
        return channels;
    }

}
