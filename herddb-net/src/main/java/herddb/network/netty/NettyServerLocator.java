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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;
import herddb.network.ServerHostData;

/**
 * Created by enrico.olivelli on 24/03/2015.
 */
public class NettyServerLocator extends GenericNettyBrokerLocator {

    private final List<ServerHostData> servers = new ArrayList<>();
    private final AtomicInteger index = new AtomicInteger();

    public NettyServerLocator(ServerHostData serverHostData) {
        this(serverHostData.getHost(), serverHostData.getPort(), serverHostData.isSsl());
    }

    public NettyServerLocator(String host, int port, boolean ssl) {
        this.servers.add(new ServerHostData(host, port, "", ssl, new HashMap<String, String>()));
    }

    public NettyServerLocator(final List<InetSocketAddress> servers, boolean ssl) {
        servers.forEach(s -> {
            this.servers.add(new ServerHostData(s.getHostName(), s.getPort(), "", ssl, new HashMap<>()));
        });

    }

    public NettyServerLocator(String host, int port, String host2, int port2) {
        this(Arrays.asList(new InetSocketAddress(host, port), new InetSocketAddress(host2, port2)), false);
    }

    @Override
    protected ServerHostData getServer() {
        return servers.get(index.get() % servers.size());
    }

    @Override
    public void brokerDisconnected() {
        index.incrementAndGet();
    }

    @Override
    public String toString() {
        return "NettyCacheServerLocator{" + "servers=" + servers + ", index=" + index + '}';
    }

}
