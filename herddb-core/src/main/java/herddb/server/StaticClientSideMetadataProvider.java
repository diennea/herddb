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

import herddb.client.ClientSideMetadataProvider;
import herddb.client.ClientSideMetadataProviderException;
import herddb.network.ServerHostData;
import java.util.HashMap;

/**
 * Static discovery of service
 *
 * @author enrico.olivelli
 */
public class StaticClientSideMetadataProvider implements ClientSideMetadataProvider {

    private final ServerHostData serverHostData;
    private final String nodeId;

    public StaticClientSideMetadataProvider(Server server) {
        this.serverHostData = server.getServerHostData();
        this.nodeId = server.getNodeId();
    }

    public StaticClientSideMetadataProvider(String host, int port, boolean ssl) {
        this.serverHostData = new ServerHostData(host, port, "", ssl, new HashMap<>());
        this.nodeId = host + ":" + port;
    }

    @Override
    public ServerHostData getServerHostData(String nodeId) throws ClientSideMetadataProviderException {
        return serverHostData;
    }

    @Override
    public String getTableSpaceLeader(String tableSpace) throws ClientSideMetadataProviderException {
        return nodeId;
    }

}
