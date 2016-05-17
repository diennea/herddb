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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Connection on the client side
 *
 * @author enrico.olivelli
 */
public class HDBConnection implements AutoCloseable {

    private final Map<String, RoutedClientSideConnection> routes = new HashMap<>();
    private final AtomicLong IDGENERATOR = new AtomicLong();
    private final long id = IDGENERATOR.incrementAndGet();
    private final HDBClient client;
    private final ReentrantLock routesLock = new ReentrantLock(true);
    private volatile boolean closed;

    public HDBConnection(HDBClient client) {
        this.client = client;
    }

    public long getId() {
        return id;
    }

    public HDBClient getClient() {
        return client;
    }

    @Override
    public void close() {
        closed = true;
        List<RoutedClientSideConnection> routesAtClose = new ArrayList<>(routes.values());
        for (RoutedClientSideConnection route : routesAtClose) {
            route.close();
        }
        client.releaseConnection(this);
    }

    void releaseRoute(String nodeId) {
        routes.remove(nodeId);
    }

    public long beginTransaction(String tableSpace) throws ClientSideMetadataProviderException, HDBException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        return route.beginTransaction(tableSpace);
    }

    public void rollbackTransaction(String tableSpace, long tx) throws ClientSideMetadataProviderException, HDBException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        route.rollbackTransaction(tableSpace, tx);
    }

    public void commitTransaction(String tableSpace, long tx) throws ClientSideMetadataProviderException, HDBException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        route.commitTransaction(tableSpace, tx);
    }

    public DMLResult executeUpdate(String tableSpace, String query, long tx, List<Object> params) throws ClientSideMetadataProviderException, HDBException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        return route.executeUpdate(query, tx, params);
    }

    public Map<String, Object> executeGet(String tableSpace, String query, long tx, List<Object> params) throws ClientSideMetadataProviderException, HDBException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        return route.executeGet(query, tx, params);
    }

    public ScanResultSet executeScan(String tableSpace, String query, List<Object> params, int maxRows) throws ClientSideMetadataProviderException, HDBException, InterruptedException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        return route.executeScan(query, params, maxRows);
    }

    private RoutedClientSideConnection getRouteToServer(String nodeId) throws ClientSideMetadataProviderException, HDBException {
        routesLock.lock();
        try {
            RoutedClientSideConnection route = routes.get(nodeId);
            if (route == null) {
                route = new RoutedClientSideConnection(this, nodeId);
                routes.put(nodeId, route);
            }
            return route;
        } finally {
            routesLock.unlock();
        }
    }

    private RoutedClientSideConnection getRouteToTableSpace(String tableSpace) throws ClientSideMetadataProviderException, HDBException {
        if (closed) {
            throw new HDBException("connection is closed");
        }
        String leaderId = client.getClientSideMetadataProvider().getTableSpaceLeader(tableSpace);
        if (leaderId == null) {
            throw new HDBException("no such tablespace " + tableSpace);
        }
        return getRouteToServer(leaderId);
    }

    public boolean isClosed() {
        return closed;
    }

}
