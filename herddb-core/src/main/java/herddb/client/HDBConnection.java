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

import herddb.client.impl.RetryRequestException;
import static herddb.utils.QueryUtils.discoverTablespace;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    private boolean discoverTablespaceFromSql = true;

    public HDBConnection(HDBClient client) {
        this.client = client;
    }

    public boolean isDiscoverTablespaceFromSql() {
        return discoverTablespaceFromSql;
    }

    public void setDiscoverTablespaceFromSql(boolean discoverTablespaceFromSql) {
        this.discoverTablespaceFromSql = discoverTablespaceFromSql;
    }

    public long getId() {
        return id;
    }

    public HDBClient getClient() {
        return client;
    }

    @Override
    public void close() {
        LOGGER.log(Level.SEVERE, "{0} close ", this);
        closed = true;
        List<RoutedClientSideConnection> routesAtClose;
        routesLock.lock();
        try {
            routesAtClose = new ArrayList<>(routes.values());
        } finally {
            routesLock.unlock();
        }
        for (RoutedClientSideConnection route : routesAtClose) {
            route.close();
        }
        client.releaseConnection(this);
    }
    private static final Logger LOGGER = Logger.getLogger(HDBConnection.class.getName());

    void releaseRoute(String nodeId) {
        routesLock.lock();
        try {
            routes.remove(nodeId);
        } finally {
            routesLock.unlock();
        }
    }

    public long beginTransaction(String tableSpace) throws ClientSideMetadataProviderException, HDBException {
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.beginTransaction(tableSpace);
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                sleepOnRetry();
            }
        }
        throw new HDBException("client is closed");
    }

    public void rollbackTransaction(String tableSpace, long tx) throws ClientSideMetadataProviderException, HDBException {
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                route.rollbackTransaction(tableSpace, tx);
                return;
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                sleepOnRetry();
            }
        }
        throw new HDBException("client is closed");
    }

    public void commitTransaction(String tableSpace, long tx) throws ClientSideMetadataProviderException, HDBException {
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                route.commitTransaction(tableSpace, tx);
                return;
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                sleepOnRetry();
            }
        }
        throw new HDBException("client is closed");
    }

    public DMLResult executeUpdate(String tableSpace, String query, long tx, List<Object> params) throws ClientSideMetadataProviderException, HDBException {
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.executeUpdate(tableSpace, query, tx, params);
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                sleepOnRetry();
            }
        }
        throw new HDBException("client is closed");
    }

    public List<DMLResult> executeUpdates(String tableSpace, String query, long tx, List<List<Object>> batch) throws ClientSideMetadataProviderException, HDBException {
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.executeUpdates(tableSpace, query, tx, batch);
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                sleepOnRetry();
            }
        }
        throw new HDBException("client is closed");
    }

    public GetResult executeGet(String tableSpace, String query, long tx, List<Object> params) throws ClientSideMetadataProviderException, HDBException {
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.executeGet(tableSpace, query, tx, params);
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                sleepOnRetry();
            }
        }
        throw new HDBException("client is closed");
    }

    public ScanResultSet executeScan(String tableSpace, String query, List<Object> params, long tx, int maxRows, int fetchSize) throws ClientSideMetadataProviderException, HDBException, InterruptedException {
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.executeScan(tableSpace, query, params, tx, maxRows, fetchSize);
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                sleepOnRetry();
            }

        }
        throw new HDBException("client is closed");
    }

    private void sleepOnRetry() throws HDBException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException err) {
            throw new HDBException(err);
        }
    }

    public void dumpTableSpace(String tableSpace, TableSpaceDumpReceiver receiver, int fetchSize) throws ClientSideMetadataProviderException, HDBException, InterruptedException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        route.dumpTableSpace(tableSpace, fetchSize, receiver);
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
        if (tableSpace == null) {
            throw new HDBException("no such tablespace " + tableSpace);
        }
        String leaderId = client.getClientSideMetadataProvider().getTableSpaceLeader(tableSpace);
        if (leaderId == null) {
            throw new HDBException("no such tablespace " + tableSpace+" (no leader found)");
        }
        return getRouteToServer(leaderId);
    }

    public boolean isClosed() {
        return closed;
    }

    void requestMetadataRefresh() throws ClientSideMetadataProviderException {
        client.getClientSideMetadataProvider().requestMetadataRefresh();
    }

    public void restoreTableSpace(String tableSpace, TableSpaceRestoreSource source) throws ClientSideMetadataProviderException, HDBException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        route.restoreTableSpace(tableSpace, source);
    }

    @Override
    public String toString() {
        return "HDBConnection{" + "routes=" + routes.size() + ", id=" + id + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + (int) (this.id ^ (this.id >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final HDBConnection other = (HDBConnection) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
    }
    
    

}
