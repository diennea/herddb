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

import static herddb.utils.QueryUtils.discoverTablespace;
import herddb.client.impl.LeaderChangedException;
import herddb.client.impl.RetryRequestException;
import herddb.model.TransactionContext;
import herddb.network.ServerHostData;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stats.Counter;

/**
 * Connection on the client side
 *
 * @author enrico.olivelli
 */
public class HDBConnection implements AutoCloseable {

    private static final AtomicLong IDGENERATOR = new AtomicLong();
    private final long id = IDGENERATOR.incrementAndGet();
    private final HDBClient client;
    private volatile boolean closed;
    private boolean discoverTablespaceFromSql = true;
    private Counter leaderChangedErrors;
    private final int maxConnectionsPerServer;
    private final Random random = new Random();
    private Map<String, RoutedClientSideConnection[]> routes;

    public HDBConnection(HDBClient client) {
        if (client == null) {
            throw new NullPointerException();
        }
        this.client = client;
        this.leaderChangedErrors = client
                .getStatsLogger()
                .getCounter("leaderChangedErrors");

        this.maxConnectionsPerServer =
                client.getConfiguration().getInt(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER_DEFAULT);

        this.routes = new ConcurrentHashMap<>();

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
        LOGGER.log(Level.FINER, "{0} close ", this);
        closed = true;
        routes.forEach((n, b) -> {
            for (RoutedClientSideConnection cc : b) {
                cc.close();
            }
        });
        routes.clear();
        client.releaseConnection(this);
    }

    private static final Logger LOGGER = Logger.getLogger(HDBConnection.class.getName());

    public boolean waitForTableSpace(String tableSpace, int timeout) throws HDBException, ClientSideMetadataProviderException {
        long start = System.currentTimeMillis();
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                try (ScanResultSet result = route.executeScan(tableSpace,
                        "select * "
                                + "from systablespaces "
                                + "where tablespace_name=?", false,
                        Arrays.asList(tableSpace), TransactionContext.NOTRANSACTION_ID,
                        1,
                        1)) {
                    boolean ok = result.hasNext();
                    if (ok) {
                        LOGGER.log(Level.INFO, "table space {0} is up now: info {1}", new Object[]{tableSpace,
                                result
                                        .consume()
                                        .get(0)});
                        return true;
                    }
                }
            } catch (ClientSideMetadataProviderException | HDBException retry) {
                long now = System.currentTimeMillis();
                if (now - start > timeout) {
                    return false;
                }
                LOGGER.log(Level.FINE, "tableSpace is still not up " + tableSpace, retry);
                handleRetryError(retry, 0 /* always zero */);
            }

            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                return false;
            }
        }
        return false;
    }

    public long beginTransaction(String tableSpace) throws ClientSideMetadataProviderException, HDBException {
        int trialCount = 0;
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.beginTransaction(tableSpace);
            } catch (RetryRequestException retry) {
                handleRetryError(retry, trialCount++);
            }
        }
        throw new HDBException("client is closed");
    }

    public void rollbackTransaction(String tableSpace, long tx) throws ClientSideMetadataProviderException, HDBException {
        int trialCount = 0;
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                route.rollbackTransaction(tableSpace, tx);
                return;
            } catch (RetryRequestException retry) {
                handleRetryError(retry, trialCount++);
            }
        }
        throw new HDBException("client is closed");
    }

    public void commitTransaction(String tableSpace, long tx) throws ClientSideMetadataProviderException, HDBException {
        int trialCount = 0;
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                route.commitTransaction(tableSpace, tx);
                return;
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                handleRetryError(retry, trialCount++);
            }
        }
        throw new HDBException("client is closed");
    }

    public DMLResult executeUpdate(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<Object> params) throws ClientSideMetadataProviderException, HDBException {
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        int trialCount = 0;
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.executeUpdate(tableSpace, query, tx, returnValues, usePreparedStatement, params);
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                handleRetryError(retry, trialCount++);
            }
        }
        throw new HDBException("client is closed");
    }

    public CompletableFuture<DMLResult> executeUpdateAsync(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<Object> params) {
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        if (closed) {
            return FutureUtils.exception(new HDBException("client is closed"));
        }
        CompletableFuture<DMLResult> res = new CompletableFuture<>();

        AtomicInteger count = new AtomicInteger(0);
        executeStatementAsyncInternal(tableSpace, res, query, tx, returnValues, usePreparedStatement, params, count);
        return res;
    }

    private void executeStatementAsyncInternal(String tableSpace, CompletableFuture<DMLResult> res, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<Object> params, AtomicInteger count) {
        RoutedClientSideConnection route;
        try {
            route = getRouteToTableSpace(tableSpace);
        } catch (ClientSideMetadataProviderException | HDBException err) {
            res.completeExceptionally(err);
            return;
        }
        route.executeUpdateAsync(tableSpace, query, tx, returnValues, usePreparedStatement, params)
                .whenComplete((dmlresult, error) -> {
                    if (error != null) {
                        if (error instanceof RetryRequestException
                                && !closed) {
                            try {
                                handleRetryError(error, count.getAndIncrement());
                            } catch (ClientSideMetadataProviderException | HDBException err) {
                                res.completeExceptionally(err);
                                return;
                            }
                            LOGGER.log(Level.INFO, "retry #{0} {1}: {2}", new Object[]{count, query, error});
                            executeStatementAsyncInternal(tableSpace, res, query, tx, returnValues, usePreparedStatement, params, count);
                        } else {
                            res.completeExceptionally(error);
                        }
                    } else {
                        res.complete(dmlresult);
                    }
                });
    }

    private void executeStatementsAsyncInternal(String tableSpace, CompletableFuture<List<DMLResult>> res, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<List<Object>> params, AtomicInteger count) {
        RoutedClientSideConnection route;
        try {
            route = getRouteToTableSpace(tableSpace);
        } catch (ClientSideMetadataProviderException | HDBException err) {
            res.completeExceptionally(err);
            return;
        }
        route.executeUpdatesAsync(tableSpace, query, tx, returnValues, usePreparedStatement, params)
                .whenComplete((dmlresult, error) -> {
                    if (error != null) {
                        if (error instanceof RetryRequestException
                                && !closed) {
                            try {
                                handleRetryError(error, count.getAndIncrement());
                            } catch (ClientSideMetadataProviderException | HDBException err) {
                                res.completeExceptionally(err);
                                return;
                            }
                            LOGGER.log(Level.INFO, "retry #{0} {1}: {2}", new Object[]{count, query, error});
                            executeStatementsAsyncInternal(tableSpace, res, query, tx, returnValues, usePreparedStatement, params, count);
                        } else {
                            res.completeExceptionally(error);
                        }
                    } else {
                        res.complete(dmlresult);
                    }
                });
    }

    public List<DMLResult> executeUpdates(
            String tableSpace, String query, long tx, boolean returnValues,
            boolean usePreparedStatement, List<List<Object>> batch
    ) throws ClientSideMetadataProviderException, HDBException {
        if (batch.isEmpty()) {
            return Collections.emptyList();
        }
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        int trialCount = 0;
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.executeUpdates(tableSpace, query, tx, returnValues, usePreparedStatement, batch);
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                handleRetryError(retry, trialCount++);
            }
        }
        throw new HDBException("client is closed");
    }

    public CompletableFuture<List<DMLResult>> executeUpdatesAsync(
            String tableSpace, String query, long tx, boolean returnValues,
            boolean usePreparedStatement, List<List<Object>> batch
    ) {
        if (batch.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        if (closed) {
            return FutureUtils.exception(new HDBException("client is closed"));
        }
        CompletableFuture<List<DMLResult>> res = new CompletableFuture<>();

        AtomicInteger count = new AtomicInteger(0);
        executeStatementsAsyncInternal(tableSpace, res, query, tx, returnValues, usePreparedStatement, batch, count);
        return res;
    }

    public GetResult executeGet(String tableSpace, String query, long tx, boolean usePreparedStatement, List<Object> params) throws ClientSideMetadataProviderException, HDBException {
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        int trialCount = 0;
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.executeGet(tableSpace, query, tx, usePreparedStatement, params);
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.SEVERE, "error " + retry, retry);
                handleRetryError(retry, trialCount++);
            }
        }
        throw new HDBException("client is closed");
    }

    public ScanResultSet executeScan(String tableSpace, String query, boolean usePreparedStatement, List<Object> params, long tx, int maxRows, int fetchSize) throws ClientSideMetadataProviderException, HDBException, InterruptedException {
        if (discoverTablespaceFromSql) {
            tableSpace = discoverTablespace(tableSpace, query);
        }
        int trialCount = 0;
        while (!closed) {
            try {
                RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
                return route.executeScan(tableSpace, query, usePreparedStatement, params, tx, maxRows, fetchSize);
            } catch (RetryRequestException retry) {
                LOGGER.log(Level.INFO, "temporary error", retry);
                handleRetryError(retry, trialCount++);
            }

        }
        throw new HDBException("client is closed");
    }

    private void handleRetryError(Throwable retry, int trialCount) throws HDBException, ClientSideMetadataProviderException {
        LOGGER.log(Level.INFO, "retry #{0}:" + retry, trialCount); // no stracktrace
        int sleepTimeout = client.getOperationRetryDelay();
        int maxTrials = client.getMaxOperationRetryCount();
        if (retry instanceof RetryRequestException) {
            if (retry instanceof LeaderChangedException) {
                leaderChangedErrors.inc();
                maxTrials = Integer.MAX_VALUE;
            }
            if (trialCount > maxTrials) {
                throw new HDBException("Too many trials (" + trialCount + "/" + maxTrials + ") for " + retry, retry);
            }
            RetryRequestException retryError = (RetryRequestException) retry;
            if (retryError.isRequireMetadataRefresh()) {
                requestMetadataRefresh(retryError);
            }
        }
        try {
            // linear back-off
            Thread.sleep((trialCount + 1) * sleepTimeout);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        }
    }

    public void dumpTableSpace(
            String tableSpace, TableSpaceDumpReceiver receiver, int fetchSize,
            boolean includeTransactionLog
    ) throws ClientSideMetadataProviderException, HDBException, InterruptedException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        route.dumpTableSpace(tableSpace, fetchSize, includeTransactionLog, receiver);
    }

    protected RoutedClientSideConnection chooseConnection(RoutedClientSideConnection[] all) {
        return all[random.nextInt(maxConnectionsPerServer)];
    }

    private RoutedClientSideConnection getRouteToServer(String nodeId) throws ClientSideMetadataProviderException, HDBException {
        try {
            RoutedClientSideConnection[] all = routes.computeIfAbsent(nodeId, n -> {
                try {
                    ServerHostData serverHostData = client.getClientSideMetadataProvider().getServerHostData(nodeId);

                    RoutedClientSideConnection[] res = new RoutedClientSideConnection[maxConnectionsPerServer];
                    for (int i = 0; i < maxConnectionsPerServer; i++) {
                        res[i] = new RoutedClientSideConnection(this, nodeId, serverHostData);
                    }
                    return res;
                } catch (ClientSideMetadataProviderException err) {
                    throw new RuntimeException(err);
                }
            });
            return chooseConnection(all);
        } catch (RuntimeException err) {
            if (err.getCause() instanceof ClientSideMetadataProviderException) {
                throw (ClientSideMetadataProviderException) (err.getCause());
            } else {
                throw new HDBException(err);
            }
        }
    }

    public RoutedClientSideConnection getRouteToTableSpace(String tableSpace) throws ClientSideMetadataProviderException, HDBException {
        if (closed) {
            throw new HDBException("connection is closed");
        }
        if (tableSpace == null) {
            throw new HDBException("null tablespace");
        }
        String leaderId = client.getClientSideMetadataProvider().getTableSpaceLeader(tableSpace);
        if (leaderId == null) {
            throw new HDBException("no such tablespace " + tableSpace + " (no leader found)");
        }
        return getRouteToServer(leaderId);
    }

    public boolean isClosed() {
        return closed;
    }

    void requestMetadataRefresh(Exception err) throws ClientSideMetadataProviderException {
        client.getClientSideMetadataProvider().requestMetadataRefresh(err);
    }

    public void restoreTableSpace(String tableSpace, TableSpaceRestoreSource source) throws ClientSideMetadataProviderException, HDBException {
        RoutedClientSideConnection route = getRouteToTableSpace(tableSpace);
        route.restoreTableSpace(tableSpace, source);
    }

    @Override
    public String toString() {
        return "HDBConnection{" + "routes=" + routes + ", id=" + id + '}';
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
        return this.id == other.id;
    }

}
