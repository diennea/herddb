/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.client;

import herddb.network.Channel;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Connection from a client to a server.
 */
public interface ClientSideConnectionPeer {

    void close();

    Channel getChannel();

    Channel ensureOpen() throws HDBException;

    String getClientId();

    String getNodeId();

    DMLResult executeUpdate(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<Object> params) throws HDBException, ClientSideMetadataProviderException;

    GetResult executeGet(String tableSpace, String query, long tx, boolean usePreparedStatement, List<Object> params) throws HDBException, ClientSideMetadataProviderException;

    long beginTransaction(String tableSpace) throws HDBException, ClientSideMetadataProviderException;

    void commitTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException;

    void rollbackTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException;

    ScanResultSet executeScan(String tableSpace, String query, boolean usePreparedStatement, List<Object> params, long tx, int maxRows, int fetchSize,
                              boolean keepReadLocks) throws HDBException, ClientSideMetadataProviderException;

    CompletableFuture<DMLResult> executeUpdateAsync(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<Object> params);

    CompletableFuture<List<DMLResult>> executeUpdatesAsync(
            String tableSpace, String query, long tx, boolean returnValues,
            boolean usePreparedStatement, List<List<Object>> batch
    );

    List<DMLResult> executeUpdates(
            String tableSpace, String query, long tx, boolean returnValues,
            boolean usePreparedStatement, List<List<Object>> batch
    ) throws HDBException, ClientSideMetadataProviderException;

    void dumpTableSpace(String tableSpace, int fetchSize, boolean includeTransactionLog, TableSpaceDumpReceiver receiver) throws HDBException, ClientSideMetadataProviderException;

    void restoreTableSpace(String tableSpace, TableSpaceRestoreSource source) throws HDBException, ClientSideMetadataProviderException;
}
