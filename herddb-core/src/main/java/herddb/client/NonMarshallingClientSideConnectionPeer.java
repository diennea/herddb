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
import herddb.network.netty.LocalVMChannel;
import herddb.server.ServerSideConnectionPeer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This is a wrapper around RoutedClientSideConnection that skips serialization to netty ByteBufs
 * in order to save CPU cycles in 'local' mode.
 */
public class NonMarshallingClientSideConnectionPeer implements ClientSideConnectionPeer {

    private final RoutedClientSideConnection realConnection;

    public NonMarshallingClientSideConnectionPeer(RoutedClientSideConnection fallback) {
        this.realConnection = fallback;
    }

    @Override
    public String getNodeId() {
        return realConnection.getNodeId();
    }

    @Override
    public String getClientId() {
        return realConnection.getClientId();
    }

    @Override
    public void close() {
        realConnection.close();
    }

    @Override
    public Channel getChannel() {
        return realConnection.getChannel();
    }

    @Override
    public Channel ensureOpen() throws HDBException {
        return realConnection.ensureOpen();
    }

    @Override
    public DMLResult executeUpdate(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<Object> params) throws HDBException, ClientSideMetadataProviderException {
        LocalVMChannel channel = (LocalVMChannel) realConnection.ensureOpen();
        ServerSideConnectionPeer serverSidePeer = (ServerSideConnectionPeer) channel.getServerSideChannel().getMessagesReceiver();
        return serverSidePeer.executeUpdate(tableSpace, query, tx, returnValues, params);
    }

    @Override
    public CompletableFuture<DMLResult> executeUpdateAsync(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<Object> params) {
        CompletableFuture<DMLResult> res = new CompletableFuture<>();
        try {
            res.complete(executeUpdate(tableSpace, query, tx, returnValues, usePreparedStatement, params));
        } catch (ClientSideMetadataProviderException | HDBException t) {
            res.completeExceptionally(t);
        }
        return res;
    }

    @Override
    public List<DMLResult> executeUpdates(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<List<Object>> batch) throws HDBException, ClientSideMetadataProviderException {
        LocalVMChannel channel = (LocalVMChannel) realConnection.ensureOpen();
        ServerSideConnectionPeer serverSidePeer = (ServerSideConnectionPeer) channel.getServerSideChannel().getMessagesReceiver();
        return serverSidePeer.executeUpdates(tableSpace, query, tx, returnValues, batch);
    }

    @Override
    public CompletableFuture<List<DMLResult>> executeUpdatesAsync(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<List<Object>> batch) {
         CompletableFuture<List<DMLResult>> res = new CompletableFuture<>();
        try {
            res.complete(executeUpdates(tableSpace, query, tx, returnValues, usePreparedStatement, batch));
        } catch (ClientSideMetadataProviderException | HDBException t) {
            res.completeExceptionally(t);
        }
        return res;
    }

    @Override
    public GetResult executeGet(String tableSpace, String query, long tx, boolean usePreparedStatement, List<Object> params) throws HDBException, ClientSideMetadataProviderException {
         LocalVMChannel channel = (LocalVMChannel) realConnection.ensureOpen();
        ServerSideConnectionPeer serverSidePeer = (ServerSideConnectionPeer) channel.getServerSideChannel().getMessagesReceiver();
        return serverSidePeer.executeGet(tableSpace, query, tx, params);
    }

    @Override
    public long beginTransaction(String tableSpace) throws HDBException, ClientSideMetadataProviderException {
        LocalVMChannel channel = (LocalVMChannel) realConnection.ensureOpen();
        ServerSideConnectionPeer serverSidePeer = (ServerSideConnectionPeer) channel.getServerSideChannel().getMessagesReceiver();
        return serverSidePeer.beginTransaction(tableSpace);
    }

    @Override
    public void commitTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        LocalVMChannel channel = (LocalVMChannel) realConnection.ensureOpen();
        ServerSideConnectionPeer serverSidePeer = (ServerSideConnectionPeer) channel.getServerSideChannel().getMessagesReceiver();
        serverSidePeer.commitTransaction(tableSpace, tx);
    }

    @Override
    public void rollbackTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        LocalVMChannel channel = (LocalVMChannel) realConnection.ensureOpen();
        ServerSideConnectionPeer serverSidePeer = (ServerSideConnectionPeer) channel.getServerSideChannel().getMessagesReceiver();
        serverSidePeer.rollbackTransaction(tableSpace, tx);
    }

    @Override
    public ScanResultSet executeScan(String tableSpace, String query, boolean usePreparedStatement, List<Object> params, long tx, int maxRows, int fetchSize, boolean keepReadLocks) throws HDBException, ClientSideMetadataProviderException {
        LocalVMChannel channel = (LocalVMChannel) realConnection.ensureOpen();
        ServerSideConnectionPeer serverSidePeer = (ServerSideConnectionPeer) channel.getServerSideChannel().getMessagesReceiver();
        return serverSidePeer.executeScan(tableSpace, query, usePreparedStatement, params, tx, maxRows, fetchSize, keepReadLocks);
    }

    @Override
    public void dumpTableSpace(String tableSpace, int fetchSize, boolean includeTransactionLog, TableSpaceDumpReceiver receiver) throws HDBException, ClientSideMetadataProviderException {
        // no need to implement this stuff in LocalMode, there is no gain and it would be very complex
        realConnection.dumpTableSpace(tableSpace, fetchSize, includeTransactionLog, receiver);
    }

    @Override
    public void restoreTableSpace(String tableSpace, TableSpaceRestoreSource source) throws HDBException, ClientSideMetadataProviderException {
        // no need to implement this stuff in LocalMode, there is no gain and it would be very complex
        realConnection.restoreTableSpace(tableSpace, source);
    }

}
