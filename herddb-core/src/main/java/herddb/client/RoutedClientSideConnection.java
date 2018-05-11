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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.backup.BackupFileConstants;
import herddb.backup.DumpedLogEntry;
import herddb.backup.DumpedTableMetadata;
import herddb.client.impl.RetryRequestException;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.KeyValue;
import herddb.network.Message;
import herddb.network.ServerHostData;
import herddb.security.sasl.SaslNettyClient;
import herddb.security.sasl.SaslUtils;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.TuplesList;

/**
 * A real connection to a server
 *
 * @author enrico.olivelli
 */
public class RoutedClientSideConnection implements AutoCloseable, ChannelEventListener {

    private Logger LOGGER = Logger.getLogger(RoutedClientSideConnection.class.getName());
    private final HDBConnection connection;
    private final String nodeId;
    private final long timeout;
    private final ServerHostData server;
    private final String clientId;
    private final ReentrantReadWriteLock connectionLock = new ReentrantReadWriteLock(true);
    private volatile Channel channel;

    private final Map<String, TableSpaceDumpReceiver> dumpReceivers = new ConcurrentHashMap<>();

    public RoutedClientSideConnection(HDBConnection connection, String nodeId) throws ClientSideMetadataProviderException {
        this.connection = connection;
        this.nodeId = nodeId;

        server = connection.getClient().getClientSideMetadataProvider().getServerHostData(nodeId);

        this.timeout = connection.getClient().getConfiguration().getLong(ClientConfiguration.PROPERTY_TIMEOUT, ClientConfiguration.PROPERTY_TIMEOUT_DEFAULT);
        this.clientId = connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENTID, ClientConfiguration.PROPERTY_CLIENTID_DEFAULT);
    }

    private void performAuthentication(Channel _channel, String serverHostname) throws Exception {

        SaslNettyClient saslNettyClient = new SaslNettyClient(
                connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT),
                connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT),
                serverHostname
        );

        byte[] firstToken = new byte[0];
        if (saslNettyClient.hasInitialResponse()) {
            firstToken = saslNettyClient.evaluateChallenge(new byte[0]);
        }
        Message saslResponse = _channel.sendMessageWithReply(Message.SASL_TOKEN_MESSAGE_REQUEST(SaslUtils.AUTH_DIGEST_MD5, firstToken), timeout);

        for (int i = 0; i < 100; i++) {
            byte[] responseToSendToServer;
            switch (saslResponse.type) {
                case Message.TYPE_SASL_TOKEN_SERVER_RESPONSE:
                    byte[] token = (byte[]) saslResponse.parameters.get("token");
                    responseToSendToServer = saslNettyClient.evaluateChallenge(token);
                    saslResponse = _channel.sendMessageWithReply(Message.SASL_TOKEN_MESSAGE_TOKEN(responseToSendToServer), timeout);
                    if (saslNettyClient.isComplete()) {
                        LOGGER.finest("SASL auth completed with success");
                        return;
                    }
                    break;
                case Message.TYPE_ERROR:
                    throw new Exception("Server returned ERROR during SASL negotiation, Maybe authentication failure (" + saslResponse.parameters + ")");
                default:
                    throw new Exception("Unexpected server response during SASL negotiation (" + saslResponse + ")");
            }
        }
        throw new Exception("SASL negotiation took too many steps");
    }

    @Override
    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT")
    public void messageReceived(Message message, Channel _channel) {
        switch (message.type) {
            case Message.TYPE_TABLESPACE_DUMP_DATA: {
                String dumpId = (String) message.parameters.get("dumpId");
                TableSpaceDumpReceiver receiver = dumpReceivers.get(dumpId);
                LOGGER.log(Level.FINE, "receiver for {0}: {1}", new Object[]{dumpId, receiver});
                if (receiver == null) {
                    if (_channel != null) {
                        _channel.sendReplyMessage(message, Message.ERROR(clientId, new Exception("no such dump receiver " + dumpId)));
                    }
                    return;
                }
                try {
                    Map<String, Object> values = (Map<String, Object>) message.parameters.get("values");
                    String command = (String) values.get("command") + "";
                    boolean sendAck = true;
                    switch (command) {
                        case "start": {
                            long ledgerId = (long) values.get("ledgerid");
                            long offset = (long) values.get("offset");
                            receiver.start(new LogSequenceNumber(ledgerId, offset));
                            break;
                        }
                        case "beginTable": {
                            byte[] tableDefinition = (byte[]) values.get("table");
                            Table table = Table.deserialize(tableDefinition);
                            Long estimatedSize = (Long) values.get("estimatedSize");
                            long dumpLedgerId = (Long) values.get("dumpLedgerid");
                            long dumpOffset = (Long) values.get("dumpOffset");
                            List<byte[]> indexesDef = (List<byte[]>) values.get("indexes");
                            List<Index> indexes = indexesDef
                                    .stream()
                                    .map(Index::deserialize)
                                    .collect(Collectors.toList());
                            Map<String, Object> stats = new HashMap<>();
                            stats.put("estimatedSize", estimatedSize);
                            stats.put("dumpLedgerId", dumpLedgerId);
                            stats.put("dumpOffset", dumpOffset);
                            receiver.beginTable(new DumpedTableMetadata(table,
                                    new LogSequenceNumber(dumpLedgerId, dumpOffset), indexes),
                                    stats);
                            break;
                        }
                        case "endTable": {
                            receiver.endTable();
                            break;
                        }
                        case "finish": {
                            long ledgerId = (long) values.get("ledgerid");
                            long offset = (long) values.get("offset");
                            receiver.finish(new LogSequenceNumber(ledgerId, offset));
                            sendAck = false;
                            break;
                        }
                        case "data": {
                            List<KeyValue> data = (List<KeyValue>) values.get("records");
                            List<Record> records = new ArrayList<>(data.size());
                            for (KeyValue kv : data) {
                                records.add(new Record(new Bytes(kv.key), new Bytes(kv.value)));
                            }
                            receiver.receiveTableDataChunk(records);
                            break;
                        }
                        case "txlog": {
                            List<KeyValue> data = (List<KeyValue>) values.get("records");
                            List<DumpedLogEntry> records = new ArrayList<>(data.size());
                            for (KeyValue kv : data) {
                                records.add(new DumpedLogEntry(LogSequenceNumber.deserialize(kv.key), kv.value));
                            }
                            receiver.receiveTransactionLogChunk(records);
                            break;
                        }
                        case "transactions": {
                            String tableSpace = (String) values.get("tableSpace");
                            List<byte[]> data = (List<byte[]>) values.get("transactions");
                            List<Transaction> transactions = data.stream().map(array -> {
                                return Transaction.deserialize(tableSpace, array);
                            }).collect(Collectors.toList());
                            receiver.receiveTransactionsAtDump(transactions);
                            break;
                        }
                        default:
                            throw new DataStorageManagerException("invalid dump command:" + command);
                    }
                    if (_channel != null && sendAck) {
                        _channel.sendReplyMessage(message, Message.ACK(clientId));
                    }
                } catch (DataStorageManagerException error) {
                    LOGGER.log(Level.SEVERE, "error while handling dump data", error);
                    if (_channel != null) {
                        _channel.sendReplyMessage(message, Message.ERROR(clientId, error));
                    }
                }
            }
            break;

        }
    }

    @Override
    public void channelClosed(Channel channel) {
        if (channel == this.channel) {
            this.channel = null;
        }
    }

    @Override
    public void close() {
        LOGGER.log(Level.SEVERE, "{0} - close", this);

        this.connection.releaseRoute(nodeId);
        connectionLock.writeLock().lock();
        try {
            if (channel != null) {
                channel.close();
            }
        } finally {
            channel = null;
            connectionLock.writeLock().unlock();;
        }
    }

    private Channel ensureOpen() throws HDBException {
        connectionLock.readLock().lock();
        try {
            if (channel != null) {
                return channel;
            }
            connectionLock.readLock().unlock();

            connectionLock.writeLock().lock();
            try {
                if (channel != null) {
                    return channel;
                }
                LOGGER.log(Level.FINE, "{0} - connect to {1}:{2} ssh:{3}", new Object[]{this, server.getHost(), server.getPort(), server.isSsl()});
                Channel _channel = this.connection.getClient().createChannelTo(server, this);
                try {
                    performAuthentication(_channel, server.getHost());
                    channel = _channel;
                    return channel;
                } catch (Exception err) {
                    if (_channel != null) {
                        _channel.close();
                    }
                    throw err;
                }
            } finally {
                connectionLock.writeLock().unlock();
                connectionLock.readLock().lock();
            }
        } catch (Exception err) {
            throw new HDBException(err);
        } finally {
            connectionLock.readLock().unlock();
        }
    }

    DMLResult executeUpdate(String tableSpace, String query, long tx, boolean returnValues, List<Object> params) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, tableSpace, query, tx, returnValues, params);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply);
            }
            long updateCount = (Long) reply.parameters.get("updateCount");
            long transactionId = (Long) reply.parameters.get("tx");

            Object key = null;
            Map<String, Object> newvalue = null;
            Map<String, Object> data = (Map<String, Object>) reply.parameters.get("data");

            if (data != null) {
                key = data.get("key");
                newvalue = (Map<String, Object>) data.get("newvalue");
            }
            return new DMLResult(updateCount, key, newvalue, transactionId);
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    List<DMLResult> executeUpdates(String tableSpace, String query, long tx, boolean returnValues, List<List<Object>> batch) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            Message message = Message.EXECUTE_STATEMENTS(clientId, tableSpace, query, tx, returnValues, batch);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply);
            }

            long transactionId = (Long) reply.parameters.get("tx");

            List<Map<String, Object>> data = (List<Map<String, Object>>) reply.parameters.get("data");
            List<Long> updateCounts = (List<Long>) reply.parameters.get("updateCount");
            List<DMLResult> results = new ArrayList<>();

            for (int i = 0; i < updateCounts.size(); i++) {
                Object key = data.get(0).get("key");
                Map<String, Object> newvalue = (Map<String, Object>) data.get(i).get("newvalue");
                DMLResult res = new DMLResult(updateCounts.get(i), key, newvalue, transactionId);
                results.add(res);
            }

            return results;
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    GetResult executeGet(String tableSpace, String query, long tx, List<Object> params) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            Message message = Message.EXECUTE_STATEMENT(clientId, tableSpace, query, tx, false, params);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply);
            }
            long found = (Long) reply.parameters.get("updateCount");
            long transactionId = (Long) reply.parameters.get("tx");
            if (found <= 0) {
                return new GetResult(null, transactionId);
            } else {
                return new GetResult((Map<String, Object>) reply.parameters.get("data"), transactionId);
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    long beginTransaction(String tableSpace) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            Message message = Message.TX_COMMAND(clientId, tableSpace, Message.TX_COMMAND_BEGIN_TRANSACTION, 0);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply);
            }
            Map<String, Object> data = (Map<String, Object>) reply.parameters.get("data");
            return (Long) data.get("tx");
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void commitTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            Message message = Message.TX_COMMAND(clientId, tableSpace, Message.TX_COMMAND_COMMIT_TRANSACTION, tx);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply);
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void rollbackTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            Message message = Message.TX_COMMAND(clientId, tableSpace, Message.TX_COMMAND_ROLLBACK_TRANSACTION, tx);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply);
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    private static final AtomicLong SCANNERID_GENERATOR = new AtomicLong();

    ScanResultSet executeScan(String tableSpace, String query, List<Object> params, long tx, int maxRows, int fetchSize) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            String scannerId = this.clientId + ":" + SCANNERID_GENERATOR.incrementAndGet();
            Message message = Message.OPEN_SCANNER(clientId, tableSpace, query, scannerId, tx, params, fetchSize, maxRows);
            LOGGER.log(Level.FINEST, "open scanner {0} for query {1}, params {2}", new Object[]{scannerId, query, params});
            Message reply = _channel.sendMessageWithReply(message, timeout);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply);
            }
            TuplesList data = (TuplesList) reply.parameters.get("data");
            List<DataAccessor> initialFetchBuffer = data.tuples;
            String[] columnNames = data.columnNames;
            boolean last = (Boolean) reply.parameters.get("last");
            long transactionId = (Long) reply.parameters.get("tx");
            //LOGGER.log(Level.SEVERE, "received first " + initialFetchBuffer.size() + " records for query " + query);
            ScanResultSetImpl impl = new ScanResultSetImpl(scannerId, columnNames, initialFetchBuffer, fetchSize, last, transactionId);

            return impl;
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void dumpTableSpace(String tableSpace, int fetchSize, boolean includeTransactionLog, TableSpaceDumpReceiver receiver) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            String dumpId = this.clientId + ":" + SCANNERID_GENERATOR.incrementAndGet();
            Message message = Message.REQUEST_TABLESPACE_DUMP(clientId, tableSpace, dumpId, fetchSize, includeTransactionLog);
            LOGGER.log(Level.SEVERE, "dumpTableSpace id " + dumpId + " for tablespace " + tableSpace);
            dumpReceivers.put(dumpId, receiver);
            Message reply = _channel.sendMessageWithReply(message, timeout);
            LOGGER.log(Level.SEVERE, "dumpTableSpace id " + dumpId + " for tablespace " + tableSpace + ": first reply " + reply.parameters);
            if (reply.type == Message.TYPE_ERROR) {
                boolean notLeader = reply.parameters.get("notLeader") != null;
                if (notLeader) {
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                throw new HDBException(reply);
            }

        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void restoreTableSpace(String tableSpace, TableSpaceRestoreSource source) throws HDBException, ClientSideMetadataProviderException {
        List<DumpedTableMetadata> tables = new ArrayList<>();
        try {
            while (true) {
                String entryType = source.nextEntryType();
                LOGGER.log(Level.SEVERE, "restore, entryType:{0}", entryType);
                switch (entryType) {

                    case BackupFileConstants.ENTRY_TYPE_START: {
                        break;
                    }

                    case BackupFileConstants.ENTRY_TYPE_TABLE: {
                        DumpedTableMetadata table = source.nextTable();
                        Channel _channel = ensureOpen();
                        Message message_create_table = Message.REQUEST_TABLE_RESTORE(clientId, tableSpace,
                                table.table.serialize(), table.logSequenceNumber.ledgerId, table.logSequenceNumber.offset);
                        Message reply_create_table = _channel.sendMessageWithReply(message_create_table, timeout);
                        if (reply_create_table.type == Message.TYPE_ERROR) {
                            throw new HDBException(reply_create_table);
                        }
                        List<KeyValue> chunk = source.nextTableDataChunk();
                        while (chunk != null) {
                            Message message = Message.PUSH_TABLE_DATA(clientId, tableSpace, table.table.name, chunk);
                            Message reply = _channel.sendMessageWithReply(message, timeout);
                            if (reply.type == Message.TYPE_ERROR) {
                                throw new HDBException(reply);
                            }
                            chunk = source.nextTableDataChunk();
                        }

                        tables.add(table);
                        break;
                    }
                    case BackupFileConstants.ENTRY_TYPE_TXLOGCHUNK: {
                        Channel _channel = ensureOpen();
                        List<KeyValue> chunk = source.nextTransactionLogChunk();
                        Message message = Message.PUSH_TXLOGCHUNK(clientId, tableSpace, chunk);
                        Message reply = _channel.sendMessageWithReply(message, timeout);
                        if (reply.type == Message.TYPE_ERROR) {
                            throw new HDBException(reply);
                        }
                        break;
                    }
                    case BackupFileConstants.ENTRY_TYPE_TRANSACTIONS: {
                        Channel _channel = ensureOpen();
                        List<byte[]> chunk = source.nextTransactionsBlock();
                        Message message = Message.PUSH_TRANSACTIONSBLOCK(clientId, tableSpace, chunk);
                        Message reply = _channel.sendMessageWithReply(message, timeout);
                        if (reply.type == Message.TYPE_ERROR) {
                            throw new HDBException(reply);
                        }
                        break;
                    }
                    case BackupFileConstants.ENTRY_TYPE_END: {
                        // send a 'table finished' event only at the end of the procedure
                        // the stream of transaction log entries is finished, so the data contained in the table is "final"
                        // we are going to create now all the indexes too
                        Channel _channel = ensureOpen();
                        for (DumpedTableMetadata table : tables) {
                            List<byte[]> indexes = table.indexes.stream().map(Index::serialize).collect(Collectors.toList());

                            Message message_table_finished = Message.TABLE_RESTORE_FINISHED(clientId, tableSpace,
                                    table.table.name, indexes);
                            Message reply_table_finished = _channel.sendMessageWithReply(message_table_finished, timeout);
                            if (reply_table_finished.type == Message.TYPE_ERROR) {
                                throw new HDBException(reply_table_finished);
                            }
                        }

                        Message message_restore_finished = Message.RESTORE_FINISHED(clientId, tableSpace);
                        Message reply_restore_finished = _channel.sendMessageWithReply(message_restore_finished, timeout);
                        if (reply_restore_finished.type == Message.TYPE_ERROR) {
                            throw new HDBException(reply_restore_finished);
                        }

                        return;
                    }
                    default:
                        throw new HDBException("bad entryType " + entryType);
                }

            }
        } catch (InterruptedException | TimeoutException | DataStorageManagerException err) {
            throw new HDBException(err);
        }
    }

    private class ScanResultSetImpl extends ScanResultSet {

        private final String scannerId;
        private final ScanResultSetMetadata metadata;

        private ScanResultSetImpl(String scannerId, String[] columns, List<DataAccessor> fetchBuffer, int fetchSize, boolean onlyOneChunk, long tx) {
            super(tx);
            this.scannerId = scannerId;
            this.metadata = new ScanResultSetMetadata(columns);

            this.fetchBuffer.addAll(fetchBuffer);
            this.fetchSize = fetchSize;
            if (fetchBuffer.isEmpty()) {
                // empty result set
                finished = true;
                noMoreData = true;
            }
            if (onlyOneChunk) {
                lastChunk = true;
            }
        }

        @Override
        public ScanResultSetMetadata getMetadata() {
            return metadata;
        }

        final List<DataAccessor> fetchBuffer = new ArrayList<>();
        Map<String, Object> next;
        boolean finished;
        boolean noMoreData;
        int bufferPosition;
        int fetchSize;
        boolean lastChunk;

        @Override
        public void close() {
            finished = true;
        }

        @Override
        public boolean hasNext() throws HDBException {
            if (finished) {
                return false;
            }
            return ensureNext();
        }

        private void fillBuffer() throws HDBException {
            if (lastChunk) {
                fetchBuffer.clear();
                noMoreData = true;
                bufferPosition = 0;
                return;
            }
            fetchBuffer.clear();
            Channel _channel = ensureOpen();
            try {
                Message result = _channel.sendMessageWithReply(Message.FETCH_SCANNER_DATA(clientId, scannerId, fetchSize), 10000);
                //LOGGER.log(Level.SEVERE, "fillBuffer result " + result);
                if (result.type == Message.TYPE_ERROR) {
                    throw new HDBException(result);
                }
                if (result.type != Message.TYPE_RESULTSET_CHUNK) {
                    finished = true;
                    throw new HDBException("protocol error: " + result);
                }
                TuplesList data = (TuplesList) result.parameters.get("data");
                List<DataAccessor> records = data.tuples;
                lastChunk = (Boolean) result.parameters.get("last");
                if (records.isEmpty()) {
                    noMoreData = true;
                }
                fetchBuffer.addAll(records);
                bufferPosition = 0;
            } catch (InterruptedException | TimeoutException err) {
                throw new HDBException(err);
            }
        }

        private boolean ensureNext() throws HDBException {
            if (next != null) {
                return true;
            }
            if (bufferPosition == fetchBuffer.size()) {
                fillBuffer();
                if (noMoreData) {
                    finished = true;
                    return false;
                }
            }
            next = fetchBuffer.get(bufferPosition++).toMap();
            return true;
        }

        @Override
        public Map<String, Object> next() throws HDBException {
            if (finished) {
                throw new HDBException("Scanner is exhausted");
            }
            Map<String, Object> _next = next;
            next = null;
            return _next;
        }

    }

}
