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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.backup.BackupFileConstants;
import herddb.backup.DumpedLogEntry;
import herddb.backup.DumpedTableMetadata;
import herddb.client.impl.HDBOperationTimeoutException;
import herddb.client.impl.LeaderChangedException;
import herddb.client.impl.RetryRequestException;
import herddb.client.impl.UnreachableServerException;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ServerHostData;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.proto.PduCodec.ErrorResponse;
import herddb.security.sasl.SaslNettyClient;
import herddb.security.sasl.SaslUtils;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.KeyValue;
import herddb.utils.RawString;
import herddb.utils.RecordsBatch;
import io.netty.buffer.ByteBuf;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A real connection to a server
 *
 * @author enrico.olivelli
 */
public class RoutedClientSideConnection implements ChannelEventListener {

    private static final Logger LOGGER = Logger.getLogger(RoutedClientSideConnection.class.getName());
    private static final RawString RAWSTRING_KEY = RawString.of("_key");

    private final HDBConnection connection;
    private final String nodeId;
    private final long timeout;
    private final ServerHostData server;
    private final String clientId;
    private final ReentrantReadWriteLock connectionLock = new ReentrantReadWriteLock(true);
    private volatile Channel channel;
    private final AtomicLong scannerIdGenerator = new AtomicLong();
    private final ClientSideQueryCache preparedStatements = new ClientSideQueryCache();

    private final Map<String, TableSpaceDumpReceiver> dumpReceivers = new ConcurrentHashMap<>();

    public RoutedClientSideConnection(HDBConnection connection, String nodeId, ServerHostData server) {
        this.connection = connection;
        this.nodeId = nodeId;
        this.server = server;

        this.timeout = connection.getClient().getConfiguration().getLong(ClientConfiguration.PROPERTY_TIMEOUT, ClientConfiguration.PROPERTY_TIMEOUT_DEFAULT);
        this.clientId = connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENTID, ClientConfiguration.PROPERTY_CLIENTID_DEFAULT);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getClientId() {
        return clientId;
    }

    private void performAuthentication(Channel channel, String serverHostname) throws Exception {

        SaslNettyClient saslNettyClient = new SaslNettyClient(
                connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT),
                connection.getClient().getConfiguration().getString(ClientConfiguration.PROPERTY_CLIENT_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT),
                serverHostname
        );

        byte[] firstToken = new byte[0];
        if (saslNettyClient.hasInitialResponse()) {
            firstToken = saslNettyClient.evaluateChallenge(new byte[0]);
        }

        long requestId = channel.generateRequestId();
        Pdu saslResponse = channel.sendMessageWithPduReply(requestId,
                PduCodec.SaslTokenMessageRequest.write(requestId, SaslUtils.AUTH_DIGEST_MD5, firstToken), timeout);
        try {
            for (int i = 0; i < 100; i++) {
                byte[] responseToSendToServer;
                switch (saslResponse.type) {
                    case Pdu.TYPE_SASL_TOKEN_SERVER_RESPONSE:
                        byte[] token = PduCodec.SaslTokenServerResponse.readToken(saslResponse);
                        responseToSendToServer = saslNettyClient.evaluateChallenge(token);
                        requestId = channel.generateRequestId();
                        saslResponse.close();
                        saslResponse = channel.sendMessageWithPduReply(requestId,
                                PduCodec.SaslTokenMessageToken.write(requestId, responseToSendToServer), timeout);
                        if (saslNettyClient.isComplete()) {
                            LOGGER.finest("SASL auth completed with success");
                            return;
                        }
                        break;
                    case Pdu.TYPE_ERROR:
                        throw new Exception("Server returned ERROR during SASL negotiation, Maybe authentication failure (" + PduCodec.ErrorResponse.readError(saslResponse) + ")");
                    default:
                        throw new Exception("Unexpected server response during SASL negotiation (" + saslResponse + ")");
                }
            }
        } finally {
            saslResponse.close();
        }
        throw new Exception("SASL negotiation took too many steps");
    }

    @Override
    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT")
    @SuppressWarnings("empty-statement")
    public void requestReceived(Pdu message, Channel channel) {
        try {
            switch (message.type) {
                case Pdu.TYPE_TABLESPACE_DUMP_DATA: {
                    String dumpId = PduCodec.TablespaceDumpData.readDumpId(message);
                    TableSpaceDumpReceiver receiver = dumpReceivers.get(dumpId);
                    LOGGER.log(Level.FINE, "receiver for {0}: {1}", new Object[]{dumpId, receiver});
                    if (receiver == null) {
                        if (channel != null) {
                            ByteBuf resp = PduCodec.ErrorResponse.write(message.messageId, "no such dump receiver " + dumpId);
                            channel.sendReplyMessage(message.messageId, resp);
                        }
                        return;
                    }
                    try {
                        String command = PduCodec.TablespaceDumpData.readCommand(message);
                        boolean sendAck = true;
                        switch (command) {
                            case "start": {
                                long ledgerId = PduCodec.TablespaceDumpData.readLedgerId(message);
                                long offset = PduCodec.TablespaceDumpData.readOffset(message);
                                receiver.start(new LogSequenceNumber(ledgerId, offset));
                                break;
                            }
                            case "beginTable": {
                                byte[] tableDefinition = PduCodec.TablespaceDumpData.readTableDefinition(message);
                                Table table = Table.deserialize(tableDefinition);
                                long estimatedSize = PduCodec.TablespaceDumpData.readEstimatedSize(message);
                                long dumpLedgerId = PduCodec.TablespaceDumpData.readLedgerId(message);
                                long dumpOffset = PduCodec.TablespaceDumpData.readOffset(message);
                                List<byte[]> indexesDef = PduCodec.TablespaceDumpData.readIndexesDefinition(message);
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
                                long ledgerId = PduCodec.TablespaceDumpData.readLedgerId(message);
                                long offset = PduCodec.TablespaceDumpData.readOffset(message);
                                receiver.finish(new LogSequenceNumber(ledgerId, offset));
                                sendAck = false;
                                break;
                            }
                            case "data": {
                                List<Record> records = new ArrayList<>();
                                PduCodec.TablespaceDumpData.readRecords(message, (key, value) -> {
                                    records.add(new Record(Bytes.from_array(key), Bytes.from_array(value)));
                                });
                                receiver.receiveTableDataChunk(records);
                                break;
                            }
                            case "txlog": {

                                List<DumpedLogEntry> records = new ArrayList<>();
                                PduCodec.TablespaceDumpData.readRecords(message, (key, value) -> {
                                    records.add(new DumpedLogEntry(LogSequenceNumber.deserialize(key), value));
                                });
                                receiver.receiveTransactionLogChunk(records);
                                break;
                            }
                            case "transactions": {
                                List<Transaction> transactions = new ArrayList<>();
                                PduCodec.TablespaceDumpData.readRecords(message, (key, value) -> {
                                    transactions.add(Transaction.deserialize(null, value));
                                });
                                receiver.receiveTransactionsAtDump(transactions);
                                break;
                            }
                            default:
                                throw new DataStorageManagerException("invalid dump command:" + command);
                        }
                        if (channel != null && sendAck) {
                            ByteBuf res = PduCodec.AckResponse.write(message.messageId);
                            channel.sendReplyMessage(message.messageId, res);
                        }
                    } catch (RuntimeException error) {
                        LOGGER.log(Level.SEVERE, "error while handling dump data", error);
                        receiver.onError(error);

                        if (channel != null) {
                            ByteBuf res = PduCodec.ErrorResponse.write(message.messageId, error);
                            channel.sendReplyMessage(message.messageId, res);
                        }
                    }
                }
                break;

            }
        } finally {
            message.close();
        }
    }

    @Override
    public void channelClosed(Channel channel) {
        connectionLock.writeLock().lock();
        try {
            // clean up local cache, if the server restarted we would use old ids
            preparedStatements.clear();
            if (channel == this.channel) {
                this.channel = null;
            }
        } finally {
            connectionLock.writeLock().unlock();
        }

    }

    Channel getChannel() {
        return channel;
    }

    public void close() {
        LOGGER.log(Level.FINER, "{0} - close", this);

        connectionLock.writeLock().lock();
        try {
            // clean up local cache
            preparedStatements.clear();
            if (channel != null) {
                channel.close();
            }
        } finally {
            channel = null;
            connectionLock.writeLock().unlock();
        }
    }

    private Channel ensureOpen() throws HDBException {
        connectionLock.readLock().lock();
        try {
            Channel channel = this.channel;
            if (channel != null && channel.isValid()) {
                return channel;
            }
            connectionLock.readLock().unlock();

            connectionLock.writeLock().lock();
            try {
                channel = this.channel;
                if (this.channel != null) {
                    if (channel.isValid()) {
                        return channel;
                    }

                    // channel is not valid, force close
                    channel.close();
                }
                // clean up local cache, if the server restarted we would use old ids
                preparedStatements.clear();
                LOGGER.log(Level.FINE, "{0} - connect to {1}:{2} ssh:{3}", new Object[]{this, server.getHost(), server.getPort(), server.isSsl()});
                channel = this.connection.getClient().createChannelTo(server, this);
                try {
                    performAuthentication(channel, server.getHost());
                    this.channel = channel;
                    return channel;
                } catch (TimeoutException err) {
                    LOGGER.log(Level.SEVERE, "Error", err);
                    if (channel != null) {
                        channel.close();
                    }
                    throw new HDBOperationTimeoutException(err);
                } catch (Exception err) {
                    LOGGER.log(Level.SEVERE, "Error", err);
                    if (channel != null) {
                        channel.close();
                    }
                    throw err;
                }
            } finally {
                connectionLock.writeLock().unlock();
                connectionLock.readLock().lock();
            }
        } catch (java.net.ConnectException err) {
            // this error will be retryed by the client
            throw new UnreachableServerException(err);
        } catch (HDBException err) {
            throw err;
        } catch (Exception err) {
            throw new HDBException(err);
        } finally {
            connectionLock.readLock().unlock();
        }
    }

    long prepareQuery(String tableSpace, String query) throws HDBException, ClientSideMetadataProviderException {
        long existing = preparedStatements.getQueryId(tableSpace, query);
        if (existing != 0) {
            return existing;
        }
        Channel channel = ensureOpen();
        try {
            long requestId = channel.generateRequestId();
            ByteBuf message = PduCodec.PrepareStatement.write(requestId, tableSpace, query);
            try (Pdu reply = channel.sendMessageWithPduReply(requestId, message, timeout)) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    handleGenericError(reply, 0);
                } else if (reply.type != Pdu.TYPE_PREPARE_STATEMENT_RESULT) {
                    throw new HDBException(reply);
                }
                long statementId = PduCodec.PrepareStatementResult.readStatementId(reply);
                preparedStatements.registerQueryId(tableSpace, query, statementId);
                return statementId;
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            throw new HDBException(err);
        }
    }

    DMLResult executeUpdate(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<Object> params) throws HDBException, ClientSideMetadataProviderException {
        Channel channel = ensureOpen();
        try {
            long requestId = channel.generateRequestId();
            long statementId = usePreparedStatement ? prepareQuery(tableSpace, query) : 0;
            query = statementId > 0 ? "" : query;
            ByteBuf message = PduCodec.ExecuteStatement.write(requestId, tableSpace, query, tx, returnValues, statementId, params);
            try (Pdu reply = channel.sendMessageWithPduReply(requestId, message, timeout)) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    handleGenericError(reply, statementId);
                } else if (reply.type != Pdu.TYPE_EXECUTE_STATEMENT_RESULT) {
                    throw new HDBException(reply);
                }
                long updateCount = PduCodec.ExecuteStatementResult.readUpdateCount(reply);
                long transactionId = PduCodec.ExecuteStatementResult.readTx(reply);
                boolean hasData = PduCodec.ExecuteStatementResult.hasRecord(reply);
                Object key = null;
                Map<RawString, Object> newvalue = null;
                if (hasData) {
                    PduCodec.ObjectListReader parametersReader = PduCodec.ExecuteStatementResult.readRecord(reply);
                    newvalue = readParametersListAsMap(parametersReader);
                    key = newvalue.get(RAWSTRING_KEY);
                }
                return new DMLResult(updateCount, key, newvalue, transactionId);
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            throw new HDBException(err);
        }
    }

    CompletableFuture<DMLResult> executeUpdateAsync(String tableSpace, String query, long tx, boolean returnValues, boolean usePreparedStatement, List<Object> params) {
        CompletableFuture<DMLResult> res = new CompletableFuture<>();
        try {
            Channel channel = ensureOpen();
            long requestId = channel.generateRequestId();
            long statementId = usePreparedStatement ? prepareQuery(tableSpace, query) : 0;
            query = statementId > 0 ? "" : query;
            ByteBuf message = PduCodec.ExecuteStatement.write(requestId, tableSpace, query, tx, returnValues, statementId, params);
            channel.sendRequestWithAsyncReply(requestId, message, timeout,
                    (msg, error) -> {
                        if (error != null) {
                            res.completeExceptionally(error);
                            return;
                        }
                        try (Pdu reply = msg) {
                            if (reply.type == Pdu.TYPE_ERROR) {
                                handleGenericError(reply, statementId);
                                return;
                            } else if (reply.type != Pdu.TYPE_EXECUTE_STATEMENT_RESULT) {
                                throw new HDBException(reply);
                            }
                            long updateCount = PduCodec.ExecuteStatementResult.readUpdateCount(reply);
                            long transactionId = PduCodec.ExecuteStatementResult.readTx(reply);
                            boolean hasData = PduCodec.ExecuteStatementResult.hasRecord(reply);
                            Object key = null;
                            Map<RawString, Object> newvalue = null;
                            if (hasData) {
                                PduCodec.ObjectListReader parametersReader = PduCodec.ExecuteStatementResult.readRecord(reply);
                                newvalue = readParametersListAsMap(parametersReader);
                                key = newvalue.get(RAWSTRING_KEY);
                            }
                            res.complete(new DMLResult(updateCount, key, newvalue, transactionId));
                        } catch (HDBException | ClientSideMetadataProviderException err) {
                            res.completeExceptionally(err);
                        }
                    });

        } catch (HDBException | ClientSideMetadataProviderException err) {
            res.completeExceptionally(err);
        }
        return res;
    }

    List<DMLResult> executeUpdates(
            String tableSpace, String query, long tx, boolean returnValues,
            boolean usePreparedStatement, List<List<Object>> batch
    ) throws HDBException, ClientSideMetadataProviderException {
        try {
            Channel channel = ensureOpen();
            long requestId = channel.generateRequestId();
            long statementId = usePreparedStatement ? prepareQuery(tableSpace, query) : 0;
            query = statementId > 0 ? "" : query;
            ByteBuf message = PduCodec.ExecuteStatements.write(requestId, tableSpace, query, tx, returnValues, statementId, batch);
            try (Pdu reply = channel.sendMessageWithPduReply(requestId, message, timeout)) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    handleGenericError(reply, statementId);
                    return Collections.emptyList(); // not possible, handleGenericError always throws an error
                } else if (reply.type != Pdu.TYPE_EXECUTE_STATEMENTS_RESULT) {
                    throw new HDBException(reply);
                }

                long transactionId = PduCodec.ExecuteStatementsResult.readTx(reply);
                List<Long> updateCounts = PduCodec.ExecuteStatementsResult.readUpdateCounts(reply);
                int numResults = updateCounts.size();

                List<DMLResult> results = new ArrayList<>(numResults);

                PduCodec.ListOfListsReader resultRecords = PduCodec.ExecuteStatementsResult.startResultRecords(reply);
                int numResultRecords = resultRecords.getNumLists();
                for (int i = 0; i < numResults; i++) {
                    Map<RawString, Object> newvalue = null;
                    Object key = null;
                    if (numResultRecords > 0) {
                        PduCodec.ObjectListReader list = resultRecords.nextList();
                        newvalue = readParametersListAsMap(list);
                        if (newvalue != null) {
                            key = newvalue.get(RAWSTRING_KEY);
                        }
                    }
                    long updateCount = updateCounts.get(i);
                    DMLResult res = new DMLResult(updateCount, key, newvalue, transactionId);
                    results.add(res);
                }
                return results;
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            throw new HDBException(err);
        }
    }

    CompletableFuture<List<DMLResult>> executeUpdatesAsync(
            String tableSpace, String query, long tx, boolean returnValues,
            boolean usePreparedStatement, List<List<Object>> batch
    ) {

        CompletableFuture<List<DMLResult>> res = new CompletableFuture<>();
        try {
            Channel channel = ensureOpen();
            long requestId = channel.generateRequestId();
            long statementId = usePreparedStatement ? prepareQuery(tableSpace, query) : 0;
            query = statementId > 0 ? "" : query;
            ByteBuf message = PduCodec.ExecuteStatements.write(requestId, tableSpace, query, tx, returnValues, statementId, batch);
            channel.sendRequestWithAsyncReply(requestId, message, timeout,
                    (msg, error) -> {
                        if (error != null) {
                            res.completeExceptionally(error);
                            return;
                        }
                        try (Pdu reply = msg) {
                            if (reply.type == Pdu.TYPE_ERROR) {
                                handleGenericError(reply, statementId);
                                return;
                            } else if (reply.type != Pdu.TYPE_EXECUTE_STATEMENTS_RESULT) {
                                throw new HDBException(reply);
                            }
                            long transactionId = PduCodec.ExecuteStatementsResult.readTx(reply);
                            List<Long> updateCounts = PduCodec.ExecuteStatementsResult.readUpdateCounts(reply);
                            int numResults = updateCounts.size();

                            List<DMLResult> results = new ArrayList<>(numResults);

                            PduCodec.ListOfListsReader resultRecords = PduCodec.ExecuteStatementsResult.startResultRecords(reply);
                            int numResultRecords = resultRecords.getNumLists();
                            for (int i = 0; i < numResults; i++) {
                                Map<RawString, Object> newvalue = null;
                                Object key = null;
                                if (numResultRecords > 0) {
                                    PduCodec.ObjectListReader list = resultRecords.nextList();
                                    newvalue = readParametersListAsMap(list);
                                    if (newvalue != null) {
                                        key = newvalue.get(RAWSTRING_KEY);
                                    }
                                }
                                long updateCount = updateCounts.get(i);
                                DMLResult _res = new DMLResult(updateCount, key, newvalue, transactionId);
                                results.add(_res);
                            }
                            res.complete(results);
                        } catch (HDBException | ClientSideMetadataProviderException err) {
                            res.completeExceptionally(err);
                        }
                    });

        } catch (HDBException | ClientSideMetadataProviderException err) {
            res.completeExceptionally(err);
        }
        return res;
    }

    GetResult executeGet(String tableSpace, String query, long tx, boolean usePreparedStatement, List<Object> params) throws HDBException, ClientSideMetadataProviderException {
        Channel channel = ensureOpen();
        try {
            long requestId = channel.generateRequestId();
            long statementId = usePreparedStatement ? prepareQuery(tableSpace, query) : 0;
            query = statementId > 0 ? "" : query;
            ByteBuf message = PduCodec.ExecuteStatement.write(requestId, tableSpace, query, tx, true, statementId, params);
            try (Pdu reply = channel.sendMessageWithPduReply(requestId, message, timeout)) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    handleGenericError(reply, statementId);
                } else if (reply.type != Pdu.TYPE_EXECUTE_STATEMENT_RESULT) {
                    throw new HDBException(reply);
                }
                long updateCount = PduCodec.ExecuteStatementResult.readUpdateCount(reply);
                long transactionId = PduCodec.ExecuteStatementResult.readTx(reply);
                boolean hasData = PduCodec.ExecuteStatementResult.hasRecord(reply);

                Map<RawString, Object> data = null;
                if (hasData) {
                    PduCodec.ObjectListReader parametersReader = PduCodec.ExecuteStatementResult.readRecord(reply);
                    data = readParametersListAsMap(parametersReader);
                }

                if (updateCount <= 0) {
                    return new GetResult(null, transactionId);
                } else {
                    return new GetResult(data, transactionId);
                }
            }

        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            throw new HDBException(err);
        }
    }

    Map<RawString, Object> readParametersListAsMap(PduCodec.ObjectListReader parametersReader) {
        Map<RawString, Object> data = new HashMap<>();
        for (int i = 0; i < parametersReader.getNumParams(); i += 2) {
            RawString _key = (RawString) parametersReader.nextObject();
            Object _value = parametersReader.nextObject();
            data.put(_key, _value);
        }
        return data;
    }

    long beginTransaction(String tableSpace) throws HDBException, ClientSideMetadataProviderException {
        Channel channel = ensureOpen();
        try {
            long requestId = channel.generateRequestId();
            ByteBuf message = PduCodec.TxCommand.write(requestId, PduCodec.TxCommand.TX_COMMAND_BEGIN_TRANSACTION, 0, tableSpace);
            try (Pdu reply = channel.sendMessageWithPduReply(requestId, message, timeout)) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    handleGenericError(reply, 0);
                    return -1; // not possible
                } else if (reply.type == Pdu.TYPE_TX_COMMAND_RESULT) {
                    long tx = PduCodec.TxCommandResult.readTx(reply);
                    if (tx <= 0) {
                        throw new HDBException("Server did not create a new transaction");
                    }
                    return tx;
                } else {
                    throw new HDBException(reply);
                }
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void commitTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        Channel channel = ensureOpen();
        try {
            long requestId = channel.generateRequestId();
            ByteBuf message = PduCodec.TxCommand.write(requestId, PduCodec.TxCommand.TX_COMMAND_COMMIT_TRANSACTION, tx, tableSpace);
            try (Pdu reply = channel.sendMessageWithPduReply(requestId, message, timeout)) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    handleGenericError(reply, 0);
                    return; // not possible
                } else if (reply.type != Pdu.TYPE_TX_COMMAND_RESULT) {
                    throw new HDBException(reply);
                }
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void rollbackTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        Channel channel = ensureOpen();
        try {
            long requestId = channel.generateRequestId();
            ByteBuf message = PduCodec.TxCommand.write(requestId, PduCodec.TxCommand.TX_COMMAND_ROLLBACK_TRANSACTION, tx, tableSpace);
            try (Pdu reply = channel.sendMessageWithPduReply(requestId, message, timeout)) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    handleGenericError(reply, 0);
                    return; // not possible
                } else if (reply.type != Pdu.TYPE_TX_COMMAND_RESULT) {
                    throw new HDBException(reply);
                }
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void handleGenericError(final Pdu reply, final long statementId) throws HDBException, ClientSideMetadataProviderException {
        handleGenericError(reply, statementId, false);
    }

    void handleGenericError(final Pdu reply, final long statementId, final boolean release) throws HDBException, ClientSideMetadataProviderException {
        boolean notLeader = PduCodec.ErrorResponse.readIsNotLeader(reply);
        boolean missingPreparedStatement = ErrorResponse.readIsMissingPreparedStatementError(reply);
        boolean sqlIntegrityViolation = ErrorResponse.readIsSqlIntegrityViolationError(reply);
        String msg = PduCodec.ErrorResponse.readError(reply);
        if (release) {
            reply.close();
        }
        if (notLeader) {
            throw new LeaderChangedException(msg);
        } else if (missingPreparedStatement) {
            LOGGER.log(Level.INFO, "Statement was flushed from server side cache " + msg);
            preparedStatements.invalidate(statementId);
            throw new RetryRequestException(msg);
        } else if (sqlIntegrityViolation) {
            LOGGER.log(Level.INFO, "SQLIntegrityViolationException: " + msg);
            throw new HDBException(msg, new SQLIntegrityConstraintViolationException(msg));
        } else {
            throw new HDBException(msg);
        }
    }

    ScanResultSet executeScan(String tableSpace, String query, boolean usePreparedStatement, List<Object> params, long tx, int maxRows, int fetchSize) throws HDBException, ClientSideMetadataProviderException {
        Channel channel = ensureOpen();
        Pdu reply = null;
        try {
            long scannerId = scannerIdGenerator.incrementAndGet();
            long requestId = channel.generateRequestId();
            long statementId = usePreparedStatement ? prepareQuery(tableSpace, query) : 0;
            query = statementId > 0 ? "" : query;
            ByteBuf message = PduCodec.OpenScanner.write(requestId, tableSpace, query, scannerId, tx, params, statementId,
                    fetchSize, maxRows);
            LOGGER.log(Level.FINEST, "open scanner {0} for query {1}, params {2}", new Object[]{scannerId, query, params});
            reply = channel.sendMessageWithPduReply(requestId, message, timeout);

            if (reply.type == Pdu.TYPE_ERROR) {
                handleGenericError(reply, statementId, true);
                return null; // not possible
            } else if (reply.type != Pdu.TYPE_RESULTSET_CHUNK) {
                HDBException err = new HDBException(reply);
                reply.close();
                throw err;
            }

            boolean last = PduCodec.ResultSetChunk.readIsLast(reply);
            long transactionId = PduCodec.ResultSetChunk.readTx(reply);
            RecordsBatch data = PduCodec.ResultSetChunk.startReadingData(reply);
            //LOGGER.log(Level.SEVERE, "received first " + initialFetchBuffer.size() + " records for query " + query);
            ScanResultSetImpl impl = new ScanResultSetImpl(scannerId, data, fetchSize, last, transactionId, channel);
            return impl;
        } catch (InterruptedException err) {
            if (reply != null) {
                reply.close();
            }
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            if (reply != null) {
                reply.close();
            }
            throw new HDBException(err);
        }
    }

    void dumpTableSpace(String tableSpace, int fetchSize, boolean includeTransactionLog, TableSpaceDumpReceiver receiver) throws HDBException, ClientSideMetadataProviderException {
        Channel channel = ensureOpen();
        try {
            String dumpId = this.clientId + ":" + scannerIdGenerator.incrementAndGet();
            long requestId = channel.generateRequestId();
            ByteBuf message = PduCodec.RequestTablespaceDump.write(requestId, tableSpace, dumpId, fetchSize, includeTransactionLog);
            LOGGER.log(Level.SEVERE, "dumpTableSpace id {0} for tablespace {1}", new Object[]{dumpId, tableSpace});
            dumpReceivers.put(dumpId, receiver);
            try (Pdu reply = channel.sendMessageWithPduReply(requestId, message, timeout)) {
                LOGGER.log(Level.SEVERE, "dumpTableSpace id {0} for tablespace {1}: first reply {2}", new Object[]{dumpId, tableSpace, reply});
                if (reply.type == Pdu.TYPE_ERROR) {
                    handleGenericError(reply, 0);
                } else if (reply.type != Pdu.TYPE_ACK) {
                    throw new HDBException(reply);
                }
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void restoreTableSpace(String tableSpace, TableSpaceRestoreSource source) throws HDBException, ClientSideMetadataProviderException {
        List<DumpedTableMetadata> tables = new ArrayList<>();
        try {
            while (true) {
                String entryType = source.nextEntryType();
                LOGGER.log(Level.FINEST, "restore, entryType:{0}", entryType);
                switch (entryType) {

                    case BackupFileConstants.ENTRY_TYPE_START: {
                        break;
                    }

                    case BackupFileConstants.ENTRY_TYPE_TABLE: {
                        DumpedTableMetadata table = source.nextTable();
                        Channel channel = ensureOpen();
                        long id = channel.generateRequestId();
                        ByteBuf message_create_table = PduCodec.RequestTableRestore.write(id, tableSpace,
                                table.table.serialize(), table.logSequenceNumber.ledgerId, table.logSequenceNumber.offset);
                        sendMessageAndCheckNoError(channel, id, message_create_table);
                        List<KeyValue> chunk = source.nextTableDataChunk();
                        while (chunk != null) {
                            id = channel.generateRequestId();
                            ByteBuf message = PduCodec.PushTableData.write(id, tableSpace, table.table.name, chunk);
                            sendMessageAndCheckNoError(channel, id, message);
                            chunk = source.nextTableDataChunk();
                        }

                        tables.add(table);
                        break;
                    }
                    case BackupFileConstants.ENTRY_TYPE_TXLOGCHUNK: {
                        Channel channel = ensureOpen();
                        List<KeyValue> chunk = source.nextTransactionLogChunk();
                        long id = channel.generateRequestId();
                        ByteBuf message = PduCodec.PushTxLogChunk.write(id, tableSpace, chunk);
                        sendMessageAndCheckNoError(channel, id, message);
                        break;
                    }
                    case BackupFileConstants.ENTRY_TYPE_TRANSACTIONS: {
                        Channel channel = ensureOpen();
                        List<byte[]> chunk = source.nextTransactionsBlock();
                        long id = channel.generateRequestId();
                        ByteBuf message = PduCodec.PushTransactionsBlock.write(id, tableSpace, chunk);
                        sendMessageAndCheckNoError(channel, id, message);
                        break;
                    }
                    case BackupFileConstants.ENTRY_TYPE_END: {
                        // send a 'table finished' event only at the end of the procedure
                        // the stream of transaction log entries is finished, so the data contained in the table is "final"
                        // we are going to create now all the indexes too
                        Channel channel = ensureOpen();
                        for (DumpedTableMetadata table : tables) {
                            List<byte[]> indexes = table.indexes.stream().map(Index::serialize).collect(Collectors.toList());

                            long id = channel.generateRequestId();
                            ByteBuf message_table_finished = PduCodec.TableRestoreFinished.write(id, tableSpace,
                                    table.table.name, indexes);
                            sendMessageAndCheckNoError(channel, id, message_table_finished);
                        }

                        long id = channel.generateRequestId();
                        ByteBuf message_restore_finished = PduCodec.RestoreFinished.write(id, tableSpace);
                        sendMessageAndCheckNoError(channel, id, message_restore_finished);

                        return;
                    }
                    default:
                        throw new HDBException("bad entryType " + entryType);
                }

            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new HDBException(err);
        } catch (TimeoutException err) {
            throw new HDBException(err);
        }
    }

    private void sendMessageAndCheckNoError(Channel channel, long id, ByteBuf message)
            throws HDBException, InterruptedException, TimeoutException {
        try (Pdu reply = channel.sendMessageWithPduReply(id, message, timeout)) {
            if (reply.type == Pdu.TYPE_ERROR) {
                throw new HDBException(reply);
            }
        }
    }

    private class ScanResultSetImpl extends ScanResultSet {

        private final long scannerId;
        private final ScanResultSetMetadata metadata;

        RecordsBatch fetchBuffer;
        DataAccessor next;
        boolean finished;
        boolean noMoreData;
        int fetchSize;
        boolean lastChunk;
        // it is important that all of the data is streamed
        // from the same channel
        // on the server side the ResultSet is bound to the connection
        // in order to ensure that resources are released
        // in case of client death
        final Channel channel;

        private ScanResultSetImpl(
                long scannerId, RecordsBatch firstFetchBuffer, int fetchSize, boolean onlyOneChunk, long tx,
                Channel channel
        ) {
            super(tx);
            this.channel = channel;
            this.scannerId = scannerId;
            this.metadata = new ScanResultSetMetadata(firstFetchBuffer.columnNames);
            this.fetchSize = fetchSize;
            this.fetchBuffer = firstFetchBuffer;
            if (firstFetchBuffer.isEmpty()) {
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

        @Override
        public void close() {
            finished = true;
            releaseBuffer();

            if (!noMoreData) {
                // try to release resources on the server
                // in case we did not consume the whole resultset
                long requestId = channel.generateRequestId();
                ByteBuf message = PduCodec.CloseScanner.write(requestId, scannerId);
                channel.sendOneWayMessage(message, (Throwable error) -> {
                    if (error != null) {
                        LOGGER.log(Level.SEVERE, "Cannot release scanner " + scannerId + ", con " + RoutedClientSideConnection.this, error);
                    }
                });

            }
        }

        private void releaseBuffer() {
            if (fetchBuffer != null) {
                fetchBuffer.release();
                fetchBuffer = null;
            }
        }

        @Override
        public boolean hasNext() throws HDBException {
            if (finished) {
                return false;
            }
            return ensureNext();
        }

        private void fillBuffer() throws HDBException {
            releaseBuffer();
            if (lastChunk) {
                noMoreData = true;
                return;
            }

            Pdu result = null;
            try {
                long requestId = channel.generateRequestId();
                ByteBuf message = PduCodec.FetchScannerData.write(requestId, scannerId, fetchSize);
                result = channel.sendMessageWithPduReply(requestId, message, timeout);

                //LOGGER.log(Level.SEVERE, "fillBuffer result " + result);
                if (result.type == Pdu.TYPE_ERROR) {
                    try {
                        throw new HDBException(result);
                    } finally {
                        result.close();
                    }
                }
                if (result.type != Pdu.TYPE_RESULTSET_CHUNK) {
                    finished = true;
                    try {
                        throw new HDBException("protocol error: " + result);
                    } finally {
                        result.close();
                    }
                }
                lastChunk = PduCodec.ResultSetChunk.readIsLast(result);
                fetchBuffer = PduCodec.ResultSetChunk.startReadingData(result);

                if (!fetchBuffer.hasNext()) {
                    noMoreData = true;
                }
            } catch (InterruptedException err) {
                if (result != null) {
                    result.close();
                }
                Thread.currentThread().interrupt();
                throw new HDBException(err);
            } catch (TimeoutException err) {
                if (result != null) {
                    result.close();
                }
                throw new HDBException(err);
            }
        }

        private boolean ensureNext() throws HDBException {
            if (next != null) {
                return true;
            }
            if (!fetchBuffer.hasNext()) {
                fillBuffer();
                if (noMoreData) {
                    finished = true;
                    return false;
                }
            }
            next = fetchBuffer.next();
            return true;
        }

        @Override
        public DataAccessor next() throws HDBException {
            if (finished) {
                throw new HDBException("Scanner is exhausted");
            }
            DataAccessor _next = next;
            next = null;
            return _next;
        }

        @Override
        public String getCursorName() {
            return "<scanner-" + scannerId + "-@tx" + transactionId + ">";
        }

    }

}
