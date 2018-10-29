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
import herddb.utils.KeyValue;
import herddb.network.MessageBuilder;
import herddb.network.MessageWrapper;
import herddb.network.ServerHostData;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.proto.flatbuf.MessageType;
import herddb.proto.flatbuf.Request;
import herddb.proto.flatbuf.Response;
import herddb.security.sasl.SaslNettyClient;
import herddb.security.sasl.SaslUtils;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.MessageUtils;
import herddb.utils.RawString;
import herddb.utils.RecordsBatch;
import io.netty.buffer.ByteBuf;
import java.util.HashMap;

/**
 * A real connection to a server
 *
 * @author enrico.olivelli
 */
public class RoutedClientSideConnection implements AutoCloseable, ChannelEventListener {

    private static final Logger LOGGER = Logger.getLogger(RoutedClientSideConnection.class.getName());
    private static final RawString RAWSTRING_KEY = RawString.of("_key");

    private final HDBConnection connection;
    private final String nodeId;
    private final long timeout;
    private final ServerHostData server;
    private final String clientId;
    private final ReentrantReadWriteLock connectionLock = new ReentrantReadWriteLock(true);
    private volatile Channel channel;

    private final Map<RawString, TableSpaceDumpReceiver> dumpReceivers = new ConcurrentHashMap<>();

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

        long requestId = _channel.generateRequestId();
        Pdu saslResponse = _channel.sendMessageWithPduReply(requestId,
                PduCodec.SaslTokenMessageRequest.write(requestId, SaslUtils.AUTH_DIGEST_MD5, firstToken), timeout);
        try {
            for (int i = 0; i < 100; i++) {
                byte[] responseToSendToServer;
                switch (saslResponse.type) {
                    case MessageType.TYPE_SASL_TOKEN_SERVER_RESPONSE:
                        byte[] token = PduCodec.SaslTokenServerResponse.readToken(saslResponse);
                        responseToSendToServer = saslNettyClient.evaluateChallenge(token);
                        requestId = _channel.generateRequestId();
                        saslResponse.close();
                        saslResponse = _channel.sendMessageWithPduReply(requestId,
                                PduCodec.SaslTokenMessageToken.write(requestId, responseToSendToServer, true), timeout);
                        if (saslNettyClient.isComplete()) {
                            LOGGER.finest("SASL auth completed with success");
                            return;
                        }
                        break;
                    case MessageType.TYPE_ERROR:
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
    public void requestReceived(MessageWrapper messageWrapper, Channel _channel) {
        try {
            Request message = messageWrapper.getRequest();
            switch (message.type()) {
                case MessageType.TYPE_TABLESPACE_DUMP_DATA: {
                    RawString dumpId = MessageUtils.readRawString(message.dumpIdAsByteBuffer());
                    TableSpaceDumpReceiver receiver = dumpReceivers.get(dumpId);
                    LOGGER.log(Level.FINE, "receiver for {0}: {1}", new Object[]{dumpId, receiver});
                    if (receiver == null) {
                        if (_channel != null) {
                            _channel.sendReplyMessage(message.id(), MessageBuilder.ERROR(message.id(), new Exception("no such dump receiver " + dumpId)));
                        }
                        return;
                    }
                    try {
                        String command = message.command();
                        boolean sendAck = true;
                        switch (command) {
                            case "start": {
                                long ledgerId = message.dumpLedgerId();
                                long offset = message.dumpOffset();
                                receiver.start(new LogSequenceNumber(ledgerId, offset));
                                break;
                            }
                            case "beginTable": {
                                byte[] tableDefinition = MessageUtils.bufferToArray(message.tableDefinition().schemaAsByteBuffer());
                                Table table = Table.deserialize(tableDefinition);
                                Long estimatedSize = message.estimatedSize();
                                long dumpLedgerId = message.dumpLedgerId();
                                long dumpOffset = message.dumpOffset();
                                List<byte[]> indexesDef = new ArrayList<>();
                                int numIndexes = message.indexesDefinitionLength();
                                for (int i = 0; i < numIndexes; i++) {
                                    indexesDef.add(MessageUtils.bufferToArray(message.indexesDefinition(i).schemaAsByteBuffer()));
                                }
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
                                long ledgerId = message.dumpLedgerId();
                                long offset = message.dumpOffset();
                                receiver.finish(new LogSequenceNumber(ledgerId, offset));
                                sendAck = false;
                                break;
                            }
                            case "data": {
                                int dataSize = message.rawDataChunkLength();
                                List<KeyValue> data = new ArrayList<>(dataSize);
                                for (int i = 0; i < dataSize; i++) {
                                    herddb.proto.flatbuf.KeyValue rawDataChunkEntry = message.rawDataChunk(i);
                                    byte[] key = MessageUtils.bufferToArray(rawDataChunkEntry.keyAsByteBuffer());
                                    byte[] value = MessageUtils.bufferToArray(rawDataChunkEntry.valueAsByteBuffer());
                                    data.add(new KeyValue(key, value));
                                }

                                List<Record> records = new ArrayList<>(data.size());
                                for (KeyValue kv : data) {
                                    records.add(new Record(new Bytes(kv.key), new Bytes(kv.value)));
                                }
                                receiver.receiveTableDataChunk(records);
                                break;
                            }
                            case "txlog": {
                                int dataSize = message.rawDataChunkLength();
                                List<KeyValue> data = new ArrayList<>(dataSize);
                                for (int i = 0; i < dataSize; i++) {
                                    herddb.proto.flatbuf.KeyValue rawDataChunkEntry = message.rawDataChunk(i);
                                    byte[] key = MessageUtils.bufferToArray(rawDataChunkEntry.keyAsByteBuffer());
                                    byte[] value = MessageUtils.bufferToArray(rawDataChunkEntry.valueAsByteBuffer());
                                    data.add(new KeyValue(key, value));
                                }
                                List<DumpedLogEntry> records = new ArrayList<>(data.size());
                                for (KeyValue kv : data) {
                                    records.add(new DumpedLogEntry(LogSequenceNumber.deserialize(kv.key), kv.value));
                                }
                                receiver.receiveTransactionLogChunk(records);
                                break;
                            }
                            case "transactions": {
                                int dataSize = message.rawDataChunkLength();
                                List<KeyValue> data = new ArrayList<>(dataSize);
                                for (int i = 0; i < dataSize; i++) {
                                    herddb.proto.flatbuf.KeyValue rawDataChunkEntry = message.rawDataChunk(i);
                                    byte[] key = MessageUtils.bufferToArray(rawDataChunkEntry.keyAsByteBuffer());
                                    byte[] value = MessageUtils.bufferToArray(rawDataChunkEntry.valueAsByteBuffer());
                                    data.add(new KeyValue(key, value));
                                }
                                List<Transaction> transactions = data.stream().map(array -> {
                                    return Transaction.deserialize(null, array.value);
                                }).collect(Collectors.toList());
                                receiver.receiveTransactionsAtDump(transactions);
                                break;
                            }
                            default:
                                throw new DataStorageManagerException("invalid dump command:" + command);
                        }
                        if (_channel != null && sendAck) {
                            _channel.sendReplyMessage(message.id(), MessageBuilder.ACK(message.id()));
                        }
                    } catch (DataStorageManagerException error) {
                        LOGGER.log(Level.SEVERE, "error while handling dump data", error);
                        if (_channel != null) {
                            _channel.sendReplyMessage(message.id(), MessageBuilder.ERROR(message.id(), error));
                        }
                    }
                }
                break;

            }
        } finally {
            messageWrapper.close();
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
                    LOGGER.log(Level.SEVERE, "Error", err);
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
            long requestId = _channel.generateRequestId();
            ByteBuf message = MessageBuilder.EXECUTE_STATEMENT(requestId, tableSpace, query, tx, returnValues, params);
            try (MessageWrapper replyWrapper = _channel.sendMessageWithReply(requestId, message, timeout);) {
                Response reply = replyWrapper.getResponse();
                if (reply.type() == MessageType.TYPE_ERROR) {
                    boolean notLeader = reply.notLeader();
                    if (notLeader) {
                        this.connection.requestMetadataRefresh();
                        throw new RetryRequestException(reply + "");
                    }
                    throw new HDBException(reply);
                }
                long updateCount = (Long) reply.updateCount();
                long transactionId = (Long) reply.tx();

                Object key = null;
                Map<RawString, Object> newvalue = MessageUtils.decodeMap(reply.newValue());
                if (newvalue != null) {
                    key = newvalue.get(RAWSTRING_KEY);
                }
                return new DMLResult(updateCount, key, newvalue, transactionId);
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    List<DMLResult> executeUpdates(String tableSpace, String query, long tx, boolean returnValues, List<List<Object>> batch) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            long requestId = _channel.generateRequestId();
            ByteBuf message = MessageBuilder.EXECUTE_STATEMENTS(requestId, tableSpace, query, tx, returnValues, batch);
            try (MessageWrapper replyWrapper = _channel.sendMessageWithReply(requestId, message, timeout);) {
                Response reply = replyWrapper.getResponse();
                if (reply.type() == MessageType.TYPE_ERROR) {
                    boolean notLeader = reply.notLeader();
                    if (notLeader) {
                        this.connection.requestMetadataRefresh();
                        throw new RetryRequestException(reply + "");
                    }
                    throw new HDBException(reply);
                }

                long transactionId = reply.tx();
                int numResults = reply.batchResultsLength();

                List<DMLResult> results = new ArrayList<>(numResults);

                for (int i = 0; i < numResults; i++) {
                    herddb.proto.flatbuf.DMLResult result = reply.batchResults(i);
                    long updateCount = result.updateCount();
                    Object key = null;
                    Map<RawString, Object> newvalue = MessageUtils.decodeMap(reply.newValue());
                    if (newvalue != null) {
                        key = newvalue.get(RAWSTRING_KEY);
                    }

                    DMLResult res = new DMLResult(updateCount, key, newvalue, transactionId);
                    results.add(res);
                }
                return results;
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    GetResult executeGet(String tableSpace, String query, long tx, List<Object> params) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            long requestId = _channel.generateRequestId();
            ByteBuf message = MessageBuilder.EXECUTE_STATEMENT(requestId, tableSpace, query, tx, false, params);
            try (MessageWrapper replyWrapper = _channel.sendMessageWithReply(requestId, message, timeout);) {
                Response reply = replyWrapper.getResponse();
                if (reply.type() == MessageType.TYPE_ERROR) {
                    boolean notLeader = reply.notLeader();
                    if (notLeader) {
                        this.connection.requestMetadataRefresh();
                        throw new RetryRequestException(reply + "");
                    }
                    throw new HDBException(reply);
                }
                long found = reply.updateCount();
                long transactionId = reply.tx();
                if (found <= 0) {
                    return new GetResult(null, transactionId);
                } else {
                    Map<RawString, Object> data = MessageUtils.decodeMap(reply.newValue());
                    return new GetResult(data, transactionId);
                }
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    long beginTransaction(String tableSpace) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            long requestId = _channel.generateRequestId();
            ByteBuf message = PduCodec.TxCommand.write(requestId, PduCodec.TxCommand.TX_COMMAND_BEGIN_TRANSACTION, 0, tableSpace);
            try (Pdu reply = _channel.sendMessageWithPduReply(requestId, message, timeout);) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    boolean notLeader = PduCodec.ErrorResponse.readIsNotLeader(reply);
                    if (notLeader) {
                        this.connection.requestMetadataRefresh();
                        throw new RetryRequestException(reply + "");
                    }
                    throw new HDBException(reply);
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
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void commitTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            long requestId = _channel.generateRequestId();
            ByteBuf message = PduCodec.TxCommand.write(requestId, PduCodec.TxCommand.TX_COMMAND_COMMIT_TRANSACTION, tx, tableSpace);
            try (Pdu reply = _channel.sendMessageWithPduReply(requestId, message, timeout);) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    boolean notLeader = PduCodec.ErrorResponse.readIsNotLeader(reply);
                    if (notLeader) {
                        this.connection.requestMetadataRefresh();
                        throw new RetryRequestException(reply + "");
                    }
                    throw new HDBException(reply);
                } else if (reply.type == Pdu.TYPE_TX_COMMAND_RESULT) {
                    return;
                } else {
                    throw new HDBException(reply);
                }
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    void rollbackTransaction(String tableSpace, long tx) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            long requestId = _channel.generateRequestId();
            ByteBuf message = PduCodec.TxCommand.write(requestId, PduCodec.TxCommand.TX_COMMAND_ROLLBACK_TRANSACTION, tx, tableSpace);
            try (Pdu reply = _channel.sendMessageWithPduReply(requestId, message, timeout);) {
                if (reply.type == Pdu.TYPE_ERROR) {
                    boolean notLeader = PduCodec.ErrorResponse.readIsNotLeader(reply);
                    if (notLeader) {
                        this.connection.requestMetadataRefresh();
                        throw new RetryRequestException(reply + "");
                    }
                    throw new HDBException(reply);
                } else if (reply.type == Pdu.TYPE_TX_COMMAND_RESULT) {
                    return;
                } else {
                    throw new HDBException(reply);
                }
            }
        } catch (InterruptedException | TimeoutException err) {
            throw new HDBException(err);
        }
    }

    private static final AtomicLong SCANNERID_GENERATOR = new AtomicLong();

    ScanResultSet executeScan(String tableSpace, String query, List<Object> params, long tx, int maxRows, int fetchSize) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        MessageWrapper reply = null;
        try {
            String scannerId = this.clientId + ":" + SCANNERID_GENERATOR.incrementAndGet();
            long requestId = _channel.generateRequestId();
            ByteBuf message = MessageBuilder.OPEN_SCANNER(requestId, tableSpace, query, scannerId, tx, params, fetchSize, maxRows);
            LOGGER.log(Level.FINEST, "open scanner {0} for query {1}, params {2}", new Object[]{scannerId, query, params});
            reply = _channel.sendMessageWithReply(requestId, message, timeout);
            Response response = reply.getResponse();
            if (response.type() == MessageType.TYPE_ERROR) {
                boolean notLeader = response.notLeader();
                if (notLeader) {
                    reply.close();
                    this.connection.requestMetadataRefresh();
                    throw new RetryRequestException(reply + "");
                }
                try {
                    throw new HDBException(response);
                } finally {
                    reply.close();
                }
            }
            RecordsBatch data = new RecordsBatch(reply);
            boolean last = (Boolean) response.last();
            long transactionId = (Long) response.tx();
            //LOGGER.log(Level.SEVERE, "received first " + initialFetchBuffer.size() + " records for query " + query);
            ScanResultSetImpl impl = new ScanResultSetImpl(scannerId, data, fetchSize, last, transactionId);
            return impl;
        } catch (InterruptedException | TimeoutException err) {
            if (reply != null) {
                reply.close();
            }
            throw new HDBException(err);
        }
    }

    void dumpTableSpace(String tableSpace, int fetchSize, boolean includeTransactionLog, TableSpaceDumpReceiver receiver) throws HDBException, ClientSideMetadataProviderException {
        Channel _channel = ensureOpen();
        try {
            String dumpId = this.clientId + ":" + SCANNERID_GENERATOR.incrementAndGet();
            long requestId = _channel.generateRequestId();
            ByteBuf message = MessageBuilder.REQUEST_TABLESPACE_DUMP(requestId, tableSpace, dumpId, fetchSize, includeTransactionLog);
            LOGGER.log(Level.SEVERE, "dumpTableSpace id {0} for tablespace {1}", new Object[]{dumpId, tableSpace});
            dumpReceivers.put(RawString.of(dumpId), receiver);
            try (MessageWrapper replyWrapper = _channel.sendMessageWithReply(requestId, message, timeout);) {
                Response reply = replyWrapper.getResponse();
                LOGGER.log(Level.SEVERE, "dumpTableSpace id {0} for tablespace {1}: first reply {2}", new Object[]{dumpId, tableSpace, reply});
                if (reply.type() == MessageType.TYPE_ERROR) {
                    boolean notLeader = reply.notLeader();
                    if (notLeader) {
                        this.connection.requestMetadataRefresh();
                        throw new RetryRequestException(reply + "");
                    }
                    throw new HDBException(reply);
                }
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
                        long id = _channel.generateRequestId();
                        ByteBuf message_create_table = MessageBuilder.REQUEST_TABLE_RESTORE(id, tableSpace,
                                table.table.serialize(), table.logSequenceNumber.ledgerId, table.logSequenceNumber.offset);
                        sendMessageAndCheckNoError(_channel, id, message_create_table);
                        List<KeyValue> chunk = source.nextTableDataChunk();
                        while (chunk != null) {
                            id = _channel.generateRequestId();
                            ByteBuf message = MessageBuilder.PUSH_TABLE_DATA(id, tableSpace, table.table.name, chunk);
                            sendMessageAndCheckNoError(_channel, id, message);
                            chunk = source.nextTableDataChunk();
                        }

                        tables.add(table);
                        break;
                    }
                    case BackupFileConstants.ENTRY_TYPE_TXLOGCHUNK: {
                        Channel _channel = ensureOpen();
                        List<KeyValue> chunk = source.nextTransactionLogChunk();
                        long id = _channel.generateRequestId();
                        ByteBuf message = MessageBuilder.PUSH_TXLOGCHUNK(id, tableSpace, chunk);
                        sendMessageAndCheckNoError(_channel, id, message);
                        break;
                    }
                    case BackupFileConstants.ENTRY_TYPE_TRANSACTIONS: {
                        Channel _channel = ensureOpen();
                        List<byte[]> chunk = source.nextTransactionsBlock();
                        long id = _channel.generateRequestId();
                        ByteBuf message = MessageBuilder.PUSH_TRANSACTIONSBLOCK(id, tableSpace, chunk);
                        sendMessageAndCheckNoError(_channel, id, message);
                        break;
                    }
                    case BackupFileConstants.ENTRY_TYPE_END: {
                        // send a 'table finished' event only at the end of the procedure
                        // the stream of transaction log entries is finished, so the data contained in the table is "final"
                        // we are going to create now all the indexes too
                        Channel _channel = ensureOpen();
                        for (DumpedTableMetadata table : tables) {
                            List<byte[]> indexes = table.indexes.stream().map(Index::serialize).collect(Collectors.toList());

                            long id = _channel.generateRequestId();
                            ByteBuf message_table_finished = MessageBuilder.TABLE_RESTORE_FINISHED(id, tableSpace,
                                    table.table.name, indexes);
                            sendMessageAndCheckNoError(_channel, id, message_table_finished);
                        }

                        long id = _channel.generateRequestId();
                        ByteBuf message_restore_finished = MessageBuilder.RESTORE_FINISHED(id, tableSpace);
                        sendMessageAndCheckNoError(_channel, id, message_restore_finished);

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

    private void sendMessageAndCheckNoError(Channel _channel, long id, ByteBuf message)
            throws HDBException, InterruptedException, TimeoutException {
        try (MessageWrapper reply = _channel.sendMessageWithReply(id, message, timeout);) {
            if (reply.getResponse().type() == MessageType.TYPE_ERROR) {
                throw new HDBException(reply.getResponse());
            }
        }
    }

    private class ScanResultSetImpl extends ScanResultSet {

        private final String scannerId;
        private final ScanResultSetMetadata metadata;

        RecordsBatch fetchBuffer;
        DataAccessor next;
        boolean finished;
        boolean noMoreData;
        int fetchSize;
        boolean lastChunk;

        private ScanResultSetImpl(String scannerId, RecordsBatch firstFetchBuffer, int fetchSize, boolean onlyOneChunk, long tx) {
            super(tx);

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
            Channel _channel = ensureOpen();
            MessageWrapper result = null;
            try {
                long requestId = _channel.generateRequestId();
                ByteBuf message = MessageBuilder.FETCH_SCANNER_DATA(requestId, scannerId, fetchSize);
                result = _channel.sendMessageWithReply(requestId, message, 10000);
                Response response = result.getResponse();
                //LOGGER.log(Level.SEVERE, "fillBuffer result " + result);
                if (response.type() == MessageType.TYPE_ERROR) {
                    try {
                        throw new HDBException(result.getResponse());
                    } finally {
                        result.close();
                    }
                }
                if (response.type() != MessageType.TYPE_RESULTSET_CHUNK) {
                    finished = true;
                    throw new HDBException("protocol error: " + result);
                }
                fetchBuffer = new RecordsBatch(result);
                lastChunk = response.last();
                if (!fetchBuffer.hasNext()) {
                    noMoreData = true;
                }
            } catch (InterruptedException | TimeoutException err) {
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

    }

}
