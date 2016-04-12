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

import herddb.codec.RecordSerializer;
import herddb.model.DDLStatementExecutionResult;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.GetResult;
import herddb.model.Statement;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.TransactionResult;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.Message;
import herddb.network.ServerSideConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles a client Connection
 *
 * @author enrico.olivelli
 */
public class ServerSideConnectionPeer implements ServerSideConnection, ChannelEventListener {

    private static final Logger LOGGER = Logger.getLogger(ServerSideConnectionPeer.class.getName());
    private static final AtomicLong IDGENERATOR = new AtomicLong();
    private final long id = IDGENERATOR.incrementAndGet();
    private final Channel channel;
    private final Server server;

    public ServerSideConnectionPeer(Channel channel, Server server) {
        this.channel = channel;
        this.channel.setMessagesReceiver(this);
        this.server = server;
    }

    @Override
    public long getConnectionId() {
        return id;
    }

    @Override
    public void messageReceived(Message message) {
        LOGGER.log(Level.SEVERE, "messageReceived", message);
        Channel _channel = channel;
        switch (message.type) {
            case Message.TYPE_EXECUTE_STATEMENT:
                Long tx = (Long) message.parameters.get("tx");
                String query = (String) message.parameters.get("query");
                List<Object> parameters = (List<Object>) message.parameters.get("params");
                try {
                    Statement statement = server.getManager().getTranslator().translate(query, parameters);
                    if (tx != null && tx > 0) {
                        statement.setTransactionId(tx);
                    }
                    StatementExecutionResult result = server.getManager().executeStatement(statement);
                    if (result instanceof DMLStatementExecutionResult) {
                        DMLStatementExecutionResult dml = (DMLStatementExecutionResult) result;
                        _channel.sendReplyMessage(message, Message.EXECUTE_STATEMENT_RESULT(dml.getUpdateCount(), null));
                    } else if (result instanceof GetResult) {
                        GetResult get = (GetResult) result;
                        if (!get.found()) {
                            _channel.sendReplyMessage(message, Message.EXECUTE_STATEMENT_RESULT(0, null));
                        } else {
                            Map<String, Object> record = RecordSerializer.toBean(get.getRecord(), get.getTable());
                            _channel.sendReplyMessage(message, Message.EXECUTE_STATEMENT_RESULT(1, record));
                        }
                    } else if (result instanceof TransactionResult) {
                        TransactionResult txresult = (TransactionResult) result;
                        Map<String, Object> data = new HashMap<>();
                        data.put("tx", txresult.getTransactionId());
                        _channel.sendReplyMessage(message, Message.EXECUTE_STATEMENT_RESULT(1, data));
                    } else if (result instanceof DDLStatementExecutionResult) {
                        DDLStatementExecutionResult ddl = (DDLStatementExecutionResult) result;
                        _channel.sendReplyMessage(message, Message.EXECUTE_STATEMENT_RESULT(1, null));
                    } else {
                        _channel.sendReplyMessage(message, Message.ERROR(null, new Exception("unknown result type " + result.getClass() + " (" + result + ")")));
                    }
                } catch (StatementExecutionException err) {
                    _channel.sendReplyMessage(message, Message.ERROR(null, err));
                }
                break;

            default:
                _channel.sendReplyMessage(message, Message.ERROR(null, new Exception("unsupported message type " + message.type)));
        }
    }

    @Override
    public void channelClosed() {
        this.server.connectionClosed(this);
    }

}
