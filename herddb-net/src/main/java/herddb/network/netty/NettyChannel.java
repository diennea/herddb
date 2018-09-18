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
package herddb.network.netty;

import herddb.network.Channel;
import herddb.network.MessageBuilder;
import herddb.network.MessageWrapper;
import herddb.network.SendResultCallback;
import herddb.proto.flatbuf.Response;
import herddb.utils.SystemProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;

/**
 * Channel implemented on Netty
 *
 * @author enrico.olivelli
 */
public class NettyChannel extends Channel {

    volatile io.netty.channel.Channel socket;
    private static final Logger LOGGER = Logger.getLogger(NettyChannel.class.getName());
    private static final AtomicLong idGenerator = new AtomicLong();

    private final ConcurrentLongHashMap<ResponseCallback> pendingReplyRequests = new ConcurrentLongHashMap<>();
    private final ConcurrentLongHashMap<MessageBuilder> pendingReplyMessagesSource = new ConcurrentLongHashMap<>();
    private final ConcurrentLongLongHashMap pendingReplyMessagesDeadline = new ConcurrentLongLongHashMap();
    private final ExecutorService callbackexecutor;
    private boolean ioErrors = false;
    private final long id = idGenerator.incrementAndGet();
    private final String remoteAddress;
    private final AtomicInteger unflushedWrites = new AtomicInteger();

    @Override
    public String toString() {
        return "NettyChannel{name=" + name + ", id=" + id + ", socket=" + socket + " pending " + pendingReplyRequests.size() + " msgs}";
    }

    public NettyChannel(String name, io.netty.channel.Channel socket,
            ExecutorService callbackexecutor) {
        this.name = name;
        this.socket = socket;
        this.callbackexecutor = callbackexecutor;
        if (socket instanceof SocketChannel) {
            this.remoteAddress = ((SocketChannel) socket).remoteAddress() + "";
        } else {
            this.remoteAddress = "jvm-local";
        }
    }

    public long getId() {
        return id;
    }

    public void responseReceived(MessageWrapper message) {
        handleResponse(message);
    }

    public void requestReceived(MessageWrapper request) {
        submitCallback(() -> {
            try {
                messagesReceiver.requestReceived(request, this);
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, this + ": error " + t, t);
                close();
            }
        });
    }

    private void handleResponse(MessageWrapper anwermessagewrapper) {
        Response anwermessage = anwermessagewrapper.getResponse();
        long replyMessageId = anwermessage.replyMessageId();
        if (replyMessageId < 0) {
            LOGGER.log(Level.SEVERE, "{0}: received response without replyId: type {1}", new Object[]{this, anwermessage.type()});
            anwermessagewrapper.close();
            return;
        }
        final ResponseCallback callback = pendingReplyRequests.remove(replyMessageId);
        pendingReplyMessagesDeadline.remove(replyMessageId);
        if (callback != null) {
            submitCallback(() -> {
                callback.responseReceived(anwermessagewrapper, null);
            });
        }
    }

    @Override
    public void sendOneWayMessage(ByteBuf message, SendResultCallback callback) {

        io.netty.channel.Channel _socket = this.socket;
        if (_socket == null || !_socket.isOpen()) {
            callback.messageSent(new Exception(this + " connection is closed"));
            return;
        }
        if (LOGGER.isLoggable(Level.FINEST)) {
            StringBuilder dumper = new StringBuilder();
            ByteBufUtil.appendPrettyHexDump(dumper, message);
            LOGGER.log(Level.FINEST, "Sending to {}: {}", new Object[]{_socket, dumper});
        }
        _socket.writeAndFlush(message).addListener(new GenericFutureListener() {

            @Override
            public void operationComplete(Future future) throws Exception {
                if (future.isSuccess()) {
                    callback.messageSent(null);
                } else {
                    LOGGER.log(Level.SEVERE, this + ": error " + future.cause(), future.cause());
                    callback.messageSent(future.cause());
                    close();
                }
            }
        });
        unflushedWrites.incrementAndGet();
    }

    @Override
    public void sendReplyMessage(long inAnswerTo, ByteBuf message) {

        if (this.socket == null) {
            LOGGER.log(Level.SEVERE, this + " channel not active, discarding reply message " + message);
            return;
        }

        sendOneWayMessage(message, new SendResultCallback() {

            @Override
            public void messageSent(Throwable error) {
                if (error != null) {
                    LOGGER.log(Level.SEVERE, this + " error:" + error, error);
                }
            }
        });
    }

    private void processPendingReplyMessagesDeadline() {
        List<Long> messagesWithNoReply = new ArrayList<>();
        long now = System.currentTimeMillis();
        pendingReplyMessagesDeadline.forEach((messageId, deadline) -> {
            if (deadline < now) {
                messagesWithNoReply.add(messageId);
            }
        });
        if (messagesWithNoReply.isEmpty()) {
            return;
        }
        LOGGER.log(Level.SEVERE, this + " found " + messagesWithNoReply + " without reply, channel will be closed");
        ioErrors = true;
        for (long messageId : messagesWithNoReply) {
            ResponseCallback callback = pendingReplyRequests.remove(messageId);
            if (callback != null) {
                submitCallback(() -> {
                    callback.responseReceived(null, new IOException(this + " reply timeout expired, channel will be closed"));
                });
            }
        }
        close();
    }

    @Override
    protected void sendRequestWithAsyncReply(long id, ByteBuf message, long timeout, ResponseCallback callback) {

        if (!isValid()) {
            callback.responseReceived(null, new Exception(this + " connection is not active"));
            return;
        }
        pendingReplyMessagesDeadline.put(id, System.currentTimeMillis() + timeout);
        pendingReplyRequests.put(id, callback);
        sendOneWayMessage(message, new SendResultCallback() {

            @Override
            public void messageSent(Throwable error) {
                if (error != null) {
                    LOGGER.log(Level.SEVERE, this + ": error while sending reply message to " + message, error);
                    callback.responseReceived(null, new Exception(this + ": error while sending reply message to " + message, error));
                }
            }
        });
    }

    @Override
    public boolean isValid() {
        io.netty.channel.Channel _socket = socket;
        return _socket != null && _socket.isOpen() && !ioErrors;
    }

    private volatile boolean closed = false;

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        LOGGER.log(Level.FINE, "{0}: closing", this);
        String socketDescription = socket + "";
        if (socket != null) {
            try {
                socket.close().await();
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
            } finally {
                socket = null;
            }
        }
        failPendingMessages(socketDescription);
    }

    private void failPendingMessages(String socketDescription) {
        pendingReplyRequests.forEach((key, callback) -> {
            pendingReplyMessagesDeadline.remove(key);
            LOGGER.log(Level.SEVERE, "{0} message {1} was not replied callback:{2}", new Object[]{this, key, callback});
            submitCallback(() -> {
                callback.responseReceived(null, new IOException("comunication channel is closed. Cannot wait for pending messages, socket=" + socketDescription));
            });
        });
        pendingReplyRequests.clear();
        pendingReplyMessagesSource.clear();
        pendingReplyMessagesDeadline.clear();
    }

    void exceptionCaught(Throwable cause) {
        LOGGER.log(Level.SEVERE, this + " io-error " + cause, cause);
        ioErrors = true;
    }

    void channelClosed() {
        failPendingMessages(socket + "");
        submitCallback(() -> {
            if (this.messagesReceiver != null) {
                this.messagesReceiver.channelClosed(this);
            }
        });
    }

    private void submitCallback(Runnable runnable) {
        try {
            callbackexecutor.submit(runnable);
        } catch (RejectedExecutionException stopped) {
            LOGGER.log(Level.SEVERE, this + " rejected runnable " + runnable + ":" + stopped);
            try {
                runnable.run();
            } catch (Throwable error) {
                LOGGER.log(Level.SEVERE, this + " error on rejected runnable " + runnable + ":" + error);
            }
        }
    }

    @Override
    public String getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void channelIdle() {
        LOGGER.log(Level.FINEST, "{0} channelIdle", this);
        processPendingReplyMessagesDeadline();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }
    
}
