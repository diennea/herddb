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
import herddb.network.SendResultCallback;
import herddb.proto.Pdu;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Common implementation.
 * @author eolivelli
 */
public abstract class AbstractChannel extends Channel {

    private static final Logger LOGGER = Logger.getLogger(AbstractChannel.class.getName());
    public static final String ADDRESS_JVM_LOCAL = "jvm-local";
    private static final AtomicLong idGenerator = new AtomicLong();

    private final ConcurrentHashMap<Long, PduCallback> callbacks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> pendingReplyMessagesDeadline = new ConcurrentHashMap<>();
    private final ExecutorService callbackexecutor;
    protected boolean ioErrors = false;
    private final long id = idGenerator.incrementAndGet();
    private final String remoteAddress;

    public AbstractChannel(
            String name, String remoteAddress,
            ExecutorService callbackexecutor
    ) {
        super(name);
        this.callbackexecutor = callbackexecutor;
        this.remoteAddress = remoteAddress;
    }

    protected final long pendingCallbacks() {
        return callbacks.size();
    }

    public final long getId() {
        return id;
    }

    /**
     * This method is intended to be used while serving a PDU coming from Network
     * @param message
     */
    public final void pduReceived(Pdu message) {
        if (message.isRequest()) {
            handlePduRequest(message);
        } else {
            processPduResponse(message);
        }
    }

    /**
     * Esecute processing (mostly) in the same thread
     * @param message
     */
    public final void directProcessPdu(Pdu message) {
        if (message.isRequest()) {
            processRequest(message);
        } else {
            processPduResponse(message);
        }
    }

    private void processRequest(Pdu request) {
        try {
            messagesReceiver.requestReceived(request, this);
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, this + ": error " + t, t);
            close();
        }
    }

    private void handlePduRequest(Pdu request) {
        submitCallback(() -> {
            processRequest(request);
        });
    }

    private void processPduResponse(Pdu pdu) {
        long replyMessageId = pdu.messageId;
        if (replyMessageId < 0) {
            LOGGER.log(Level.SEVERE, "{0}: received response without replyId: type {1}", new Object[]{this, pdu.messageId});
            pdu.close();
            return;
        }
        final PduCallback callback = callbacks.remove(replyMessageId);
        pendingReplyMessagesDeadline.remove(replyMessageId);
        if (callback != null) {
            submitCallback(() -> {
                callback.responseReceived(pdu, null);
            });
        }
    }

    @Override
    public final void sendReplyMessage(long inAnswerTo, ByteBuf message) {

        if (!isValid()) {
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
        LOGGER.log(Level.SEVERE, "{0} found {1} without reply, channel will be closed", new Object[]{this, messagesWithNoReply});
        ioErrors = true;
        for (Long messageId : messagesWithNoReply) {
            PduCallback callback = callbacks.remove(messageId);
            if (callback != null) {
                submitCallback(() -> {
                    callback.responseReceived(null, new IOException(this + " reply timeout expired, channel will be closed"));
                });
            }
        }
        close();
    }

    @Override
    public final void sendRequestWithAsyncReply(long id, ByteBuf message, long timeout, PduCallback callback) {

        if (!isValid()) {
            callback.responseReceived(null, new Exception(this + " connection is not active"));
            return;
        }
        pendingReplyMessagesDeadline.put(id, System.currentTimeMillis() + timeout);
        callbacks.put(id, callback);
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

    private AtomicBoolean closed = new AtomicBoolean(false);

    protected abstract String describeSocket();
    protected abstract void doClose();

    @Override
    public final void close() {
        if (!closed.compareAndSet(false, true)) {
            // Already closed or in closing procedure
            return;
        }
        LOGGER.log(Level.FINE, "{0}: closing", this);
        String socketDescription = describeSocket();
        doClose();
        failPendingMessages(socketDescription);
    }

    @Override
    public final boolean isClosed() {
        return closed.get();
    }
    private void failPendingMessages(String socketDescription) {

        callbacks.forEach((key, callback) -> {
            pendingReplyMessagesDeadline.remove(key);
            LOGGER.log(Level.SEVERE, "{0} message {1} was not replied callback:{2}", new Object[]{this, key, callback});
            submitCallback(() -> {
                callback.responseReceived(null, new IOException("comunication channel is closed. Cannot wait for pending messages, socket=" + socketDescription));
            });
        });
        pendingReplyMessagesDeadline.clear();
        callbacks.clear();
    }

    final void exceptionCaught(Throwable cause) {
        LOGGER.log(Level.SEVERE, this + " io-error " + cause, cause);
        ioErrors = true;
    }

    final void channelClosed() {
        failPendingMessages(describeSocket());
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
    public final String getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public final void channelIdle() {
        LOGGER.log(Level.FINEST, "{0} channelIdle", this);
        processPendingReplyMessagesDeadline();
    }

}
