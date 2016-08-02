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
package herddb.network;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Abstract for two-way async comunication channels
 *
 * @author enrico.olivelli
 */
public abstract class Channel implements AutoCloseable {

    protected ChannelEventListener messagesReceiver;
    protected String name = "unnamed";

    public Channel() {
    }

    public ChannelEventListener getMessagesReceiver() {
        return messagesReceiver;
    }

    public void setMessagesReceiver(ChannelEventListener messagesReceiver) {
        this.messagesReceiver = messagesReceiver;
    }

    public abstract void sendOneWayMessage(Message message, SendResultCallback callback);

    public abstract void sendReplyMessage(Message inAnswerTo, Message message);

    public abstract void sendMessageWithAsyncReply(Message message, long timeout, ReplyCallback callback);

    public abstract void channelIdle();

    public abstract String getRemoteAddress();

    @Override
    public abstract void close();

    public Message sendMessageWithReply(Message message, long timeout) throws InterruptedException, TimeoutException {
        CompletableFuture<Message> resp = new CompletableFuture<>();
        long _start = System.currentTimeMillis();
        sendMessageWithAsyncReply(message, timeout, (Message originalMessage, Message message1, Throwable error) -> {
            if (error != null) {
                resp.completeExceptionally(error);
            } else {
                resp.complete(message1);
            }
        });
        try {
            return resp.get(timeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException err) {
            if (err.getCause() instanceof IOException) {
                TimeoutException te = new TimeoutException("io-error while waiting for reply: " + err.getCause());
                te.initCause(err.getCause());
                throw te;
            }
            throw new RuntimeException(err.getCause());
        } catch (TimeoutException timeoutException) {
            long _stop = System.currentTimeMillis();
            TimeoutException err = new TimeoutException("Timed-out after waitin response for " + (_stop - _start) + " ms");
            err.initCause(timeoutException);
            throw err;
        }
    }

    public abstract boolean isValid();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
