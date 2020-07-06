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

import herddb.proto.Pdu;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract for two-way async comunication channels
 *
 * @author enrico.olivelli
 */
public abstract class Channel implements AutoCloseable {

    public interface PduCallback {

        void responseReceived(Pdu message, Throwable error);
    }

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

    public abstract void sendOneWayMessage(ByteBuf message, SendResultCallback callback);

    public abstract void sendReplyMessage(long inAnswerTo, ByteBuf message);

    public abstract void sendRequestWithAsyncReply(long id, ByteBuf message, long timeout, PduCallback callback);

    public abstract void channelIdle();

    public abstract String getRemoteAddress();

    @Override
    public abstract void close();

    private static final AtomicLong requestIdGeneator = new AtomicLong();

    public final long generateRequestId() {
        return requestIdGeneator.incrementAndGet();
    }

    public Pdu sendMessageWithPduReply(long id, ByteBuf request, long timeout) throws InterruptedException, TimeoutException {
        CompletableFuture<Pdu> resp = new CompletableFuture<>();
        long _start = System.currentTimeMillis();
        sendRequestWithAsyncReply(id, request, timeout, (Pdu message1, Throwable error) -> {
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
                TimeoutException te = new TimeoutException("io-error while waiting for reply from " + this.getRemoteAddress() + ": " + err.getCause());
                te.initCause(err.getCause());
                throw te;
            }
            throw new RuntimeException("Error " + err + " while talking to " + this.getRemoteAddress(), err.getCause());
        } catch (TimeoutException timeoutException) {
            long _stop = System.currentTimeMillis();
            TimeoutException err = new TimeoutException("Request timedout (" + ((_stop - _start) / 1000) + "s). Slow server " + this.getRemoteAddress() + " or internal error");
            err.initCause(timeoutException);
            throw err;
        }
    }

    public abstract boolean isValid();

    public abstract boolean isLocalChannel();


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
