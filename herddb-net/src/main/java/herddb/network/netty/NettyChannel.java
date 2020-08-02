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

import herddb.network.SendResultCallback;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Channel implemented on Netty
 *
 * @author enrico.olivelli
 */
public class NettyChannel extends AbstractChannel {

    volatile io.netty.channel.Channel socket;
    protected final AtomicInteger unflushedWrites = new AtomicInteger();
    private static final Logger LOGGER = Logger.getLogger(NettyChannel.class.getName());

    @Override
    public String toString() {
        return "NettyChannel{name=" + getName() + ", id=" + getId() + ", socket=" + socket + " pending " + pendingCallbacks() + " msgs}";
    }

    public NettyChannel(
            String name, io.netty.channel.Channel socket,
            ExecutorService callbackexecutor
    ) {
        super(name, ((SocketChannel) socket).remoteAddress() + "", callbackexecutor);
        this.socket = socket;
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
    public boolean isValid() {
        io.netty.channel.Channel _socket = socket;
        return _socket != null && _socket.isOpen() && !ioErrors;
    }

    @Override
    public boolean isLocalChannel() {
        return false;
    }

    @Override
    public void doClose() {
        if (socket != null) {
            try {
                socket.close().await();
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
            } finally {
                socket = null;
            }
        }
    }

// visible for testing only
    public io.netty.channel.Channel getSocket() {
        return socket;
    }

    @Override
    protected String describeSocket() {
        return socket + "";
    }

}
