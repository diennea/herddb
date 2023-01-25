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
import herddb.network.ChannelEventListener;
import herddb.network.SendResultCallback;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Short circuit implementation of {@link herddb.network.Channel} for in-VM
 * communications.
 *
 * @author eolivelli
 */
public class LocalVMChannel extends AbstractChannel implements Comparable<LocalVMChannel> {

    private final ServerSideLocalVMChannel serverSideChannel;
    private final LocalVMChannelAcceptor parent;

    LocalVMChannel(String name, ChannelEventListener clientSidePeer, ExecutorService executorService, LocalVMChannelAcceptor parent) {
        super(name, ADDRESS_JVM_LOCAL, executorService);
        setMessagesReceiver(clientSidePeer);
        serverSideChannel = new ServerSideLocalVMChannel(ADDRESS_JVM_LOCAL, ADDRESS_JVM_LOCAL, executorService);
        this.parent = parent;
    }

    public Channel getServerSideChannel() {
        return serverSideChannel;
    }

    @Override
    public void sendOneWayMessage(ByteBuf message, SendResultCallback callback) {
        if (isClosed()) {
            ReferenceCountUtil.safeRelease(message);
            callback.messageSent(new IOException("channel closed"));
            return;
        }
        try {
            Pdu pdu = PduCodec.decodePdu(message);
            // execute server side code in this thread
            serverSideChannel.directProcessPdu(pdu);

        } catch (IOException ex) {
            ReferenceCountUtil.safeRelease(message);
            callback.messageSent(ex);
        }
    }

    @Override
    protected String describeSocket() {
        return "jvm-local";
    }

    @Override
    protected void doClose() {
        parent.deregister(this);
        // emulate Netty on channel close
        channelClosed();
        serverSideChannel.close();
    }

    @Override
    public boolean isValid() {
        return !ioErrors;
    }

    @Override
    public boolean isLocalChannel() {
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 19 * hash + Objects.hashCode(this.getId());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LocalVMChannel other = (LocalVMChannel) obj;
        if (!Objects.equals(this.getId(), other.getId())) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(LocalVMChannel o) {
        return Long.compare(this.getId(), o.getId());
    }

    private class ServerSideLocalVMChannel extends AbstractChannel {

        public ServerSideLocalVMChannel(String name, String remoteAddress, ExecutorService executor) {
            super(name, remoteAddress, executor);
        }

        @Override
        public void sendOneWayMessage(ByteBuf message, SendResultCallback callback) {
            if (isClosed()) {
                ReferenceCountUtil.safeRelease(message);
                callback.messageSent(new IOException("channel closed"));
                return;
            }
            try {
                Pdu pdu = PduCodec.decodePdu(message);
                LocalVMChannel.this.directProcessPdu(pdu);
            } catch (IOException ex) {
                ReferenceCountUtil.safeRelease(message);
                callback.messageSent(ex);
            }
        }

        @Override
        protected String describeSocket() {
            return LocalVMChannel.this.describeSocket();
        }

        @Override
        protected void doClose() {
            // emulate Netty on channel close
            channelClosed();
            LocalVMChannel.this.close();
        }

        @Override
        public boolean isValid() {
            return LocalVMChannel.this.isValid();
        }

        @Override
        public boolean isLocalChannel() {
            return true;
        }

        @Override
        public String toString() {
            return "server-localvm-channel";
        }

    }

}
