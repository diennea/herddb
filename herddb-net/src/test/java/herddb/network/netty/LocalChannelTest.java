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

import static herddb.network.netty.Utils.buildAckRequest;
import static herddb.network.netty.Utils.buildAckResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ServerSideConnection;
import herddb.proto.Pdu;
import io.netty.buffer.ByteBuf;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

public class LocalChannelTest {

    @Test
    public void test() throws Exception {
        InetSocketAddress addr = new InetSocketAddress("localhost", 1111);
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor(addr.getHostName(), addr.getPort(), true)) {
            acceptor.setEnableRealNetwork(false);
            acceptor.setAcceptor((Channel channel) -> {
                channel.setMessagesReceiver(new ChannelEventListener() {
                    @Override
                    public void requestReceived(Pdu message, Channel channel) {
                        ByteBuf msg = buildAckResponse(message);
                        channel.sendReplyMessage(message.messageId, msg);
                        message.close();
                    }

                    @Override
                    public void channelClosed(Channel channel) {

                    }
                });
                return (ServerSideConnection) () -> new Random().nextLong();
            });
            acceptor.start();
            assertNotNull(LocalServerRegistry.getLocalServer(NetworkUtils.getAddress(addr), addr.getPort()));
            ExecutorService executor = Executors.newCachedThreadPool();
            try (Channel client = NettyConnector.connect(addr.getHostName(), addr.getPort(), true, 0, 0, new ChannelEventListener() {

                @Override
                public void channelClosed(Channel channel) {
                    System.out.println("client channelClosed");

                }
            }, executor, new NioEventLoopGroup(10, executor), new DefaultEventLoopGroup())) {
                for (int i = 0; i < 100; i++) {
                    ByteBuf buffer = buildAckRequest(i);
                    try (Pdu result = client.sendMessageWithPduReply(i, buffer, 10000)) {
                        assertEquals(Pdu.TYPE_ACK, result.type);
                    }
                }
            } finally {
                executor.shutdown();
            }

        }
        assertNull(LocalServerRegistry.getLocalServer(NetworkUtils.getAddress(addr), addr.getPort()));
    }
}
