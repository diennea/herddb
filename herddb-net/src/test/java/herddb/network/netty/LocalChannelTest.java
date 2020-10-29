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
import static herddb.utils.TestUtils.NOOP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ServerSideConnection;
import herddb.proto.Pdu;
import herddb.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class LocalChannelTest {

    @Test
    public void test() throws Exception {
        InetSocketAddress addr = new InetSocketAddress("localhost", NetworkUtils.assignFirstFreePort());
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
            }, executor, null)) {
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

    @Test
    public void testCloseServer() throws Exception {
        InetSocketAddress addr = new InetSocketAddress("localhost", NetworkUtils.assignFirstFreePort());
        try (NettyChannelAcceptor server = new NettyChannelAcceptor(addr.getHostName(), addr.getPort(), true)) {
            server.setEnableRealNetwork(false);
            server.setAcceptor((Channel channel) -> {
                channel.setMessagesReceiver(new ChannelEventListener() {
                });
                return (ServerSideConnection) () -> new Random().nextLong();
            });
            server.start();
            assertNotNull(LocalServerRegistry.getLocalServer(NetworkUtils.getAddress(addr), addr.getPort()));
            ExecutorService executor = Executors.newCachedThreadPool();

            AtomicBoolean closeNotificationReceived = new AtomicBoolean();
            try (Channel client = NettyConnector.connect(addr.getHostName(), addr.getPort(), true, 0, 0, new ChannelEventListener() {

                @Override
                public void channelClosed(Channel channel) {
                    System.out.println("client channelClosed");
                    closeNotificationReceived.set(true);

                }
            }, executor, null)) {
                //  closing the server should close the client
                server.close();
                assertTrue(client.isClosed());
                TestUtils.waitForCondition(() -> closeNotificationReceived.get(), NOOP, 100);
            } finally {
                executor.shutdown();
            }

        }
        assertNull(LocalServerRegistry.getLocalServer(NetworkUtils.getAddress(addr), addr.getPort()));
    }

    @Test
    public void testServerPushesData() throws Exception {
        InetSocketAddress addr = new InetSocketAddress("localhost", NetworkUtils.assignFirstFreePort());
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor(addr.getHostName(), addr.getPort(), true)) {
            acceptor.setEnableRealNetwork(false);
            acceptor.setAcceptor((Channel channel) -> {
                channel.setMessagesReceiver(new ChannelEventListener() {
                    @Override
                    public void requestReceived(Pdu message, Channel channel) {
                        try {
                            ByteBuf msg = buildAckResponse(message);

                            // send a message to the client
                            ByteBuf buffer = buildAckRequest(900);
                            Pdu response = channel.sendMessageWithPduReply(900, buffer, 10000);
                            assertEquals(Pdu.TYPE_ACK, response.type);

                            // send the response to the client
                            channel.sendReplyMessage(message.messageId, msg);
                            message.close();
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        } catch (TimeoutException ex) {
                            ex.printStackTrace();
                        }

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
            CopyOnWriteArrayList<Long> pushedMessagesFromServer = new CopyOnWriteArrayList<>();
            try (Channel client = NettyConnector.connect(addr.getHostName(), addr.getPort(), true, 0, 0, new ChannelEventListener() {
                @Override
                public void requestReceived(Pdu pdu, Channel channel) {
                    pushedMessagesFromServer.add(pdu.messageId);
                    assertTrue(pdu.isRequest());

                    ByteBuf msg = buildAckResponse(pdu);

                    // send the response to the server
                    channel.sendReplyMessage(pdu.messageId, msg);
                    pdu.close();
                }

                @Override
                public void channelClosed(Channel channel) {
                    System.out.println("client channelClosed");

                }
            }, executor, null)) {

                ByteBuf buffer = buildAckRequest(134);
                try (Pdu result = client.sendMessageWithPduReply(134, buffer, 10000)) {
                    assertEquals(Pdu.TYPE_ACK, result.type);
                }
                assertEquals(1, pushedMessagesFromServer.size());
            } finally {
                executor.shutdown();
            }

        }
        assertNull(LocalServerRegistry.getLocalServer(NetworkUtils.getAddress(addr), addr.getPort()));
    }
}
