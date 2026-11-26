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

import static herddb.utils.TestUtils.NOOP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.SendResultCallback;
import herddb.network.ServerSideConnection;
import herddb.proto.Pdu;
import herddb.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A channel owns every message handed to it, so it has to release the messages it cannot send. The buffers come from a
 * pool of direct buffers, and a message that is dropped without being released never returns to that pool.
 *
 * <p>
 * The tests use unpooled buffers, whose reference count can be read back safely after the release: a pooled buffer is
 * recycled the moment it is released and its reference count then belongs to whoever allocated it next.
 * </p>
 */
public class ChannelMessageReleaseTest {

    private ExecutorService callbackExecutor;
    private TestChannel channel;

    @Before
    public void setUp() {
        callbackExecutor = Executors.newSingleThreadExecutor();
        channel = new TestChannel(callbackExecutor);
    }

    @After
    public void tearDown() {
        channel.close();
        callbackExecutor.shutdown();
    }

    @Test
    public void replyToADeadChannelIsReleased() {
        channel.valid = false;

        ByteBuf message = Unpooled.buffer(16).writeInt(1);
        channel.sendReplyMessage(1, message);

        assertEquals(0, message.refCnt());
        assertEquals(0, channel.sentMessages.get());
    }

    @Test
    public void replyToALiveChannelIsReleasedOnlyByTheTransport() {
        ByteBuf message = Unpooled.buffer(16).writeInt(1);
        channel.sendReplyMessage(1, message);

        assertEquals(1, channel.sentMessages.get());
        assertEquals("the message must reach the transport alive", 1, channel.refCntOnSend.get());
        assertEquals(0, message.refCnt());
    }

    @Test
    public void requestToADeadChannelIsReleased() {
        channel.valid = false;

        ByteBuf message = Unpooled.buffer(16).writeInt(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        channel.sendRequestWithAsyncReply(1, message, 10000, (reply, error) -> failure.set(error));

        assertEquals(0, message.refCnt());
        assertEquals(0, channel.sentMessages.get());
        assertNotNull("the caller must be told that no answer is coming", failure.get());
        assertEquals("a request that was never sent must leave nothing waiting for an answer", 0, channel.pendingCallbacks());
    }

    @Test
    public void requestToALiveChannelIsReleasedOnlyByTheTransport() {
        ByteBuf message = Unpooled.buffer(16).writeInt(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        channel.sendRequestWithAsyncReply(1, message, 10000, (reply, error) -> failure.set(error));

        assertEquals(1, channel.sentMessages.get());
        assertEquals("the message must reach the transport alive", 1, channel.refCntOnSend.get());
        assertEquals(0, message.refCnt());
        assertNull(failure.get());
        assertEquals("the request is waiting for its answer", 1, channel.pendingCallbacks());
    }

    @Test
    public void nettyChannelReleasesTheMessagesItCannotSend() throws Exception {
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor("localhost", NetworkUtils.assignFirstFreePort(), false)) {
            acceptor.setEnableJVMNetwork(false);
            acceptor.setAcceptor(ChannelMessageReleaseTest::answerWithAck);
            acceptor.start();

            ExecutorService executor = Executors.newCachedThreadPool();
            NioEventLoopGroup networkGroup = new NioEventLoopGroup(2, executor);
            try {
                Channel client = NettyConnector.connect(acceptor.getHost(), acceptor.getPort(), acceptor.isSsl(), 0, 0,
                        new ChannelEventListener() {
                        }, executor, networkGroup);
                assertTrue(client instanceof NettyChannel);

                ByteBuf request = unpooledCopyOf(Utils.buildAckRequest(7));
                try (Pdu result = client.sendMessageWithPduReply(7, request, 10000)) {
                    assertEquals(Pdu.TYPE_ACK, result.type);
                }
                TestUtils.waitForCondition(() -> request.refCnt() == 0, NOOP, 10);

                client.close();

                ByteBuf oneWay = Unpooled.buffer(16).writeInt(1);
                AtomicReference<Throwable> oneWayFailure = new AtomicReference<>();
                client.sendOneWayMessage(oneWay, oneWayFailure::set);
                assertEquals(0, oneWay.refCnt());
                assertNotNull(oneWayFailure.get());

                ByteBuf reply = Unpooled.buffer(16).writeInt(1);
                client.sendReplyMessage(1, reply);
                assertEquals(0, reply.refCnt());

                ByteBuf pending = Unpooled.buffer(16).writeInt(1);
                AtomicReference<Throwable> pendingFailure = new AtomicReference<>();
                client.sendRequestWithAsyncReply(2, pending, 10000, (answer, error) -> pendingFailure.set(error));
                assertEquals(0, pending.refCnt());
                assertNotNull(pendingFailure.get());
            } finally {
                networkGroup.shutdownGracefully();
                executor.shutdown();
            }
        }
    }

    @Test
    public void localChannelReleasesTheMessagesItCannotSend() throws Exception {
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor("localhost", NetworkUtils.assignFirstFreePort(), false)) {
            acceptor.setEnableRealNetwork(false);
            acceptor.setAcceptor(ChannelMessageReleaseTest::answerWithAck);
            acceptor.start();

            ExecutorService executor = Executors.newCachedThreadPool();
            try {
                Channel client = NettyConnector.connect(acceptor.getHost(), acceptor.getPort(), acceptor.isSsl(), 0, 0,
                        new ChannelEventListener() {
                        }, executor, null);
                assertTrue(client instanceof LocalVMChannel);
                client.close();

                ByteBuf oneWay = Unpooled.buffer(16).writeInt(1);
                AtomicReference<Throwable> oneWayFailure = new AtomicReference<>();
                client.sendOneWayMessage(oneWay, oneWayFailure::set);
                assertEquals(0, oneWay.refCnt());
                assertNotNull(oneWayFailure.get());

                // a closed local channel still reports itself as valid, so the reply travels down to the transport,
                // which is the one that drops it
                ByteBuf reply = Unpooled.buffer(16).writeInt(1);
                client.sendReplyMessage(1, reply);
                assertEquals(0, reply.refCnt());
            } finally {
                executor.shutdown();
            }
        }
    }

    private static ServerSideConnection answerWithAck(Channel channel) {
        channel.setMessagesReceiver(new ChannelEventListener() {
            @Override
            public void requestReceived(Pdu message, Channel channel) {
                channel.sendReplyMessage(message.messageId, Utils.buildAckResponse(message));
                message.close();
            }

            @Override
            public void channelClosed(Channel channel) {
            }
        });
        return () -> new Random().nextLong();
    }

    /**
     * Copies a message built by the codec, which allocates from the pool, into a buffer that nobody recycles, so that
     * its reference count can still be read after it has been released.
     */
    private static ByteBuf unpooledCopyOf(ByteBuf pooled) {
        try {
            return Unpooled.copiedBuffer(pooled);
        } finally {
            pooled.release();
        }
    }

    /**
     * A channel that hands no message to any network, so that the branches that give up on sending can be reached
     * without one.
     */
    private static final class TestChannel extends AbstractChannel {

        private volatile boolean valid = true;
        private final AtomicInteger sentMessages = new AtomicInteger();
        private final AtomicInteger refCntOnSend = new AtomicInteger(-1);

        TestChannel(ExecutorService callbackExecutor) {
            super("test-channel", "test-address", callbackExecutor);
        }

        @Override
        public void sendOneWayMessage(ByteBuf message, SendResultCallback callback) {
            sentMessages.incrementAndGet();
            refCntOnSend.set(message.refCnt());
            // this is what a real transport does: it takes the message over and releases it once it has been written
            message.release();
            callback.messageSent(null);
        }

        @Override
        public boolean isValid() {
            return valid;
        }

        @Override
        public boolean isLocalChannel() {
            return false;
        }

        @Override
        protected String describeSocket() {
            return "test-socket";
        }

        @Override
        protected void doClose() {
        }
    }
}
