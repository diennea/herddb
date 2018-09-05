package herddb.network;

import static herddb.network.Utils.buildAckRequest;
import static herddb.network.Utils.buildAckResponse;
import herddb.network.netty.NettyChannelAcceptor;
import herddb.network.netty.NettyConnector;
import herddb.proto.flatbuf.MessageType;
import herddb.proto.flatbuf.Request;
import herddb.proto.flatbuf.Response;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ChannelBenchTest {

    @Test
    public void test() throws Exception {
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor("localhost", 1111, true)) {
            acceptor.setAcceptor((ServerSideConnectionAcceptor) (Channel channel) -> {
                channel.setMessagesReceiver(new ChannelEventListener() {
                    @Override
                    public void requestReceived(RequestWrapper message, Channel channel) {
                        ByteBuffer msg = buildAckResponse(message.request);
                        channel.sendReplyMessage(message.request.id(), Unpooled.wrappedBuffer(msg));
                        message.release();
                    }

                    @Override
                    public void channelClosed(Channel channel) {

                    }
                });
                return (ServerSideConnection) () -> new Random().nextLong();
            });
            acceptor.start();
            ExecutorService executor = Executors.newCachedThreadPool();
            try (Channel client = NettyConnector.connect("localhost", 1111, true, 0, 0, new ChannelEventListener() {

                @Override
                public void channelClosed(Channel channel) {
                    System.out.println("client channelClosed");

                }
            }, executor, new NioEventLoopGroup(10, executor), new DefaultEventLoopGroup())) {
                for (int i = 0; i < 100; i++) {
                    ByteBuffer buffer = buildAckRequest(i);
                    ResponseWrapper result = client.sendMessageWithReply(i, Unpooled.wrappedBuffer(buffer), 10000);
                    assertEquals(MessageType.TYPE_ACK, result.response.type());
                    result.release();
                }
            } finally {
                executor.shutdown();
            }
        }

    }

}
