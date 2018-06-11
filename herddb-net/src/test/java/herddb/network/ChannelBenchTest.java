package herddb.network;

import herddb.network.netty.NettyChannelAcceptor;
import herddb.network.netty.NettyConnector;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ChannelBenchTest {

    @Test
    public void test() throws Exception {
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor("localhost", 1111, true)) {
            acceptor.setAcceptor(new ServerSideConnectionAcceptor() {

                @Override
                public ServerSideConnection createConnection(Channel channel) {
                    channel.setMessagesReceiver(new ChannelEventListener() {

                        @Override
                        public void messageReceived(Message message, Channel channel) {
                            channel.sendReplyMessage(message, Message.ACK());
                        }

                        @Override
                        public void channelClosed(Channel channel) {

                        }
                    });
                    return new ServerSideConnection() {
                        @Override
                        public long getConnectionId() {
                            return new Random().nextLong();
                        }
                    };
                }
            });
            acceptor.start();
            ExecutorService executor = Executors.newCachedThreadPool();
            try (Channel client = NettyConnector.connect("localhost", 1111, true, 0, 0, new ChannelEventListener() {

                @Override
                public void messageReceived(Message message, Channel channel) {
                    System.out.println("client messageReceived " + message);
                }

                @Override
                public void channelClosed(Channel channel) {
                    System.out.println("client channelClosed");

                }
            }, executor, new NioEventLoopGroup(10, executor), new DefaultEventLoopGroup())) {
                for (int i = 0; i < 100; i++) {
                    Message result = client.sendMessageWithReply(Message.ACK(), 10000);
//                        System.out.println("result:" + result);
                    assertEquals(Message.TYPE_ACK, result.type);
                }
            } finally {
                executor.shutdown();
            }
        }

    }
}
