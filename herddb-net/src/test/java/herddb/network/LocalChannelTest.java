package herddb.network;

import herddb.network.netty.NettyChannelAcceptor;
import herddb.network.netty.NettyServerLocator;
import java.util.Random;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LocalChannelTest {

    @Test
    public void test() throws Exception {
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor("localhost", 1111, true)) {
            acceptor.setEnableRealNetwork(false);
            acceptor.setAcceptor(new ServerSideConnectionAcceptor() {

                @Override
                public ServerSideConnection createConnection(Channel channel) {
                    channel.setMessagesReceiver(new ChannelEventListener() {

                        @Override
                        public void messageReceived(Message message, Channel channel) {
                            channel.sendReplyMessage(message, Message.ACK("ciao"));
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
            try (NettyServerLocator locator = new NettyServerLocator("localhost", 1111, true)) {
                try (Channel client = locator.connect(new ChannelEventListener() {

                    @Override
                    public void messageReceived(Message message, Channel channel) {
                        System.out.println("client messageReceived " + message);
                    }

                    @Override
                    public void channelClosed(Channel channel) {
                        System.out.println("client channelClosed");

                    }
                }, new ConnectionRequestInfo() {

                    @Override
                    public String getClientId() {
                        return "myclient";
                    }

                    @Override
                    public String getSharedSecret() {
                        return "secret";
                    }

                });) {
                    for (int i = 0; i < 100; i++) {
                        Message result = client.sendMessageWithReply(Message.ACK("clientId"), 10000);
//                        System.out.println("result:" + result);
                        assertEquals(Message.TYPE_ACK, result.type);
                    }
                }
            }
        }
    }
}
