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

import herddb.network.ChannelEventListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.ws.Holder;

/**
 * Worker-side connector
 *
 * @author enrico.olivelli
 */
public class NettyConnector {

    private static final Logger LOGGER = Logger.getLogger(NettyConnector.class.getName());

    public static NettyChannel connect(String host, int port, boolean ssl, int connectTimeout, int socketTimeout,
        ChannelEventListener receiver, final ExecutorService callbackExecutor, final MultithreadEventLoopGroup networkGroup, final DefaultEventLoopGroup localEventsGroup) throws IOException {
        try {
            final SslContext sslCtx = !ssl ? null : SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

            Class<? extends Channel> channelType;

            InetSocketAddress inet = new InetSocketAddress(host, port);
            SocketAddress address;
            String hostAddress = NetworkUtils.getAddress(inet);

            MultithreadEventLoopGroup group;
            if (LocalServerRegistry.isLocalServer(hostAddress, port, ssl)) {
                channelType = LocalChannel.class;
                address = new LocalAddress(hostAddress + ":" + port + ":" + ssl);
                group = localEventsGroup;
            } else {
                channelType = networkGroup instanceof EpollEventLoopGroup ? EpollSocketChannel.class : NioSocketChannel.class;
                address = inet;
                group = networkGroup;
            }
            Bootstrap b = new Bootstrap();
            Holder<NettyChannel> result = new Holder<>();

            b.group(group)
                .channel(channelType)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel ch) throws Exception {
                        NettyChannel channel = new NettyChannel(host + ":" + port, ch, callbackExecutor);
                        result.value = channel;
                        channel.setMessagesReceiver(receiver);
                        if (ssl) {
                            ch.pipeline().addLast(sslCtx.newHandler(ch.alloc(), host, port));
                        }
                        if (socketTimeout > 0) {
                            ch.pipeline().addLast("readTimeoutHandler", new ReadTimeoutHandler(socketTimeout));
                        }
                        ch.pipeline().addLast("lengthprepender", new LengthFieldPrepender(4));
                        ch.pipeline().addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
//
                        ch.pipeline().addLast("messageencoder", new DataMessageEncoder());
                        ch.pipeline().addLast("messagedecoder", new DataMessageDecoder());
                        ch.pipeline().addLast(new InboundMessageHandler(channel));
                    }
                }
                );

            LOGGER.log(Level.FINE, "connecting to {0}:{1} ssl={2} address={3}", new Object[]{host, port, ssl, address
            }
            );
            b.connect(address).sync();
            return result.value;
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        }

    }

}
