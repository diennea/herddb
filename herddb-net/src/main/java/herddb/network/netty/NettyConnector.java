/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.network.netty;

import herddb.network.ChannelEventListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLException;

/**
 * Client-side connector
 *
 * @author enrico.olivelli
 */
public class NettyConnector {

    private static final Logger LOGGER = Logger.getLogger(NettyConnector.class.getName());

    public static herddb.network.Channel connect(
            String host, int port, boolean ssl, int connectTimeout, int socketTimeout,
            ChannelEventListener receiver, final ExecutorService callbackExecutor, final MultithreadEventLoopGroup networkGroup
    ) throws IOException {
        try {
            InetSocketAddress inet = new InetSocketAddress(host, port);
            String hostAddress = NetworkUtils.getAddress(inet);
            LocalVMChannelAcceptor localVm = LocalServerRegistry.getLocalServer(hostAddress, port);
            if (localVm != null && socketTimeout <= 0) {
                // if socketTimeout is greater than zero we cannot use our local transport implement
                // that timeout would need a timer
                // it is useful only to detect stuck network problems
                return localVm.connect(host + ":" + port, receiver, callbackExecutor);
            }
            return createNettyChannel(inet, host, port, ssl, connectTimeout, socketTimeout, receiver, callbackExecutor, networkGroup);
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        }
    }

    public static herddb.network.Channel connectUsingNetwork(String host, int port, boolean ssl, int connectTimeout, int socketTimeout,
            ChannelEventListener receiver, final ExecutorService callbackExecutor, final MultithreadEventLoopGroup networkGroup) throws IOException, SSLException {
        try {
            InetSocketAddress inet = new InetSocketAddress(host, port);
            return createNettyChannel(inet, host, port, ssl, connectTimeout, socketTimeout,
                    receiver, callbackExecutor, networkGroup);
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        }
    }

    private static herddb.network.Channel createNettyChannel(SocketAddress address, String host, int port, boolean ssl, int connectTimeout, int socketTimeout,
            ChannelEventListener receiver, final ExecutorService callbackExecutor, final MultithreadEventLoopGroup networkGroup) throws IOException, SSLException, InterruptedException {
        if (networkGroup == null) {
                throw new IOException("Connection using network is disabled, cannot connect to " + host + ":" + port);
        }
        Class<? extends Channel> channelType;
        MultithreadEventLoopGroup group = networkGroup;
        final SslContext sslCtx = !ssl ? null : SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        Bootstrap b = new Bootstrap();
        AtomicReference<NettyChannel> result = new AtomicReference<>();
        channelType = detectChannelType(networkGroup);
        b.group(group)
                .channel(channelType)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel ch) throws Exception {
                        try {
                            NettyChannel channel = new NettyChannel(host + ":" + port,
                                    ch, callbackExecutor);
                            result.set(channel);
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
                            ch.pipeline().addLast("messagedecoder", new ProtocolMessageDecoder());
                            ch.pipeline().addLast(new ClientInboundMessageHandler(channel));
                        } catch (Throwable t) {
                            LOGGER.log(Level.SEVERE, "error connecting", t);
                            ch.close();
                        }
                    }
                }
                );
        LOGGER.log(Level.FINE, "connecting to {0}:{1} ssl={2} address={3}", new Object[]{host, port, ssl, address
        }
        );
        b.connect(address).sync();
        NettyChannel nettyChannel = result.get();
        if (!nettyChannel.isValid()) {
            throw new IOException("returned channel is not valid");
        }
        return nettyChannel;
    }

    private static Class<? extends Channel> detectChannelType(final MultithreadEventLoopGroup networkGroup) {
        if (networkGroup.getClass().getName().contains("EpollEventLoopGroup")) {
            return EpollSocketChannel.class;
        }
        return NioSocketChannel.class;
    }

}
