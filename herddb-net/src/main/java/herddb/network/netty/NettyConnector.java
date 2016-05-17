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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker-side connector
 *
 * @author enrico.olivelli
 */
public class NettyConnector implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(NettyConnector.class.getName());

    private int port = 7000;
    private String host = "localhost";
    private NettyChannel channel;
    private Channel socketchannel;
    private EventLoopGroup group;
    private SslContext sslCtx;
    private boolean ssl;
    protected int connectTimeout = 60000;
    protected int socketTimeout = 240000;
    private final ExecutorService callbackExecutor = Executors.newCachedThreadPool();

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    private ChannelEventListener receiver;

    public NettyConnector(ChannelEventListener receiver) {
        this.receiver = receiver;
    }

    public NettyChannel connect() throws Exception {
        if (ssl) {
            this.sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        }
        group = new NioEventLoopGroup();

        Class<? extends Channel> channelType;

        InetSocketAddress inet = new InetSocketAddress(host, port);
        SocketAddress address;
        String _host = inet.getAddress().getHostAddress();
        
        if (LocalServerRegistry.isLocalServer(_host, port, ssl)) {
            channelType = LocalChannel.class;
            address = new LocalAddress(_host + ":" + port + ":" + ssl);
            LOGGER.log(Level.SEVERE, "connecting to local in-JVM server");
        } else {
            channelType = NioSocketChannel.class;
            address = inet;
            LOGGER.log(Level.SEVERE, "connecting to remote server " + address);
        }

        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(channelType)                
                .option(ChannelOption.SO_TIMEOUT, socketTimeout)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel ch) throws Exception {
                        channel = new NettyChannel(host + ":" + port, ch, callbackExecutor, NettyConnector.this);
                        channel.setMessagesReceiver(receiver);
                        if (ssl) {
                            ch.pipeline().addLast(sslCtx.newHandler(ch.alloc(), host, port));
                        }
                        ch.pipeline().addLast("lengthprepender", new LengthFieldPrepender(4));
                        ch.pipeline().addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
//
                        ch.pipeline().addLast("messageencoder", new DataMessageEncoder());
                        ch.pipeline().addLast("messagedecoder", new DataMessageDecoder());
                        ch.pipeline().addLast(new InboundMessageHandler(channel));
                    }
                });

        LOGGER.log(Level.SEVERE, "connecting to {0}:{1} ssl={2} address={3}", new Object[]{host, port, ssl, address});
        ChannelFuture f = b.connect(address).sync();
        socketchannel = f.channel();
        return channel;

    }

    public NettyChannel getChannel() {
        return channel;
    }

    @Override
    public void close() {
        LOGGER.log(Level.SEVERE, "close channel {0}", channel);
        if (socketchannel != null) {
            try {
                socketchannel.close().await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                socketchannel = null;
            }
        }
        if (group != null) {
            try {
                group.shutdownGracefully();
            } finally {
                group = null;
            }
        }
        if (callbackExecutor != null) {
            callbackExecutor.shutdown();
        }
    }

    public void setHost(String host) {
        this.host = host;
    }

}
