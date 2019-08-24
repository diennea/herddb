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

import herddb.network.ServerSideConnectionAcceptor;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Accepts connections from workers
 *
 * @author enrico.olivelli
 */
public class NettyChannelAcceptor implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(NettyChannelAcceptor.class.getName());

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventLoopGroup localBossGroup;
    private EventLoopGroup localWorkerGroup;
    private int port = 7000;
    private String host = "localhost";
    private boolean ssl;
    private ServerSideConnectionAcceptor acceptor;
    private SslContext sslCtx;
    private List<String> sslCiphers;
    private File sslCertChainFile;
    private File sslCertFile;
    private String sslCertPassword;
    private int workerThreads = 16;
    private int callbackThreads = 64;
    private ExecutorService callbackExecutor;
    private BlockingQueue callbackExecutorQueue;
    private boolean enableRealNetwork = true;
    private boolean enableJVMNetwork = true;

    public boolean isEnableRealNetwork() {
        return enableRealNetwork;
    }

    public void setEnableRealNetwork(boolean enableRealNetwork) {
        this.enableRealNetwork = enableRealNetwork;
    }

    public boolean isEnableJVMNetwork() {
        return enableJVMNetwork;
    }

    public void setEnableJVMNetwork(boolean enableJVMNetwork) {
        this.enableJVMNetwork = enableJVMNetwork;
    }

    public int getCallbackThreads() {
        return callbackThreads;
    }

    public void setCallbackThreads(int callbackThreads) {
        this.callbackThreads = callbackThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public File getSslCertChainFile() {
        return sslCertChainFile;
    }

    public void setSslCertChainFile(File sslCertChainFile) {
        this.sslCertChainFile = sslCertChainFile;
    }

    public File getSslCertFile() {
        return sslCertFile;
    }

    public void setSslCertFile(File sslCertFile) {
        this.sslCertFile = sslCertFile;
    }

    public String getSslCertPassword() {
        return sslCertPassword;
    }

    public void setSslCertPassword(String sslCertPassword) {
        this.sslCertPassword = sslCertPassword;
    }

    public List<String> getSslCiphers() {
        return sslCiphers;
    }

    public void setSslCiphers(List<String> sslCiphers) {
        this.sslCiphers = sslCiphers;
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

    public void setHost(String host) {
        this.host = host;
    }

    private Channel channel;
    private Channel localChannel;
    private StatsLogger statsLogger;

    private static final ThreadFactory threadFactory = new ThreadFactory() {
        private final AtomicLong count = new AtomicLong();

        @Override
        public Thread newThread(Runnable r) {
            return new FastThreadLocalThread(r, "herddb-srvcall-" + count.incrementAndGet());
        }
    };

    public NettyChannelAcceptor(String host, int port, boolean ssl) {
        this(host, port, ssl, NullStatsLogger.INSTANCE);
    }

    public NettyChannelAcceptor(String host, int port, boolean ssl, StatsLogger statsLogger) {
        this.host = host;
        this.port = port;
        this.ssl = ssl;
        this.statsLogger = statsLogger;
    }

    public void start() throws Exception {
        if (ssl) {
            if (sslCertFile == null) {
                LOGGER.log(Level.SEVERE, "start SSL with self-signed auto-generated certificate");
                if (sslCiphers != null) {
                    LOGGER.log(Level.SEVERE, "required sslCiphers " + sslCiphers);
                }
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                try {
                    sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).ciphers(sslCiphers).build();
                } finally {
                    ssc.delete();
                }
            } else {
                LOGGER.log(Level.SEVERE, "start SSL with certificate " + sslCertFile.getAbsolutePath() + " chain file " + sslCertChainFile.getAbsolutePath());
                if (sslCiphers != null) {
                    LOGGER.log(Level.SEVERE, "required sslCiphers " + sslCiphers);
                }
                sslCtx = SslContextBuilder.forServer(sslCertChainFile, sslCertFile, sslCertPassword).ciphers(sslCiphers).build();
            }

        }

        if (callbackThreads == 0) {
            callbackExecutorQueue = new SynchronousQueue<Runnable>();
            callbackExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                    60L, TimeUnit.SECONDS,
                    callbackExecutorQueue,
                    threadFactory);
        } else {
            callbackExecutorQueue = new LinkedBlockingQueue<Runnable>();
            callbackExecutor = new ThreadPoolExecutor(callbackThreads, callbackThreads,
                    0L, TimeUnit.MILLISECONDS,
                    callbackExecutorQueue,
                    threadFactory);
        }
        statsLogger.registerGauge("callbacksqueue", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return callbackExecutorQueue.size();
            }

        });
        InetSocketAddress address = new InetSocketAddress(host, port);
        LOGGER.log(Level.SEVERE, "Starting HerdDB network server at {0}:{1}", new Object[]{host, port + ""});
        if (address.isUnresolved()) {
            throw new IOException("Bind address " + host + ":" + port + " cannot be resolved");
        }
        ChannelInitializer<io.netty.channel.Channel> channelInitialized = new ChannelInitializer<io.netty.channel.Channel>() {
            @Override
            public void initChannel(io.netty.channel.Channel ch) throws Exception {
                NettyChannel session = new NettyChannel("unnamed", ch, callbackExecutor);
                if (acceptor != null) {
                    acceptor.createConnection(session);
                }

//                        ch.pipeline().addLast(new LoggingHandler());
                // Add SSL handler first to encrypt and decrypt everything.
                if (ssl) {
                    ch.pipeline().addLast(sslCtx.newHandler(ch.alloc()));
                }

                ch.pipeline().addLast("lengthprepender", new LengthFieldPrepender(4));
                ch.pipeline().addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
//
                ch.pipeline().addLast("messagedecoder", new ProtocolMessageDecoder());
                ch.pipeline().addLast(new ServerInboundMessageHandler(session));
            }
        };
        if (enableRealNetwork) {
            if (NetworkUtils.isEnableEpoolNative()) {
                bossGroup = new EpollEventLoopGroup(workerThreads);
                workerGroup = new EpollEventLoopGroup(workerThreads);
                LOGGER.log(Level.FINE, "Using netty-native-epoll network type");
            } else {
                bossGroup = new NioEventLoopGroup(workerThreads);
                workerGroup = new NioEventLoopGroup(workerThreads);
                LOGGER.log(Level.FINE, "Using nio network type");
            }

            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NetworkUtils.isEnableEpoolNative() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                    .childHandler(channelInitialized)
                    .option(ChannelOption.SO_BACKLOG, 128);
            ChannelFuture f = b.bind(address).sync();
            this.channel = f.channel();

        }

        if (enableJVMNetwork) {
            localBossGroup = new DefaultEventLoopGroup(workerThreads);
            localWorkerGroup = new DefaultEventLoopGroup(workerThreads);
            ServerBootstrap b_local = new ServerBootstrap();
            b_local.group(localBossGroup, localWorkerGroup)
                    .channel(LocalServerChannel.class)
                    .childHandler(channelInitialized);

            String hostAddress = NetworkUtils.getAddress(address);
            LocalServerRegistry.registerLocalServer(hostAddress, port, ssl);

            ChannelFuture local_f = b_local.bind(new LocalAddress(hostAddress + ":" + port + ":" + ssl)).sync();
            this.localChannel = local_f.channel();
        }

    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close();
        }
        if (localChannel != null) {
            localChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (localWorkerGroup != null) {
            localWorkerGroup.shutdownGracefully();
        }
        if (localBossGroup != null) {
            localBossGroup.shutdownGracefully();
        }
        if (callbackExecutor != null) {
            callbackExecutor.shutdown();
        }
    }

    public ServerSideConnectionAcceptor getAcceptor() {
        return acceptor;
    }

    public void setAcceptor(ServerSideConnectionAcceptor acceptor) {
        this.acceptor = acceptor;
    }

}
