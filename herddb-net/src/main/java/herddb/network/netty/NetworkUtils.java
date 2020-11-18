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

import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Network utility
 *
 * @author enrico.olivelli
 */
public class NetworkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class.getName());

    // computed lazily, in order not to force loading of Netty EPoll if not needed
    private static Boolean nettyEpoolNativeAvailable;

    public static synchronized boolean isEnableEpoolNative() {
        if (nettyEpoolNativeAvailable == null) {
            nettyEpoolNativeAvailable =
                    System.getProperty("os.name").equalsIgnoreCase("linux")
                    && !Boolean.getBoolean("herddb.network.disablenativeepoll")
                    && Epoll.isAvailable();
            if (!nettyEpoolNativeAvailable && !Epoll.isAvailable()) {
                LOG.info("Netty Epoll is not enabled, os.name {}, Epoll.isAvailable(): {} cause: {}",
                        System.getProperty("os.name"), Epoll.isAvailable(), Epoll.unavailabilityCause());
            }
        }
        return nettyEpoolNativeAvailable;
    }

    public static String getAddress(InetSocketAddress address) {
        if (address.getAddress() != null) {
            return address.getAddress().getHostAddress();
        } else {
            return address.getHostName();
        }
    }

    public static int assignFirstFreePort() throws IOException {
        try (
                ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();

        }
    }

    public static String getLocalNetworkAddress() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
