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

import herddb.network.MessageWrapper;
import herddb.proto.Pdu;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles messages
 *
 * @author enrico.olivelli
 */
public class ServerInboundMessageHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = Logger.getLogger(ServerInboundMessageHandler.class.getName());

    private final NettyChannel session;

    public ServerInboundMessageHandler(NettyChannel session) {
        this.session = session;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.log(Level.SEVERE, "error on channel " + ctx, cause);
        ctx.close();
        session.exceptionCaught(cause);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        session.channelClosed();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Pdu) {
            session.pduReceived((Pdu) msg);
            return;
        }
        MessageWrapper message = (MessageWrapper) msg;
        if (message.getResponse() != null) {
            session.responseReceived(message);
        } else {
            session.requestReceived(message);
        }
    }

}
