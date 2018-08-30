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

import herddb.utils.MessageUtils;
import herddb.network.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Encodes messages to bytes
 *
 * @author enrico.olivelli
 */
public class DataMessageEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger LOG = Logger.getLogger(DataMessageEncoder.class.getName());

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        Message m = (Message) msg;
        ByteBuf encoded = ctx.alloc().buffer();
        MessageUtils.encodeMessage(encoded, m);
        ctx.writeAndFlush(encoded, promise);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.log(Level.SEVERE, "Error on encoder, channel " + ctx, cause);
    }

}
