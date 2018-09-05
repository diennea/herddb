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

import herddb.network.RequestWrapper;
import herddb.network.ResponseWrapper;
import herddb.proto.flatbuf.Message;
import herddb.proto.flatbuf.Request;
import herddb.proto.flatbuf.Response;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Decodes bytes to messages
 *
 * @author enrico.olivelli
 */
public class ProtocolMessageDecoder extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = Logger.getLogger(ProtocolMessageDecoder.class.getName());

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;

        if (LOGGER.isLoggable(Level.FINEST)) {
            StringBuilder dumper = new StringBuilder();
            ByteBufUtil.appendPrettyHexDump(dumper, in);
            LOGGER.log(Level.FINEST, "Received from {}: {}", new Object[]{ctx.channel(), dumper});
        }

        Message message = Message.getRootAsMessage(in.nioBuffer());
        Response response = message.response();
        if (response != null) {
            ctx.fireChannelRead(new ResponseWrapper(response, in));
            return;
        }
        Request request = message.request();
        if (request != null) {
            ctx.fireChannelRead(new RequestWrapper(request, in));
        }

    }
}
