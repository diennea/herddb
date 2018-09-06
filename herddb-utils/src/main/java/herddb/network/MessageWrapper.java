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
package herddb.network;

import herddb.proto.flatbuf.Message;
import herddb.proto.flatbuf.Request;
import herddb.proto.flatbuf.Response;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;

/**
 * Wraps a Messsage.
 * <p>
 * This is a recyclable class, we are going to reclyce: - the object itseld -
 * the Message/Request/Response Flatbuffer objects.
 * <p>
 * It also manages the reference to the original ByteBuf, in order to handle
 * correcly refcounts.
 *
 * @author enrico.olivelli
 */
public final class MessageWrapper implements AutoCloseable {

    private ByteBuf buffer;
    private final Message message = new Message();
    private final Request request = new Request();
    private final Response response = new Response();
    private boolean responsePresent;
    private boolean requestPresent;
    private final Recycler.Handle<MessageWrapper> recyclerHandle;

    MessageWrapper(Recycler.Handle<MessageWrapper> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public Request getRequest() {
        if (requestPresent) {
            return request;
        }
        // This will return "null" if message does not really have a "request" field
        Request res = message.request(request);
        requestPresent = res != null;
        return res;
    }

    public Response getResponse() {
        if (responsePresent) {
            return response;
        }
        // This will return "null" if message does not really have a "response" field
        Response res = message.response(response);
        responsePresent = res != null;
        return res;
    }

    public static MessageWrapper newMessageWrapper(ByteBuf in) {
        MessageWrapper pooled = RECYCLER.get();
        Message.getRootAsMessage(in.nioBuffer(), pooled.message);
        pooled.buffer = in;

        // TO BE lazily initialized
        pooled.request.__init(-1, null);
        pooled.response.__init(-1, null);
        pooled.responsePresent = false;
        pooled.requestPresent = false;
        return pooled;
    }

    private static final Recycler<MessageWrapper> RECYCLER = new Recycler<MessageWrapper>() {
        @Override
        protected MessageWrapper newObject(Recycler.Handle<MessageWrapper> handle) {
            return new MessageWrapper(handle);
        }
    };

    @Override
    public void close() {
        buffer.release();
        message.__init(-1, null);
        request.__init(-1, null);
        response.__init(-1, null);
        buffer = null;
        responsePresent = false;
        requestPresent = false;
        recyclerHandle.recycle(this);
    }
}
