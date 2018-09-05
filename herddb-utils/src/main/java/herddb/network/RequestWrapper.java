/*
 * Copyright 2018 enrico.olivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.network;

import herddb.proto.flatbuf.Request;
import io.netty.buffer.ByteBuf;

/**
 * Wraps a Request and the underlying ByteBuf
 */
public class RequestWrapper implements AutoCloseable {

    public Request request;
    private ByteBuf buffer;

    public RequestWrapper(Request request, ByteBuf buffer) {
        this.request = request;
        this.buffer = buffer;
    }

    public void release() {
        request = null;
        buffer.release();
        buffer = null;
    }

    @Override
    public void close() {
        release();
    }
}
