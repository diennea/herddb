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

import com.google.flatbuffers.FlatBufferBuilder;
import herddb.proto.flatbuf.Message;
import herddb.proto.flatbuf.MessageType;
import herddb.proto.flatbuf.Request;
import herddb.proto.flatbuf.Response;
import java.nio.ByteBuffer;

public class Utils {

    static ByteBuffer buildAckResponse(Request message) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        Response.startResponse(builder);
        Response.addType(builder, MessageType.TYPE_ACK);
        Response.addReplyMessageId(builder, message.id());
        int pos = Response.endResponse(builder);
        Message.startMessage(builder);
        Message.addResponse(builder, pos);
        int endMsg = Message.endMessage(builder);
        Message.finishMessageBuffer(builder, endMsg);
        ByteBuffer dataBuffer = builder.dataBuffer();
        return dataBuffer;
    }

    static ByteBuffer buildAckRequest(int i) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        Request.startRequest(builder);
        Request.addId(builder, i);
        Request.addType(builder, MessageType.TYPE_ACK);
        int pos = Request.endRequest(builder);
        Message.startMessage(builder);
        Message.addRequest(builder, pos);
        int endMsg = Message.endMessage(builder);
        Message.finishMessageBuffer(builder, endMsg);
        ByteBuffer buffer = builder.dataBuffer();
        return buffer;
    }
}
