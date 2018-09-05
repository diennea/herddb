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
package herddb.proto.flatbuf;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class SerializerTest {

    @Test
    public void test() {

        ByteBuffer bb = ByteBuffer.allocate(1024);
        FlatBufferBuilder builder = new FlatBufferBuilder(bb);

        int dataOffset = builder.createByteVector("foo".getBytes(StandardCharsets.UTF_8));

        Response.startResponse(builder);
        Response.addType(builder, MessageType.TYPE_ACK);
        Response.addReplyMessageId(builder, 10);
        Response.addData(builder, dataOffset);
        int endMessage = Response.endResponse(builder);
        Response.finishResponseBuffer(builder, endMessage);

        Response decoded = Response.getRootAsResponse(bb);
        assertEquals(10, decoded.replyMessageId());
        assertEquals(MessageType.TYPE_ACK, decoded.type());
        ByteBuffer data = decoded.dataAsByteBuffer();
        byte[] arr = new byte[data.remaining()];
        data.get(arr);
        assertEquals("foo", new String(arr, StandardCharsets.UTF_8));

    }
}
