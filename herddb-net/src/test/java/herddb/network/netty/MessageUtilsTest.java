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
import herddb.utils.DataAccessor;
import herddb.utils.MapDataAccessor;
import herddb.utils.RawString;
import herddb.utils.RecordsBatch;
import herddb.utils.TuplesList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author enrico.olivelli
 */
public class MessageUtilsTest {

    @Test
    public void testEncodeMessage() {

        System.out.println("encodeMessage");
        ByteBuf buffer = Unpooled.buffer();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("string", RawString.of("value"));
        payload.put("int", 1234);
        payload.put("long", 12345L);
        payload.put("rawstring", RawString.of("value"));
        payload.put("list", Arrays.asList(RawString.of("foo"), RawString.of("bar")));
        payload.put("set", new HashSet<>(Arrays.asList(
                RawString.of("foo"), RawString.of("bar"))));
        String[] colNames = {"one", "null", "two", "notfound"};
        List<DataAccessor> records = new ArrayList<>();

        Map<String, Object> record1 = new HashMap<>();
        record1.put("one", 1234);
        record1.put("two", RawString.of("test"));
        records.add(new MapDataAccessor(record1, colNames));

        Map<String, Object> record2 = new HashMap<>();
        record2.put("one", 2234);
        record2.put("two", RawString.of("test2"));
        record2.put("null", null);
        records.add(new MapDataAccessor(record2, colNames));

        // this must be the last
        TuplesList tl = new TuplesList(colNames, records);
        payload.put("data", tl);

        Message m = new Message(1234, payload);
        m.assignMessageId();
        m.setReplyMessageId(2343);
        MessageUtils.encodeMessage(buffer, m);

        Message read = MessageUtils.decodeMessage(buffer);
        assertEquals(read.messageId, m.messageId);
        assertEquals(read.replyMessageId, m.replyMessageId);
        assertEquals(read.type, m.type);
        assertEquals(read.parameters.size(), m.parameters.size());
        read.parameters.forEach((String k, Object v) -> {
            Object o = m.parameters.get(k);
            if (v instanceof RecordsBatch) {
                assertTrue(o instanceof TuplesList);
            } else {
                assertEquals(o, v);
            }
        });
        RecordsBatch tl2 = (RecordsBatch) read.parameters.get("data");
        assertTrue(tl2.hasNext());
        DataAccessor next = tl2.next();
        assertEquals(4, next.getValues().length);
        assertArrayEquals(colNames, next.getFieldNames());
        assertTrue(tl2.hasNext());
        next = tl2.next();
        assertEquals(4, next.getValues().length);
        assertArrayEquals(colNames, next.getFieldNames());
        assertFalse(tl2.hasNext());
        tl2.release();
    }

}
