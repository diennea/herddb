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
package herddb.codec;

import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.Table;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class RecordSerializerTest {

    public RecordSerializerTest() {
    }

    @Test
    public void testToBean() {
        Table table = Table.builder()
            .name("t1")
            .column("pk", ColumnTypes.STRING)
            .column("a", ColumnTypes.STRING)
            .column("b", ColumnTypes.LONG)
            .column("c", ColumnTypes.INTEGER)
            .column("d", ColumnTypes.TIMESTAMP)
            .column("e", ColumnTypes.BYTEARRAY)
            .primaryKey("pk")
            .build();
        Record record = RecordSerializer.makeRecord(table, "pk", "a",
            "a", "test", "b", 1L, "c", 2, "d", new java.sql.Timestamp(System.currentTimeMillis()), "e", "foo".getBytes(StandardCharsets.UTF_8));
        Map<String, Object> toBean = RecordSerializer.toBean(record, table);
    }

}
