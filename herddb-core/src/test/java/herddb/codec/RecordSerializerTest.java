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
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import static org.junit.Assert.assertEquals;
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

    @Test
    public void testConvert() {
        testTimestamp("2015-03-29 01:00:00", "UTC", 1427590800000L);
        testTimestamp("2015-03-29 02:00:00", "UTC", 1427594400000L);
        testTimestamp("2015-03-29 03:00:00", "UTC", 1427598000000L);

    }

    private static void testTimestamp(String testCase, String timezone, long expectedResult) throws StatementExecutionException {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ZZZ");
        fmt.setTimeZone(TimeZone.getTimeZone(timezone));
        java.sql.Timestamp result = (java.sql.Timestamp) RecordSerializer.convert(ColumnTypes.TIMESTAMP, testCase);
        String formattedResult = fmt.format(result);
        System.out.println("result:" + result.getTime());
        System.out.println("test case " + testCase + ", result:" + formattedResult);
        long delta = (expectedResult - result.getTime()) / (1000 * 60 * 60);
        assertEquals("failed for " + testCase + " delta is " + delta + " h, result is " + formattedResult, expectedResult, result.getTime());
    }

}
