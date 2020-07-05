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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.utils.Bytes;
import herddb.utils.RawString;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;
import org.junit.Test;

/**
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

    @Test
    public void testSerializeWithNullAndNonNullTypes() {
        byte[] iBytes = RecordSerializer.serialize(new Integer(10), ColumnTypes.INTEGER);
        byte[] iBytesNonNullType = RecordSerializer.serialize(new Integer(10), ColumnTypes.NOTNULL_INTEGER);
        assertArrayEquals(iBytes, iBytesNonNullType);

        byte[] lBytes = RecordSerializer.serialize(new Long(1982), ColumnTypes.NOTNULL_LONG);
        byte[] lBytesNonNullType = RecordSerializer.serialize(new Long(1982), ColumnTypes.LONG);
        assertArrayEquals(lBytes, lBytesNonNullType);

        byte[] sBytes = RecordSerializer.serialize("test", ColumnTypes.STRING);
        byte[] sBytesNonNullType = RecordSerializer.serialize("test", ColumnTypes.NOTNULL_STRING);
        assertArrayEquals(sBytes, sBytesNonNullType);

        byte[] dBytes = RecordSerializer.serialize(10.01d, ColumnTypes.DOUBLE);
        byte[] dBytesNonNullType = RecordSerializer.serialize(10.01d, ColumnTypes.NOTNULL_DOUBLE);
        assertArrayEquals(dBytes, dBytesNonNullType);

        byte[] bBytes = RecordSerializer.serialize(Boolean.TRUE, ColumnTypes.BOOLEAN);
        byte[] bBytesNonNullType = RecordSerializer.serialize(Boolean.TRUE, ColumnTypes.NOTNULL_BOOLEAN);
        assertArrayEquals(bBytes, bBytesNonNullType);

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        byte[] tBytes = RecordSerializer.serialize(timestamp, ColumnTypes.TIMESTAMP);
        byte[] tBytesNonNullType = RecordSerializer.serialize(timestamp, ColumnTypes.NOTNULL_TIMESTAMP);
        assertArrayEquals(tBytes, tBytesNonNullType);
    }

    @Test
    public void testSerializeThrowsExceptionOnNullObject() {
        assertNull(RecordSerializer.serialize(null, ColumnTypes.STRING));
    }

    @Test
    public void testDeserializeWithNullAndNonNullTypes() {
        byte[] byteValueForInt = Bytes.from_int(1000).to_array();
        int iValue = (int) RecordSerializer.deserialize(byteValueForInt, ColumnTypes.INTEGER);
        assertEquals(iValue, 1000);

        int iValueNonNullType = (int) RecordSerializer.deserialize(byteValueForInt, ColumnTypes.NOTNULL_INTEGER);
        assertEquals(iValueNonNullType, 1000);


        byte[] byteValueForLong = Bytes.from_long(99999).to_array();
        long lValue = (long) RecordSerializer.deserialize(byteValueForLong, ColumnTypes.LONG);
        assertEquals(lValue, 99999);

        long lValueNonNullType = (long) RecordSerializer.deserialize(byteValueForLong, ColumnTypes.NOTNULL_LONG);
        assertEquals(lValueNonNullType, 99999);


        byte[] strValueAsByteArray = Bytes.from_string("test").to_array();
        RawString sValue = (RawString) RecordSerializer.deserialize(strValueAsByteArray, ColumnTypes.STRING);
        assertEquals(sValue, "test");

        RawString sValueNonNullType = (RawString) RecordSerializer.deserialize(strValueAsByteArray, ColumnTypes.NOTNULL_STRING);
        assertEquals(sValueNonNullType, "test");

        byte[] booleanToByteArray = Bytes.booleanToByteArray(Boolean.TRUE);
        Boolean bValue = (Boolean) RecordSerializer.deserialize(booleanToByteArray, ColumnTypes.BOOLEAN);
        assertEquals(bValue, Boolean.TRUE);

        booleanToByteArray = Bytes.booleanToByteArray(Boolean.FALSE);
        bValue = (Boolean) RecordSerializer.deserialize(booleanToByteArray, ColumnTypes.NOTNULL_BOOLEAN);
        assertEquals(bValue, Boolean.FALSE);

        byte[] doubleToByteArray = Bytes.doubleToByteArray(Double.valueOf(11.0120d));
        Double dValue = (Double) RecordSerializer.deserialize(doubleToByteArray, ColumnTypes.DOUBLE);
        assertEquals(dValue,Double.valueOf(11.0120d));

        dValue = (Double) RecordSerializer.deserialize(doubleToByteArray, ColumnTypes.NOTNULL_DOUBLE);
        assertEquals(dValue, Double.valueOf(11.0120d));

        Timestamp ts = Timestamp.valueOf("2020-07-04 13:17:47.221");
        byte[] tsToByteArray = Bytes.timestampToByteArray(ts);
        Timestamp tsValue = (Timestamp) RecordSerializer.deserialize(tsToByteArray, ColumnTypes.TIMESTAMP);
        assertEquals(tsValue, ts);

        tsValue = (Timestamp) RecordSerializer.deserialize(tsToByteArray, ColumnTypes.NOTNULL_TIMESTAMP);
        assertEquals(tsValue, ts);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSerializeThrowsExceptionOnUnknownType() {
        RecordSerializer.serialize("test", ColumnTypes.ANYTYPE);
    }
}
