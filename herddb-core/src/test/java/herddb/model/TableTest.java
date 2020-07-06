/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.model;

import static org.junit.Assert.assertEquals;
import herddb.utils.Bytes;
import org.junit.Test;

public class TableTest {

    @Test
    public void testSerializeNoDefaults() {
        Table instance = Table
                .builder()
                .name("tt")
                .primaryKey("n1", true)
                .column("k1", ColumnTypes.STRING)
                .column("k2", ColumnTypes.NOTNULL_STRING)
                .column("n1", ColumnTypes.INTEGER)
                .column("n2", ColumnTypes.NOTNULL_INTEGER)
                .column("l1", ColumnTypes.LONG)
                .column("l3", ColumnTypes.NOTNULL_LONG)
                .column("b1", ColumnTypes.BOOLEAN)
                .column("b2", ColumnTypes.NOTNULL_BOOLEAN)
                .column("d1", ColumnTypes.DOUBLE)
                .column("d2", ColumnTypes.NOTNULL_DOUBLE)
                .column("a1", ColumnTypes.BYTEARRAY)
                .column("t1", ColumnTypes.TIMESTAMP)
                .column("t2", ColumnTypes.NOTNULL_TIMESTAMP)
                .build();
        assertEquals(13, instance.maxSerialPosition);
        Table deserialize = Table.deserialize(instance.serialize());
        assertEquals(deserialize, instance);
    }

    @Test
    public void testSerializeWithDefaults() {
        Table instance = Table
                .builder()
                .name("tt")
                .primaryKey("n1", true)
                .column("k1", ColumnTypes.STRING, Bytes.from_string("defaultString"))
                .column("k2", ColumnTypes.NOTNULL_STRING, Bytes.from_string("defaultString"))
                .column("n1", ColumnTypes.INTEGER, Bytes.from_int(42342))
                .column("n2", ColumnTypes.NOTNULL_INTEGER, Bytes.from_int(323))
                .column("l1", ColumnTypes.LONG, Bytes.from_long(-242L))
                .column("l3", ColumnTypes.NOTNULL_LONG, Bytes.from_long(12341L))
                .column("b1", ColumnTypes.BOOLEAN, Bytes.from_boolean(true))
                .column("b2", ColumnTypes.NOTNULL_BOOLEAN, Bytes.from_boolean(false))
                .column("d1", ColumnTypes.DOUBLE, Bytes.from_double(12.3))
                .column("d2", ColumnTypes.NOTNULL_DOUBLE, Bytes.from_double(1.3))
                .column("a1", ColumnTypes.BYTEARRAY) // no default for byte[]
                .column("t1", ColumnTypes.TIMESTAMP, Bytes.from_string("CURRENT_TIMESTAMP"))
                .column("t2", ColumnTypes.NOTNULL_TIMESTAMP, Bytes.from_string("CURRENT_TIMESTAMP"))
                .build();
        assertEquals(13, instance.maxSerialPosition);
        Table deserialize = Table.deserialize(instance.serialize());
        assertEquals(deserialize, instance);
    }

}
