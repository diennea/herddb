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

package herddb.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Test on {@link Bytes} facility
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class BytesTest {

    public BytesTest() {
    }

    @Test
    public void testNext() {
        byte[] array1 = "test".getBytes(StandardCharsets.UTF_8);
        Bytes bytes1 = Bytes.from_array(array1);
        Bytes next = bytes1.next();
        byte[] array2 = "tesu".getBytes(StandardCharsets.UTF_8);
        System.out.println("a:" + bytes1.to_string());
        System.out.println("b:" + next.to_string());
        assertArrayEquals(array2, next.to_array());
    }

    @Test
    public void testNextSameByte() {

        byte[] array = new byte[]{0, 1};
        byte[] nextExpected = new byte[]{0, 2};

        Bytes bytes = Bytes.from_array(array);
        Bytes next = bytes.next();

        assertArrayEquals(nextExpected, next.to_array());
    }

    /**
     * Check that the change propagate to next byte if 255 (-1)
     */
    @Test
    public void testNextChangeByte() {

        byte[] array = new byte[]{0, -1};
        byte[] nextExpected = new byte[]{1, 0};

        Bytes bytes = Bytes.from_array(array);
        Bytes next = bytes.next();

        assertArrayEquals(nextExpected, next.to_array());
    }

    /**
     * Check that more than one byte is changed if needed
     */
    @Test
    public void testNextChangeByteMoreTimes() {

        byte[] array = new byte[]{0, -1, -1};
        byte[] nextExpected = new byte[]{1, 0, 0};

        Bytes bytes = Bytes.from_array(array);
        Bytes next = bytes.next();

        assertArrayEquals(nextExpected, next.to_array());
    }

    /**
     * Checks that prefix bytes aren't touched
     */
    @Test
    public void testNextChangeByteMoreTimesWithPrefix() {

        byte[] array = new byte[]{1, 0, -1, -1};
        byte[] nextExpected = new byte[]{1, 1, 0, 0};

        Bytes bytes = Bytes.from_array(array);
        Bytes next = bytes.next();

        assertArrayEquals(nextExpected, next.to_array());
    }

    /**
     * Checks that next fails if there is no more space
     */
    @Test(expected = IllegalStateException.class)
    public void testNextNoMoreSpace() {

        byte[] array = new byte[]{-1, -1};

        Bytes bytes = Bytes.from_array(array);
        bytes.next();
    }

    /**
     * Check that leading zeros are preserved
     */
    @Test
    public void testNextLenPreservation() {

        byte[] src = new byte[]{0, 0, 0, -1};

        Bytes bytes = Bytes.from_array(src);

        assertArrayEquals(src, bytes.to_array());

        Bytes next = bytes.next();

        assertEquals(bytes.getLength(), next.getLength());

    }

}
