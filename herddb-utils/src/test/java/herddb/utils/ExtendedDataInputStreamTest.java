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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

/**
 * {@link ExtendedDataInputStream} tests
 *
 * @author diego.salvi
 */
public class ExtendedDataInputStreamTest {

    /**
     * Test read folded and unfolded vint version for compatibility
     *
     * @throws IOException
     */
    @Test
    public void testReadVInt() throws IOException {

        checkAndCompareVInt(0, new byte[]{0});
        checkAndCompareVInt(1, new byte[]{1});
        checkAndCompareVInt(-1, new byte[]{-1, -1, -1, -1, 15});
        checkAndCompareVInt(Integer.MIN_VALUE, new byte[]{-128, -128, -128, -128, 8});
        checkAndCompareVInt(Integer.MAX_VALUE, new byte[]{-1, -1, -1, -1, 7});

    }

    protected int readVIntFolded(byte first, ExtendedDataInputStream ii) throws IOException {
        byte b = first;
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = ii.readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }

    private void checkAndCompareVInt(int expected, byte[] data) throws IOException {

        try (ExtendedDataInputStream is = new ExtendedDataInputStream(new ByteArrayInputStream(data))) {
            is.mark(5);
            int folded = readVIntFolded(is.readByte(), is);

            is.reset();
            int unfolded = is.readVInt();

            Assert.assertEquals(expected, unfolded);
            Assert.assertEquals(expected, folded);
        }

    }

}
