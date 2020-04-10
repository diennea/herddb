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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Test;

/**
 * @author enrico.olivelli
 */
public class VisibleByteArrayOutputStreamTest {

    @Test
    public void testMd5() throws Exception {
        byte[] content = "foo".getBytes(StandardCharsets.UTF_8);
        byte[] md5;
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(1000)) {
            oo.write(content);
            assertArrayEquals(content, oo.toByteArray());
            md5 = oo.xxhash64();
            System.out.println("hash:" + Arrays.toString(md5));
            System.out.println("content:" + Arrays.toString(content));
        }

        byte[] expected = XXHash64Utils.digest(content, 0, content.length);
        System.out.println("expected:" + Arrays.toString(expected));
        assertArrayEquals(expected, md5);
    }

    @Test
    public void testToByteArrayNoCopy() throws Exception {
        byte[] content = "foo".getBytes(StandardCharsets.UTF_8);
        byte[] content2 = "fooa".getBytes(StandardCharsets.UTF_8);
        try (VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(3)) {
            oo.write(content);
            assertArrayEquals(content, oo.toByteArray());
            assertNotSame(content, oo.toByteArray());
            // accessing directly the buffer
            assertSame(oo.getBuffer(), oo.toByteArrayNoCopy());

            oo.write('a');
            assertArrayEquals(content2, oo.toByteArray());
            assertNotSame(content, oo.toByteArray());
            assertNotSame(oo.getBuffer(), oo.toByteArrayNoCopy());

        }
    }

}
