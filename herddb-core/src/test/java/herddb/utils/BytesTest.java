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

import java.nio.charset.StandardCharsets;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author enrico.olivelli
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
        System.out.println("a:"+bytes1.to_string());
        System.out.println("b:"+next.to_string());
        assertArrayEquals(array2, next.data);
    }

}
