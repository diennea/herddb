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

import static org.junit.Assert.assertEquals;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * This test must be here and not in herddb-utils, in order to leverage Multi
 * Release JAR feature
 *
 * @author enrico.olivelli
 */
public class RawStringTest {

    @Test
    public void test() throws Exception {
        byte[] test2 = "aaaaaaaaab".getBytes(StandardCharsets.UTF_8);
        RawString a = RawString.of("b");
        RawString b = RawString.newPooledRawString(test2, 9, 1);
        assertEquals(a, b);
    }
}
