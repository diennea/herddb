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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * @author eolivelli
 */
public class InstrumentationUtilsTest {

    @Test
    public void testInstrument() {
        AtomicInteger i = new AtomicInteger();
        InstrumentationUtils.addListener((String id, Object... args) -> {
            if (id.equals("one") && args.length == 1 && args[0].equals("two")) {
                i.incrementAndGet();
            }
        });
        InstrumentationUtils.instrument("one", "two");
        assertEquals(1, i.get());
        InstrumentationUtils.instrument("one", "three");
        assertEquals(1, i.get());
        InstrumentationUtils.instrument("one", "two");
        assertEquals(2, i.get());
        InstrumentationUtils.clear();
        InstrumentationUtils.instrument("one", "two");
        assertEquals(2, i.get());
    }

}
