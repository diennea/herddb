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

import herddb.utils.MinDeltaLongIncrementAccumulator;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConditionalLongIncrementAccumulatorTest {

    @Test
    public void testApplyAsLong() {
        AtomicLong value = new AtomicLong(0);
        assertEquals(0, value.accumulateAndGet(10, new MinDeltaLongIncrementAccumulator(50)));
        value.set(0);
        assertEquals(10, value.accumulateAndGet(10, new MinDeltaLongIncrementAccumulator(5)));
        value.set(0);
        assertEquals(0, value.accumulateAndGet(0, new MinDeltaLongIncrementAccumulator(5)));
        value.set(0);
        assertEquals(0, value.accumulateAndGet(15, new MinDeltaLongIncrementAccumulator(15)));
        assertEquals(16, value.accumulateAndGet(16, new MinDeltaLongIncrementAccumulator(15)));
    }

}
