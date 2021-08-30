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
package herddb.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class LogSequenceNumberTest {

    @Test
    public void testCompare() {
        assertEquals(LogSequenceNumber.START_OF_TIME, LogSequenceNumber.START_OF_TIME);
        assertTrue(new LogSequenceNumber(1, 0).compareTo(new LogSequenceNumber(2, 0)) < 0);
        assertTrue(new LogSequenceNumber(2, 0).compareTo(new LogSequenceNumber(1, 0)) > 0);
        assertTrue(new LogSequenceNumber(1, 0).compareTo(new LogSequenceNumber(1, 0)) == 0);
        assertTrue(new LogSequenceNumber(1, 2).compareTo(new LogSequenceNumber(1, 1)) > 0);
        assertTrue(new LogSequenceNumber(1, 1).compareTo(new LogSequenceNumber(1, 2)) < 0);
    }

}
