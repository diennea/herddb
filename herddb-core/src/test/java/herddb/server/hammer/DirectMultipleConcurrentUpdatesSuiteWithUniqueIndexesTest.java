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

package herddb.server.hammer;

import org.junit.Test;


public class DirectMultipleConcurrentUpdatesSuiteWithUniqueIndexesTest extends DirectMultipleConcurrentUpdatesSuite {

    @Test
    public void testWithUniqueIndexes() throws Exception {
        performTest(false, 0, true, true);
    }

    @Test
    public void testWithTransactionsAndUniqueIndexes() throws Exception {
        performTest(true, 0, true, true);
    }

    @Test
    public void testWithCheckpointsAndUniqueIndexes() throws Exception {
        performTest(false, 2000, true, true);
    }

    @Test
    public void testWithTransactionsWithCheckpointsAndUniqueIndexes() throws Exception {
        performTest(true, 2000, true, true);
    }

}
