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
package herddb.index.brin;

import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexBench {

    @Test
//    @Ignore
    public void testHuge() {
        final int testSize = 1_000_000;

        long _start = System.currentTimeMillis();
        BlockRangeIndex<Integer, String> index = new BlockRangeIndex<>(10000);
        for (int i = 0; i < testSize; i++) {
            index.put(i, "test_" + i);
        }
        long _stop = System.currentTimeMillis();
        System.out.println("time w: " + (_stop - _start));
        System.out.println("num segments: " + index.getNumBlocks());
        for (int i = 0; i < testSize; i++) {
            index.search(i);
//            index.lookUpRange(i, i + 1000);
        }
        _start = _stop;
        _stop = System.currentTimeMillis();
        System.out.println("time r: " + (_stop - _start));
        index.clear();
    }

}
