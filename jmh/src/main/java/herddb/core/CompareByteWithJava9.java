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
package herddb.core;

import herddb.utils.CompareBytesUtils;
import java.math.BigInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@Fork(1)
@State(Scope.Benchmark)
public class CompareByteWithJava9 {

    byte[] LONG_ARRAY_1;
    byte[] LONG_ARRAY_2;

    @Param({"8" /*long 8bytes pk*/, "500" /*email address as PK*/})
    public int arraySize;

    @Param({
//        "first", 
        "middle"
//        "last", "none"
    })
    public String mismatchPoint;

    @Setup
    public void setup() throws Exception {

        LONG_ARRAY_1 = new byte[arraySize];
        for (int i = 0; i < LONG_ARRAY_1.length; i++) {
            LONG_ARRAY_1[i] = (byte) i;
        }
        LONG_ARRAY_2 = new byte[arraySize];
        System.arraycopy(LONG_ARRAY_1, 0, LONG_ARRAY_2, 0, arraySize);
        switch (mismatchPoint) {
            case "first":
                LONG_ARRAY_1[0] = 4;
                LONG_ARRAY_2[0] = 8;
                break;
            case "middle":
                LONG_ARRAY_1[LONG_ARRAY_1.length / 2] = 4;
                LONG_ARRAY_2[LONG_ARRAY_2.length / 2] = 8;
                break;
            case "last":
                LONG_ARRAY_1[LONG_ARRAY_1.length - 1] = 4;
                LONG_ARRAY_2[LONG_ARRAY_2.length - 1] = 8;
                break;
            case "none":
                // equals
                break;
            default:
                throw new IllegalStateException();
        }

        System.out.println();
        System.out.println("LONG ARRAY1: " + new BigInteger(LONG_ARRAY_1).toString(16));
        System.out.println("LONG ARRAY2: " + new BigInteger(LONG_ARRAY_2).toString(16));
    }

    @TearDown
    public void tearDown() {

    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int compareComparator() throws Exception {
        return CompareBytesUtils.compare(LONG_ARRAY_1, LONG_ARRAY_2);        
    }
}
