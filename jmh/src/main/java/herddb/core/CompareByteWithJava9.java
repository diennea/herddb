/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
package herddb.core;

import herddb.utils.Bytes;
import herddb.utils.CompareBytesUtils;
import java.math.BigInteger;
import java.util.Arrays;
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
