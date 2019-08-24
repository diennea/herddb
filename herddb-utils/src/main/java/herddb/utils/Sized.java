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

/**
 * An utility class for wrap objects into {@link SizeAwareObject} instances. It should used only for testing
 * purposes.
 *
 * @param <O>
 * @author diego.salvi
 */
public final class Sized<O extends Comparable<O>> implements SizeAwareObject, Comparable<Sized<O>> {

    private static final long DEFAULT_SIZE = 100L;

    public static <X extends Comparable<X>> Sized<X> valueOf(X x) {
        return new Sized<>(x, DEFAULT_SIZE);
    }

    public static Sized<Integer> valueOf(Integer x) {
        return new Sized<>(x, Integer.BYTES);
    }

    public static Sized<Long> valueOf(Long x) {
        return new Sized<>(x, Long.BYTES);
    }

    public static Sized<String> valueOf(String x) {
        return new Sized<>(x, x.getBytes(StandardCharsets.UTF_16BE).length);
    }

    public final O dummy;
    public final long size;

    private Sized(O dummy, long size) {
        super();
        this.dummy = dummy;
        this.size = size;
    }

    @Override
    public long getEstimatedSize() {
        return size;
    }

    @Override
    public int compareTo(Sized<O> o) {
        return dummy.compareTo(o.dummy);
    }

    @Override
    public String toString() {
        return dummy.toString();
    }

    @Override
    public int hashCode() {
        return dummy.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Sized) {
            return dummy.equals(((Sized<?>) obj).dummy);
        }

        return false;
    }

}
