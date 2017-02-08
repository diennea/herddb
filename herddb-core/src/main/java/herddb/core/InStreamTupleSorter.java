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

import java.util.Arrays;
import java.util.Comparator;

import herddb.model.Tuple;

/**
 * Collect and sort a maximum number of {@link Tuple Tuples}.
 * <p>
 * A configurable maximum number of tuples will be kept; when reached such limit exceeding tuples will be
 * discarded keeping only the lower ones as stated from given comparator.
 * </p>
 * 
 * @author diego.salvi
 */
public final class InStreamTupleSorter {

    private final int size;
    private final Comparator<Tuple> comparator;
    
    private final Tuple[] tuples;
    private int count;
    
    public InStreamTupleSorter(int size, Comparator<Tuple> comparator) {
        super();
        this.size = size;
        this.comparator = comparator;
        this.tuples = new Tuple[size];
    }
    
    public void collect(Tuple tuple) {
        boolean full = (count >= size);
        
        if (full) {
            final int cmp = comparator.compare(tuples[count -1], tuple);
            if (cmp < 0) {
                return;
            }
        }
        
        int idx = Arrays.binarySearch(tuples, 0, count, tuple, comparator);
        if (idx < 0) {
            idx = -idx - 1;
        }
        
        if (!full) {
            ++count;
        }
        
        System.arraycopy(tuples, idx, tuples, idx + 1, count - (idx + 1));
        
        tuples[idx] = tuple;
    }
    
    public void flushToRecordSet(MaterializedRecordSet rs) {
        for(int i = 0; i < count; ++i){
            rs.add(tuples[i]);
        }
    }
    
}
