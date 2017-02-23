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

import herddb.core.HerdDBInternalException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Groups processing of data
 *
 * @author enrico.olivelli
 */
public class BatchOrderedExecutor<T> implements Consumer<T> {

    private ArrayList<T> batch;
    private final int batchsize;
    private int partialcount = 0;
    private final Executor<T> executor;
    private final Comparator<T> bufferComparator;

    public static interface Executor<VV> {

        public void execute(List<VV> batch) throws HerdDBInternalException;
    }

    public BatchOrderedExecutor(int batchsize, Executor<T> executor, Comparator<T> bufferComparator) {
        this.batchsize = batchsize;
        this.batch = new ArrayList<>();
        this.executor = executor;
        this.bufferComparator = bufferComparator;
    }

    @Override
    public void accept(T object) {
        batch.add(object);
        if (++partialcount == batchsize) {
            try {
                batch.sort(bufferComparator);
                executor.execute(batch);
            } finally {
                batch.clear();
                partialcount = 0;
            }
        }
    }

    public void finish() throws HerdDBInternalException {
        try {
            if (!batch.isEmpty()) {
                try {
                    batch.sort(bufferComparator);
                    executor.execute(batch);
                } finally {
                    batch.clear();
                    partialcount = 0;
                }
            }
        } finally {
            batch = null;
        }
    }

}
