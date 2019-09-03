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

import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.Transaction;
import herddb.utils.DataAccessor;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple data scanner on a in memory MaterializedRecordSet
 *
 * @author enrico.olivelli
 */
public class SimpleDataScanner extends DataScanner {

    private final MaterializedRecordSet recordSet;
    private Iterator<DataAccessor> iterator;
    private DataAccessor next;
    private boolean finished;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SimpleDataScanner(Transaction transaction, MaterializedRecordSet recordSet) {
        super(transaction, recordSet.fieldNames, recordSet.columns);
        this.recordSet = recordSet;
        this.iterator = this.recordSet.iterator();
        if (transaction != null) {
            transaction.increaseRefcount();
        }
    }

    @Override
    public void close() throws DataScannerException {
        if (closed.compareAndSet(false, true)) {
            if (transaction != null) {
                transaction.decreaseRefCount();
            }
        }
        finished = true;
        try {
            recordSet.close();
        } finally {
            super.close();
        }
    }

    @Override
    public boolean hasNext() throws DataScannerException {
        if (finished) {
            return false;
        }
        return ensureNext();
    }

    private boolean ensureNext() throws DataScannerException {
        if (next != null) {
            return true;
        }
        while (true) {
            if (!iterator.hasNext()) {
                finished = true;
                return false;
            }
            next = iterator.next();
            return true;
            // RECORD does not match, iterate again
        }
    }

    @Override
    public DataAccessor next() throws DataScannerException {
        if (finished) {
            throw new DataScannerException("Scanner is exhausted");
        }
        DataAccessor _next = next;
        next = null;
        return _next;
    }

    @Override
    public void rewind() throws DataScannerException {
        this.finished = false;
        this.iterator = this.recordSet.iterator();
        this.next = null;
    }

}
