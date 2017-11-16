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
package herddb.model;

import herddb.utils.DataAccessor;

/**
 *
 * @author enrico.olivelli
 */
public class LimitedDataScanner extends DataScanner {

    int remaining;
    final int offset;
    final DataScanner wrapped;

    public LimitedDataScanner(DataScanner wrapped, ScanLimits limits, StatementEvaluationContext context) throws DataScannerException, StatementExecutionException {
        this(wrapped, limits.computeMaxRows(context), limits.computeOffset(context), context);
    }
    public LimitedDataScanner(DataScanner wrapped, int maxRows,int offset,  StatementEvaluationContext context) throws DataScannerException, StatementExecutionException {
        super(wrapped.transactionId, wrapped.getFieldNames(), wrapped.getSchema());
        this.remaining = maxRows > 0 ? maxRows : -1;
        this.offset = offset;
        this.wrapped = wrapped;
        if (this.offset > 0) {
            for (int i = 0; i < this.offset; i++) {
                if (wrapped.hasNext()) {
                    wrapped.next();
                }
            }
        }
    }

    @Override
    public boolean hasNext() throws DataScannerException {
        if (remaining == -1) {
            return wrapped.hasNext();
        } else {
            return remaining > 0 & wrapped.hasNext();
        }
    }

    @Override
    public DataAccessor next() throws DataScannerException {
        DataAccessor result = wrapped.next();
        remaining--;
        return result;
    }

    @Override
    public void close() throws DataScannerException {
        wrapped.close();
    }

}
