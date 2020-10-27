/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.server;

import herddb.client.HDBException;
import herddb.client.ScanResultSet;
import herddb.client.ScanResultSetMetadata;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.utils.DataAccessor;

/**
 * Adapted from DataScanner to ScanResultSet.
 */
public class LocalClientScanResultSetImpl extends ScanResultSet {

    private final DataScanner dataScanner;
    private final ScanResultSetMetadata metadata;

    LocalClientScanResultSetImpl(DataScanner dataScanner) {
        super(dataScanner.getTransactionId());
        this.dataScanner = dataScanner;
        this.metadata = new ScanResultSetMetadata(dataScanner.getFieldNames());
    }

    @Override
    public ScanResultSetMetadata getMetadata() {
        return metadata;
    }

    @Override
    public void close() {
        if (dataScanner.isClosed()) {
            return;
        }
        try {
            dataScanner.close();
        } catch (DataScannerException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean hasNext() throws HDBException {
        try {
            return dataScanner.hasNext();
        } catch (DataScannerException ex) {
            throw new HDBException(ex);
        }
    }

    @Override
    public DataAccessor next() throws HDBException {
        try {
            return dataScanner.next();
        } catch (DataScannerException ex) {
            throw new HDBException(ex);
        }
    }

    @Override
    public String getCursorName() {
        return "<scanner-" + System.identityHashCode(dataScanner) + "-@tx" + transactionId + ">";
    }

}
