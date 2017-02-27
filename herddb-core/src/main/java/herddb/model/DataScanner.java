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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Live scanner of data
 *
 * @author enrico.olivelli
 */
public abstract class DataScanner implements AutoCloseable {

    private final Column[] schema;
    private final String[] fieldNames;
    public final long transactionId;

    public DataScanner(long transactionId, String[] fieldNames, Column[] schema) {
        this.schema = schema;
        this.transactionId = transactionId;
        this.fieldNames = fieldNames != null ? fieldNames : Column.buildFieldNamesList(schema);

    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public Column[] getSchema() {
        return schema;
    }

    public abstract boolean hasNext() throws DataScannerException;

    public abstract Tuple next() throws DataScannerException;

    /**
     * Consumers all the records in memory
     *
     * @param consumer
     * @throws herddb.model.DataScannerException
     */
    public void forEach(Consumer<Tuple> consumer) throws DataScannerException {
        while (hasNext()) {
            consumer.accept(next());
        }
    }

    @Override
    public void close() throws DataScannerException {
    }

    /**
     * Consumers all the records in memory
     *
     * @return
     */
    public List<Tuple> consume() throws DataScannerException {
        List<Tuple> records = new ArrayList<>();
        forEach(records::add);
        return records;
    }

    public List<Tuple> consume(int fetchSize) throws DataScannerException {
        List<Tuple> records = new ArrayList<>();
        while (fetchSize-- > 0 && hasNext()) {
            records.add(next());
        }
        return records;
    }

    public boolean isFinished() throws DataScannerException {
        return !hasNext();
    }

    public void rewind() throws DataScannerException {
        throw new RuntimeException("yet implemented");
    }
}
