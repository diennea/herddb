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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.HerdDBInternalException;
import herddb.utils.DataAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

/**
 * Live scanner of data
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public abstract class DataScanner implements AutoCloseable {

    private final Column[] schema;
    private final String[] fieldNames;
    public long transactionId;

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

    public abstract DataAccessor next() throws DataScannerException;

    /**
     * Consumers all the records in memory
     *
     * @param consumer
     * @throws herddb.model.DataScannerException
     */
    public void forEach(Consumer<DataAccessor> consumer) throws DataScannerException {
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
    public List<DataAccessor> consume() throws DataScannerException {
        List<DataAccessor> records = new ArrayList<>();
        forEach(records::add);
        return records;
    }

    public List<DataAccessor> consume(int fetchSize) throws DataScannerException {
        List<DataAccessor> records = new ArrayList<>();
        while (fetchSize-- > 0 && hasNext()) {
            records.add(next());
        }
        return records;
    }

    public boolean isFinished() throws DataScannerException {
        return !hasNext();
    }

    public void rewind() throws DataScannerException {
        throw new RuntimeException("not implemented for " + this.getClass());
    }

    public Enumerable<DataAccessor> createEnumerable() {
        return new AbstractEnumerable<DataAccessor>() {
            @Override
            public Enumerator<DataAccessor> enumerator() {
                return asEnumerator();
            }
        };
    }

    public Enumerator<DataAccessor> asEnumerator() {
        return new Enumerator<DataAccessor>() {
            private DataAccessor current;

            @Override
            public DataAccessor current() {
                return current;
            }

            @Override
            public boolean moveNext() {
                try {
                    if (hasNext()) {
                        current = next();
                        return true;
                    } else {
                        return false;
                    }
                } catch (DataScannerException ex) {
                    throw new HerdDBInternalException(ex);
                }
            }

            @Override
            public void reset() {
                try {
                    rewind();
                } catch (DataScannerException ex) {
                    throw new HerdDBInternalException(ex);
                }
            }

            @Override
            public void close() {
                try {
                    DataScanner.this.close();
                } catch (DataScannerException ex) {
                    throw new HerdDBInternalException(ex);
                }
            }

        };
    }
;
}
