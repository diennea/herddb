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
    public Transaction transaction; // no final

    public DataScanner(Transaction transaction, String[] fieldNames, Column[] schema) {
        this.schema = schema;
        this.transaction = transaction;
        this.fieldNames = fieldNames != null ? fieldNames : Column.buildFieldNamesList(schema);
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public long getTransactionId() {
        return transaction != null ? transaction.transactionId : 0;
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

    public List<DataAccessor> consumeAndClose() throws DataScannerException {
        List<DataAccessor> res = consume();
        close();
        return res;
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

    public boolean isRewindSupported() {
        return false;
    }

    public void rewind() throws DataScannerException {
        throw new RuntimeException("not supported for " + this.getClass());
    }

    public Enumerable<DataAccessor> createRewindOnCloseEnumerable() {
        return createEnumerable(true);
    }

    public Enumerable<DataAccessor> createNonRewindableEnumerable() {
        return createEnumerable(false);
    }

    private Enumerable<DataAccessor> createEnumerable(boolean rewindOnClose) {
        return new AbstractEnumerable<DataAccessor>() {
            @Override
            public Enumerator<DataAccessor> enumerator() {
                return asEnumerator(rewindOnClose);
            }
        };
    }

    private boolean enumeratorOpened = false;
    private Enumerator<DataAccessor> asEnumerator(final boolean rewindOnClose) {
        if (enumeratorOpened) {
            try {
                rewind();
            } catch (DataScannerException ex) {
               throw new StatementExecutionException(ex);
            }
        }
        enumeratorOpened = true;
        if (rewindOnClose && !isRewindSupported()) {
            throw new HerdDBInternalException("This datascanner is not rewindable");
        }
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
                if (rewindOnClose) {
                    // close() is another flavour of "rewind", see org.apache.calcite.linq4j.EnumerableDefaults$11$1.closeInner(EnumerableDefaults.java:1953) in Calcite 1.22.0
                    if (isRewindSupported()) {
                        try {
                            rewind();
                        } catch (DataScannerException ex) {
                            throw new HerdDBInternalException(ex);
                        }
                    }
                }
            }

        };
    }
}
