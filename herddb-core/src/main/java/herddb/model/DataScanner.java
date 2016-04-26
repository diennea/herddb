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

import herddb.model.commands.ScanStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Live scanner of data
 *
 * @author enrico.olivelli
 */
public abstract class DataScanner implements AutoCloseable {

    protected final Table table;
    protected final ScanStatement scan;

    public DataScanner(Table table, ScanStatement scan) {
        this.table = table;
        this.scan = scan;
    }

    public Table getTable() {
        return table;
    }

    public ScanStatement getScan() {
        return scan;
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
}
