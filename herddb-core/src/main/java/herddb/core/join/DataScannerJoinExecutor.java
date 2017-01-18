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
package herddb.core.join;

import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import java.util.List;

/**
 * Executes a JOINS between DataScanners
 *
 * @author enrico.olivelli
 */
public class DataScannerJoinExecutor {

    private final DataScanner[] scanners;
    private final int numScanners;
    private final TupleAcceptor consumer;
    private final String[] fieldNames;
    private final Object[] tmpTuple;

    @FunctionalInterface
    public static interface TupleAcceptor {

        public void accept(Tuple tuple) throws StatementExecutionException;
    }

    public DataScannerJoinExecutor(Column[] schema, List<DataScanner> scanners, TupleAcceptor consumer) {
        this.scanners = new DataScanner[scanners.size()];
        scanners.toArray(this.scanners);
        this.consumer = consumer;
        this.numScanners = this.scanners.length;
        this.fieldNames = new String[schema.length];
        for (int i = 0; i < schema.length; i++) {
            fieldNames[i] = schema[i].name;
        }
        tmpTuple = new Object[fieldNames.length];
    }

    public void executeJoin() throws DataScannerException, StatementExecutionException {
        if (numScanners <= 0) {
            throw new DataScannerException("no tables in JOIN ?");
        }
        nestedLoop(0, 0);

        for (DataScanner scan : scanners) {
            scan.close();
        }
    }

    private void nestedLoop(int index, int pos) throws DataScannerException, StatementExecutionException {
        if (index == numScanners) {
            Object[] clone = new Object[fieldNames.length];
            System.arraycopy(tmpTuple, 0, clone, 0, fieldNames.length);
            consumer.accept(new Tuple(fieldNames, clone));
            return;
        }
        DataScanner scanner = scanners[index];
        scanner.rewind();
        while (scanner.hasNext()) {
            int startPos = pos;
            Tuple rightTuple = scanner.next();
            for (Object o : rightTuple.values) {
                tmpTuple[startPos++] = o;
            }
            nestedLoop(index + 1, startPos);
        }
    }

}
