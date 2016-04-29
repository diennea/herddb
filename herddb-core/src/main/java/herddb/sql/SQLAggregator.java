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
package herddb.sql;

import herddb.core.MaterializedRecordSet;
import herddb.core.SimpleDataScanner;
import herddb.model.Aggregator;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.Tuple;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Aggregates a dataset using group by/aggregate functions
 *
 * @author enrico.olivelli
 */
public class SQLAggregator implements Aggregator {

    private final List<SelectItem> selectItems;
    private final List<Expression> groupByColumnReferences;

    public SQLAggregator(List<SelectItem> selectItems, List<Expression> groupByColumnReferences) {
        this.selectItems = selectItems;
        this.groupByColumnReferences = groupByColumnReferences;
    }

    @Override
    public DataScanner aggregate(DataScanner scanner) {
        return new AggregatedDataScanner(scanner);
    }

    private class AggregatedDataScanner extends DataScanner {

        private final DataScanner wrapped;
        private final ColumnCalculator[] columns;
        private DataScanner aggregatedScanner;

        public AggregatedDataScanner(DataScanner wrapped) {
            this.wrapped = wrapped;
            columns = new ColumnCalculator[selectItems.size()];
        }

        private void compute() throws DataScannerException {
            if (groupByColumnReferences != null && !groupByColumnReferences.isEmpty()) {
                throw new DataScannerException("not yet implemented, GROUP BY");
            } else {
                // NO GROUP BY, aggregating the whole dataset   
                int i = 0;
                for (SelectItem item : selectItems) {
                    boolean done = false;
                    if (item instanceof SelectExpressionItem) {
                        SelectExpressionItem sei = (SelectExpressionItem) item;
                        Expression expression = sei.getExpression();
                        if (expression instanceof Function) {
                            Function f = (Function) expression;
                            if (f.isAllColumns() && f.getName().equalsIgnoreCase("count")) {
                                String fieldName = f.toString();
                                if (sei.getAlias() != null && sei.getAlias().getName() != null) {
                                    fieldName = sei.getAlias().getName();
                                }

                                columns[i] = new CountColumnCalculator(fieldName);
                                done = true;
                            }
                        }
                    }
                    i++;
                    if (!done) {
                        throw new DataScannerException("unhandled aggregate function " + item);
                    }
                }
                while (wrapped.hasNext()) {
                    Tuple tuple = wrapped.next();
                    for (ColumnCalculator cc : columns) {
                        cc.consume(tuple);
                    }
                }
                MaterializedRecordSet results = new MaterializedRecordSet();
                String[] fieldNames = new String[columns.length];
                Object[] values = new Object[columns.length];
                int k = 0;
                for (ColumnCalculator cc : columns) {
                    fieldNames[k] = cc.getFieldName();
                    values[k] = cc.getValue();
                    k++;
                }
                Tuple tuple = new Tuple(fieldNames, values);
                results.records.add(tuple);
                aggregatedScanner = new SimpleDataScanner(results);
            }

        }

        @Override
        public boolean hasNext() throws DataScannerException {
            if (aggregatedScanner == null) {
                compute();
            }
            return aggregatedScanner.hasNext();
        }

        @Override
        public Tuple next() throws DataScannerException {
            if (aggregatedScanner == null) {
                compute();
            }
            return aggregatedScanner.next();
        }

        @Override
        public void close() throws DataScannerException {
            wrapped.close();
        }
    }

    private static interface ColumnCalculator {

        public Object getValue();

        public String getFieldName();

        public void consume(Tuple tuple);
    }

    private static class CountColumnCalculator implements ColumnCalculator {

        long count;
        String fieldName;

        public CountColumnCalculator(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public void consume(Tuple tuple) {
            count++;
        }

        @Override
        public Object getValue() {
            return count;
        }

    }

}
