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
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.utils.Bytes;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public SQLAggregator(List<SelectItem> selectItems, List<Expression> groupByColumnReferences) throws StatementExecutionException {
        this.selectItems = selectItems;
        this.groupByColumnReferences = groupByColumnReferences != null ? groupByColumnReferences : Collections.emptyList();
        for (SelectItem e : selectItems) {
            for (SelectItem item : selectItems) {
                boolean done = false;
                System.out.println("looking for "+item);
                if (item instanceof SelectExpressionItem) {
                    SelectExpressionItem sei = (SelectExpressionItem) item;
                    Expression expression = sei.getExpression();
                    if (expression instanceof Function) {
                        Function f = (Function) expression;
                        if (f.isAllColumns() && f.getName().equalsIgnoreCase("count")) {
                            done = true;
                            System.out.println("looking for "+item+" OK!");
                        }
                    } else if (expression instanceof net.sf.jsqlparser.schema.Column) {
                        net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) expression;
                        for (Expression ex : this.groupByColumnReferences) {
                            if (ex instanceof net.sf.jsqlparser.schema.Column) {
                                net.sf.jsqlparser.schema.Column cex = (net.sf.jsqlparser.schema.Column) ex;
                                if (cex.getColumnName().equals(c.getColumnName())) {
                                    System.out.println("looking for "+item+" OK!");
                                    done = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                if (!done) {
                    throw new StatementExecutionException("field " + item + " MUST appear in GROUP BY clause");
                }
            }
        }

    }

    @Override
    public DataScanner aggregate(DataScanner scanner) throws StatementExecutionException {
        return new AggregatedDataScanner(scanner);
    }

    private static final class Group {

        ColumnCalculator[] columns;
        Bytes key;

        public Group(Bytes key, ColumnCalculator[] columns) {
            this.columns = columns;
            this.key = key;
        }

    }

    private class AggregatedDataScanner extends DataScanner {

        private final DataScanner wrapped;

        private DataScanner aggregatedScanner;

        public AggregatedDataScanner(DataScanner wrapped) throws StatementExecutionException {
            super(createOutputColumns(selectItems, wrapped));
            this.wrapped = wrapped;
        }

        private Bytes key(Tuple tuple) throws DataScannerException {
            StringBuilder key = new StringBuilder(); // TODO: use a better way...
            for (Expression groupby : groupByColumnReferences) {
                if (groupby instanceof net.sf.jsqlparser.schema.Column) {
                    net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) groupby;
                    Object value = tuple.get(c.getColumnName());
                    if (value == null) {
                        key.append("NULL");
                    } else if (value instanceof Number) {
                        key.append(value.toString());
                    } else if (value instanceof CharSequence) {
                        key.append(value.toString());
                    } else if (value instanceof java.util.Date) {
                        key.append(((java.util.Date) value).getTime());
                    } else {
                        throw new DataScannerException("cannot group values of type " + value.getClass());
                    }
                    key.append(",");
                } else {
                    throw new DataScannerException("cannot group values of type " + groupby);
                }
            }
            return Bytes.from_string(key.toString());
        }

        private void compute() throws DataScannerException {
            if (!groupByColumnReferences.isEmpty()) {
                Map<Bytes, Group> groups = new HashMap<>();
                while (wrapped.hasNext()) {
                    Tuple tuple = wrapped.next();
                    Bytes key = key(tuple);
                    Group group = groups.get(key);
                    if (group == null) {
                        group = createGroup(key);
                        groups.put(key, group);
                    }
                    for (ColumnCalculator cc : group.columns) {
                        cc.consume(tuple);
                    }
                }
                MaterializedRecordSet results = new MaterializedRecordSet(getSchema());
                for (Group group : groups.values()) {
                    ColumnCalculator[] columns = group.columns;
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
                }
                aggregatedScanner = new SimpleDataScanner(results);
            } else {
                Group group = createGroup(null);
                ColumnCalculator[] columns = group.columns;
                while (wrapped.hasNext()) {
                    Tuple tuple = wrapped.next();
                    for (ColumnCalculator cc : columns) {
                        cc.consume(tuple);
                    }
                }
                MaterializedRecordSet results = new MaterializedRecordSet(getSchema());
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

    private static Column[] createOutputColumns(List<SelectItem> selectItems, DataScanner wrapped) throws StatementExecutionException {
        Column[] columns = new Column[selectItems.size()];
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
                        if (fieldName == null) {
                            fieldName = "field" + i;
                        }
                        columns[i] = Column.column(fieldName, ColumnTypes.LONG);
                        done = true;
                    }
                } else if (expression instanceof net.sf.jsqlparser.schema.Column) {
                    net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) expression;
                    String name = c.getColumnName();
                    boolean found = false;
                    for (Column co : wrapped.getSchema()) {
                        if (co.name.equals(name)) {
                            columns[i] = co;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new StatementExecutionException("cannot find column " + name + " is upstream scanner");
                    }
                    done = true;
                } else {
                    throw new StatementExecutionException("unhandled aggregate query selectable item:" + expression);
                }
            }
            i++;
            if (!done) {
                throw new StatementExecutionException("unhandled aggregate function " + item);
            }
        }
        return columns;
    }

    private Group createGroup(Bytes key) throws DataScannerException {
        // NO GROUP BY, aggregating the whole dataset
        ColumnCalculator[] columns = new ColumnCalculator[selectItems.size()];
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
                } else if (expression instanceof net.sf.jsqlparser.schema.Column) {
                    net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) expression;
                    String fieldName = c.getColumnName();
                    if (sei.getAlias() != null && sei.getAlias().getName() != null) {
                        fieldName = sei.getAlias().getName();
                    }
                    columns[i] = new ColumnValue(fieldName);
                    done = true;

                }
            }
            i++;
            if (!done) {
                throw new DataScannerException("unhandled aggregate function " + item);
            }
        }
        return new Group(key, columns);
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

    private static class ColumnValue implements ColumnCalculator {

        Object value;
        String fieldName;

        public ColumnValue(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public void consume(Tuple tuple) {
            Object _value = tuple.get(fieldName);
            if (value == null) {
                value = _value;
            } else if (!Objects.equals(value, _value)) {
                throw new IllegalStateException("groupby failed: " + value + " <> " + _value);
            }
        }

        @Override
        public Object getValue() {
            return value;
        }

    }

}
