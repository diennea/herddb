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

import herddb.sql.functions.ColumnValue;
import herddb.core.MaterializedRecordSet;
import herddb.core.RecordSetFactory;
import herddb.core.SimpleDataScanner;
import herddb.model.Aggregator;
import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.sql.functions.BuiltinFunctions;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.RawString;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final RecordSetFactory recordSetFactory;

    public SQLAggregator(List<SelectItem> selectItems, List<Expression> groupByColumnReferences, RecordSetFactory recordSetFactory) throws StatementExecutionException {
        this.recordSetFactory = recordSetFactory;
        this.selectItems = selectItems;
        this.groupByColumnReferences = groupByColumnReferences != null ? groupByColumnReferences : Collections.emptyList();

        for (SelectItem item : selectItems) {
            boolean done = false;
            if (item instanceof SelectExpressionItem) {
                SelectExpressionItem sei = (SelectExpressionItem) item;
                Expression expression = sei.getExpression();
                if (expression instanceof Function) {
                    Function f = (Function) expression;
                    if (BuiltinFunctions.isAggregateFunction(f.getName())) {
                        done = true;
                    }
                } else if (expression instanceof net.sf.jsqlparser.schema.Column) {
                    net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) expression;
                    for (Expression ex : this.groupByColumnReferences) {
                        if (ex instanceof net.sf.jsqlparser.schema.Column) {
                            net.sf.jsqlparser.schema.Column cex = (net.sf.jsqlparser.schema.Column) ex;
                            if (cex.getColumnName().equals(c.getColumnName())) {
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

    @Override
    public DataScanner aggregate(DataScanner scanner, StatementEvaluationContext context) throws StatementExecutionException {
        return new AggregatedDataScanner(scanner, context);
    }

    private static final class Group {

        AggregatedColumnCalculator[] columns;

        public Group(AggregatedColumnCalculator[] columns) {
            this.columns = columns;
        }

    }

    private class AggregatedDataScanner extends DataScanner {

        private final DataScanner wrapped;

        private DataScanner aggregatedScanner;
        private StatementEvaluationContext context;

        public AggregatedDataScanner(DataScanner wrapped, StatementEvaluationContext context) throws StatementExecutionException {
            super(wrapped.transactionId, null, createOutputColumns(selectItems, wrapped));
            this.wrapped = wrapped;
            this.context = context;
        }

        private Bytes key(DataAccessor tuple) throws DataScannerException {
            VisibleByteArrayOutputStream o = new VisibleByteArrayOutputStream(0);
            try (ExtendedDataOutputStream key = new ExtendedDataOutputStream(o);) {
                for (Expression groupby : groupByColumnReferences) {
                    if (groupby instanceof net.sf.jsqlparser.schema.Column) {
                        net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) groupby;
                        Object value = tuple.get(c.getColumnName());
                        if (value == null) {
                            key.write(0);
                        } else if (value instanceof Integer) {
                            key.writeInt(((Integer) value).intValue());
                        } else if (value instanceof Long) {
                            key.writeLong(((Long) value).longValue());
                        } else if (value instanceof Number) {
                            key.writeLong(((Number) value).longValue());
                        } else if (value instanceof RawString) {
                            key.writeArray(((RawString) value).data);
                        } else if (value instanceof CharSequence) {
                            key.writeUTF(value.toString());
                        } else if (value instanceof java.util.Date) {
                            key.writeLong(((java.util.Date) value).getTime());
                        } else {
                            throw new DataScannerException("cannot group values of type " + value.getClass());
                        }
                        key.write(',');
                    } else {
                        throw new DataScannerException("cannot group values of type " + groupby);
                    }
                }
            } catch (IOException impossible) {
                throw new RuntimeException(impossible);
            };
            return Bytes.from_array(o.toByteArray());
        }

        private void compute() throws DataScannerException {
            try {
                if (!groupByColumnReferences.isEmpty()) {
                    Map<Bytes, Group> groups = new HashMap<>();
                    while (wrapped.hasNext()) {
                        DataAccessor tuple = wrapped.next();
                        Bytes key = key(tuple);
                        Group group = groups.get(key);
                        if (group == null) {
                            group = createGroup(key);
                            groups.put(key, group);
                        }
                        for (AggregatedColumnCalculator cc : group.columns) {
                            cc.consume(tuple);
                        }
                    }
                    MaterializedRecordSet results = recordSetFactory
                        .createFixedSizeRecordSet(groups.values().size(),
                            getFieldNames(), getSchema());
                    for (Group group : groups.values()) {
                        AggregatedColumnCalculator[] columns = group.columns;
                        String[] fieldNames = new String[columns.length];
                        Object[] values = new Object[columns.length];
                        int k = 0;
                        for (AggregatedColumnCalculator cc : columns) {
                            fieldNames[k] = cc.getFieldName();
                            values[k] = cc.getValue();
                            k++;
                        }
                        Tuple tuple = new Tuple(fieldNames, values);
                        results.add(tuple);
                    }
                    results.writeFinished();
                    aggregatedScanner = new SimpleDataScanner(wrapped.transactionId, results);
                } else {
                    Group group = createGroup(null);
                    AggregatedColumnCalculator[] columns = group.columns;
                    while (wrapped.hasNext()) {
                        DataAccessor tuple = wrapped.next();
                        for (AggregatedColumnCalculator cc : columns) {
                            cc.consume(tuple);
                        }
                    }
                    String[] fieldNames = new String[columns.length];
                    Object[] values = new Object[columns.length];
                    int k = 0;
                    for (AggregatedColumnCalculator cc : columns) {
                        fieldNames[k] = cc.getFieldName();
                        values[k] = cc.getValue();
                        k++;
                    }
                    Tuple tuple = new Tuple(fieldNames, values);
                    MaterializedRecordSet results = recordSetFactory
                        .createFixedSizeRecordSet(1, getFieldNames(), getSchema());
                    results.add(tuple);
                    results.writeFinished();
                    aggregatedScanner = new SimpleDataScanner(wrapped.transactionId, results);
                }
            } catch (StatementExecutionException err) {
                throw new DataScannerException(err);
            }
        }

        private Group createGroup(Bytes key) throws DataScannerException, StatementExecutionException {
            // NO GROUP BY, aggregating the whole dataset
            AggregatedColumnCalculator[] columns = new AggregatedColumnCalculator[selectItems.size()];
            int i = 0;
            for (SelectItem item : selectItems) {
                boolean done = false;
                if (item instanceof SelectExpressionItem) {
                    SelectExpressionItem sei = (SelectExpressionItem) item;

                    Expression expression = sei.getExpression();
                    if (expression instanceof Function) {
                        Function f = (Function) expression;
                        String fieldName = f.toString();
                        if (sei.getAlias() != null && sei.getAlias().getName() != null) {
                            fieldName = sei.getAlias().getName();
                        }
                        AggregatedColumnCalculator calculator = BuiltinFunctions.getColumnCalculator(f, fieldName, context);
                        if (calculator != null) {
                            columns[i] = calculator;
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
            return new Group(columns);
        }

        @Override
        public boolean hasNext() throws DataScannerException {
            if (aggregatedScanner == null) {
                compute();
            }
            return aggregatedScanner.hasNext();
        }

        @Override
        public DataAccessor next() throws DataScannerException {
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
                    String fieldName = f.toString();
                    if (sei.getAlias() != null && sei.getAlias().getName() != null) {
                        fieldName = sei.getAlias().getName();
                    }
                    if (fieldName == null) {
                        fieldName = "field" + i;
                    }
                    Column aggregated = BuiltinFunctions.toAggregatedOutputColumn(fieldName, f);
                    if (aggregated != null) {
                        columns[i] = aggregated;
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

}
