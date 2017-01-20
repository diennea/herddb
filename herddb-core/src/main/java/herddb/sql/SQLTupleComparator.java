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

import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.model.TupleComparator;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Comparator of Tuples, based on SQL
 *
 * @author enrico.olivelli
 */
public class SQLTupleComparator implements TupleComparator {

    private static final class OrderElement {

        private final String name;
        private final boolean asc;

        public OrderElement(String name, boolean asc) {
            this.name = name;
            this.asc = asc;
        }

    }
    private final List<OrderElement> orderByElements;

    SQLTupleComparator(String tableAlias, List<OrderByElement> orderByElements) throws StatementExecutionException {
        if (tableAlias != null) {
            for (OrderByElement element : orderByElements) {
                net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) element.getExpression();
                if (c.getTable() != null && c.getTable().getName() != null && !c.getTable().getName().equalsIgnoreCase(tableAlias)) {
                    throw new StatementExecutionException("invalid column name " + c.getColumnName() + " invalid table name " + c.getTable().getName() + ", expecting " + tableAlias);
                }
            }
        }
        this.orderByElements = new ArrayList<>();
        for (OrderByElement element : orderByElements) {
            net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) element.getExpression();
            this.orderByElements.add(new OrderElement(column.getColumnName().toLowerCase(), element.isAsc()));
        }

    }

    @Override
    public int compare(Tuple o1, Tuple o2) {
        for (OrderElement element : orderByElements) {
            String name = element.name;
            Object value1 = o1.toMap().get(name);
            Object value2 = o2.toMap().get(name);
            int result = compareValues(value1, value2);
            if (result != 0) {
                if (element.asc) {
                    return result;
                } else {
                    return -result;
                }
            }
        }
        return 0;
    }

    private static int compareValues(Object valueA, Object valueB) {
        // NULLS LAST
        if (valueA == null && valueB == null) {
            return 0;
        } else if (valueA != null && valueB == null) {
            return -1;
        } else if (valueA == null && valueB != null) {
            return 1;
        } else {
            if (valueA instanceof Number
                && valueB instanceof Number) {
                double doubleA = ((Number) valueA).doubleValue();
                double doubleB = ((Number) valueB).doubleValue();
                return Double.compare(doubleA, doubleB);
            }

            if (valueA instanceof Comparable
                && valueB instanceof Comparable) {
                try {
                    return ((Comparable<Object>) valueA).compareTo((Object) valueB);
                } catch (Throwable t) {
                }
            }
        }
        throw new IllegalArgumentException("cannot compare " + valueA + " with " + valueB + "!");
    }

}
