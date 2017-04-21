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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.StatementExecutionException;
import herddb.model.TupleComparator;
import herddb.utils.DataAccessor;
import java.util.List;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Comparator of Tuples, based on SQL
 *
 * @author enrico.olivelli
 */
public class SingleColumnSQLTupleComparator implements TupleComparator {

    private static final long serialVersionUID = 007;
    private final String columnName;
    private final boolean asc;
    private final boolean primarykey;

    static TupleComparator make(String tableAlias, List<OrderByElement> orderByElements, String[] primaryKey) throws StatementExecutionException {
        if (orderByElements.size() == 1) {
            return new SingleColumnSQLTupleComparator(tableAlias, orderByElements, primaryKey);
        } else {
            return new MultiColumnSQLTupleComparator(tableAlias, orderByElements);
        }
    }

    private SingleColumnSQLTupleComparator(String tableAlias, List<OrderByElement> orderByElements, String[] primarykey) throws StatementExecutionException {
        
        if (tableAlias != null) {
            for (OrderByElement element : orderByElements) {
                net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) element.getExpression();
                if (c.getTable() != null && c.getTable().getName() != null && !c.getTable().getName().equals(tableAlias)) {
                    throw new StatementExecutionException("invalid column name " + c.getColumnName() + " invalid table name " + c.getTable().getName() + ", expecting " + tableAlias);
                }
            }
        }
        OrderByElement element = orderByElements.get(0);
        net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) element.getExpression();
        this.columnName = column.getColumnName();
        this.asc = element.isAsc();
        this.primarykey = primarykey != null
            && primarykey.length == 1
            && primarykey[0].equals(this.columnName);

    }

    @Override
    @SuppressFBWarnings("RV_NEGATING_RESULT_OF_COMPARETO")
    public int compare(DataAccessor o1, DataAccessor o2) {
        Object value1 = o1.get(columnName);
        Object value2 = o2.get(columnName);
        int result = SQLRecordPredicate.compare(value1, value2);
        if (result != 0) {
            if (asc) {
                return result;
            } else {
                return -result;
            }
        }
        return 0;
    }

    @Override
    public boolean isOnlyPrimaryKeyAndAscending() {
        return primarykey && asc;
    }

}
