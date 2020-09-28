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

package herddb.model.commands;

import herddb.model.Column;
import herddb.model.Predicate;
import herddb.model.Projection;
import herddb.model.ScanLimits;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableAwareStatement;
import herddb.model.TupleComparator;
import herddb.model.planner.SortOp;
import herddb.utils.ObjectSizeUtils;
import java.util.Arrays;

/**
 * Lookup a bunch record with a condition
 *
 * @author enrico.olivelli
 */
public class ScanStatement extends TableAwareStatement {

    private Predicate predicate;
    private Projection projection;
    private TupleComparator comparator;
    private ScanLimits limits;
    private Table tableDef;
    private boolean allowExecutionFromFollower = false;

    public ScanStatement(String tableSpace, Table table, Predicate predicate) {
        this(tableSpace, table, Projection.IDENTITY(table.columnNames, table.columns), predicate);
    }

    public ScanStatement(String tableSpace, Table table, final Projection projection, Predicate predicate) {
        this(tableSpace, table.name, projection, predicate, null, null);
        this.tableDef = table;
    }

    public ScanStatement(final String tableSpace, final String table, final Projection projection, final Predicate predicate, final TupleComparator comparator, final ScanLimits limits) {
        super(table, tableSpace);
        this.predicate = predicate;
        this.projection = projection;
        this.comparator = comparator;
        this.limits = limits;
    }

    public Table getTableDef() {
        return tableDef;
    }

    public ScanLimits getLimits() {
        return limits;
    }

    public TupleComparator getComparator() {
        return comparator;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public Projection getProjection() {
        return projection;
    }
    public boolean getAllowExecutionFromFollower() {
        return allowExecutionFromFollower;
    }

    public void setAllowExecutionFromFollower(boolean allowExecutionFromFollower) {
        this.allowExecutionFromFollower = allowExecutionFromFollower;
    }

    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    public void setProjection(Projection projection) {
        this.projection = projection;
    }

    public void setComparator(TupleComparator comparator) {
        this.comparator = comparator;
    }

    public void setLimits(ScanLimits limits) {
        this.limits = limits;
    }

    @Override
    public String toString() {
        String comparatorString = "";
        if (comparator != null) {
            if (comparator instanceof SortOp) {
                comparatorString = "SortOp";  // prevent recursion
            } else {
                comparatorString = comparator.toString();
            }
        }
        String schema = Arrays.toString(getSchema());
        if (limits != null) {
            return "ScanStatement{table=" + table + "," + "predicate=" + predicate + ",proj=" + projection + ", comparator=" + comparatorString + ",limits=" + limits.toStringForScan() + ", schema=" + schema + '}';
        } else {
            return "ScanStatement{table=" + table + "," + "predicate=" + predicate + ",proj=" + projection + ", comparator=" + comparatorString + ", schema=" + schema + '}';
        }
    }

    @Override
    public void validateContext(StatementEvaluationContext context) throws StatementExecutionException {
        if (predicate != null) {
            predicate.validateContext(context);
        }
    }

    public void setTableDef(Table table) {
        this.tableDef = table;
    }

    @Override
    public int estimateObjectSizeForCache() {
        int res = super.estimateObjectSizeForCache() + ObjectSizeUtils.BOOLEAN_FIELD_SIZE;
        if (predicate != null) {
            res += predicate.estimateObjectSizeForCache();
        }
        // tableDef is a shared object, it does not have a real impact
        // on heap usage for the cache
        if (limits != null) {
            res += ObjectSizeUtils.DEFAULT_OBJECT_SIZE_OVERHEAD;
        }
        if (comparator != null) {
            res += ObjectSizeUtils.DEFAULT_OBJECT_SIZE_OVERHEAD;
        }
        if (projection != null) {
            res += ObjectSizeUtils.DEFAULT_OBJECT_SIZE_OVERHEAD;
        }
        return res;
    }

    public Column[] getSchema() {
        if (projection != null) {
            return projection.getColumns();
        }
        return tableDef.getColumns();
    }

}
