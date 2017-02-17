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

import herddb.model.commands.InsertStatement;
import herddb.model.commands.ScanStatement;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data access execution plan.
 *
 * @author enrico.olivelli
 */
public class ExecutionPlan {

    private final static AtomicLong ID = new AtomicLong();
    private final long id = ID.incrementAndGet();
    public final Statement mainStatement;
    public final Aggregator mainAggregator;
    public final ScanLimits limits;
    public final TupleComparator comparator;
    public final List<ScanStatement> joinStatements;
    public final TuplePredicate joinFilter;
    public final Projection joinProjection;
    public final ExecutionPlan dataSource;
    public final List<InsertStatement> insertStatements;

    private ExecutionPlan(Statement mainStatement,
        Aggregator mainAggregator,
        ScanLimits limits,
        TupleComparator comparator,
        ExecutionPlan dataSource,
        List<ScanStatement> joinStatements,
        TuplePredicate joinFilter,
        Projection joinProjection,
        List<InsertStatement> insertStatements) {
        this.mainStatement = mainStatement;
        this.mainAggregator = mainAggregator;
        this.limits = limits;
        this.comparator = comparator;
        this.dataSource = dataSource;
        this.joinStatements = joinStatements;
        this.joinFilter = joinFilter;
        this.joinProjection = joinProjection;
        this.insertStatements = insertStatements;
    }

    public static ExecutionPlan simple(Statement statement) {
        return new ExecutionPlan(statement, null, null, null, null, null, null, null, null);
    }

    public static ExecutionPlan multiInsert(List<InsertStatement> statements) {
        return new ExecutionPlan(null, null, null, null, null, null, null, null, statements);
    }

    public static ExecutionPlan make(Statement statement, Aggregator aggregator, ScanLimits limits, TupleComparator comparator) {
        return new ExecutionPlan(statement, aggregator, limits, comparator, null, null, null, null, null);
    }

    public static ExecutionPlan joinedScan(List<ScanStatement> statements,
        TuplePredicate joinFilter,
        Projection joinProjection,
        ScanLimits limits, TupleComparator comparator) {
        return new ExecutionPlan(null, null, limits, comparator, null, statements, joinFilter, joinProjection, null);
    }

    public static ExecutionPlan dataManipulationFromSelect(DMLStatement statement, ExecutionPlan dataSource) {
        return new ExecutionPlan(statement, null, null, null, dataSource, null, null, null, null);
    }

    @Override
    public String toString() {
        return "Plan" + id;
    }

}
