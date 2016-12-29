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
import java.util.List;

/**
 * Data access execution plan.
 *
 * @author enrico.olivelli
 */
public class ExecutionPlan {

    public final Statement mainStatement;
    public final Aggregator mainAggregator;
    public final ScanLimits limits;
    public final TupleComparator comparator;
    public final List<ScanStatement> joinStatements;
    public final TuplePredicate joinFilter;
    public final Projection joinProjection;
    public final ExecutionPlan dataSource;

    private ExecutionPlan(Statement mainStatement,
        Aggregator mainAggregator,
        ScanLimits limits,
        TupleComparator comparator,
        ExecutionPlan dataSource,
        List<ScanStatement> joinStatements,
        TuplePredicate joinFilter,
        Projection joinProjection) {
        this.mainStatement = mainStatement;
        this.mainAggregator = mainAggregator;
        this.limits = limits;
        this.comparator = comparator;
        this.dataSource = dataSource;
        this.joinStatements = joinStatements;
        this.joinFilter = joinFilter;
        this.joinProjection = joinProjection;
    }

    public static ExecutionPlan simple(Statement statement) {
        return new ExecutionPlan(statement, null, null, null, null, null, null, null);
    }

    public static ExecutionPlan make(Statement statement, Aggregator aggregator, ScanLimits limits, TupleComparator comparator) {
        return new ExecutionPlan(statement, aggregator, limits, comparator, null, null, null, null);
    }

    public static ExecutionPlan joinedScan(List<ScanStatement> statements,
        TuplePredicate joinFilter,
        Projection joinProjection,
        ScanLimits limits, TupleComparator comparator) {
        return new ExecutionPlan(null, null, limits, comparator, null, statements, joinFilter, joinProjection);
    }

    public static ExecutionPlan dataManipulationFromSelect(DMLStatement statement, ExecutionPlan dataSource) {
        return new ExecutionPlan(statement, null, null, null, dataSource, null, null, null);
    }

    @Override
    public String toString() {
        return "ExecutionPlan{" + "mainStatement=" + mainStatement + ", mainAggregator=" + mainAggregator + ", limits=" + limits + ", comparator=" + comparator + ", joinStatements=" + joinStatements + ", joinFilter=" + joinFilter + ", joinProjection=" + joinProjection + ", dataSource=" + dataSource + '}';
    }

}
