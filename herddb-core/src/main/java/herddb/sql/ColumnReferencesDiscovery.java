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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Discover main table alias
 *
 * @author enrico.olivelli
 */
public class ColumnReferencesDiscovery implements ExpressionVisitor, ItemsListVisitor {

    private String mainTableAlias;
    private boolean containsMixedAliases;
    private final Map<String, List<Column>> columnsByTable = new HashMap<>();
    private final Expression expression;

    public Expression getExpression() {
        return expression;
    }

    public Map<String, List<Column>> getColumnsByTable() {
        return columnsByTable;
    }

    public String getMainTableAlias() {
        return mainTableAlias;
    }

    public boolean isContainsMixedAliases() {
        return containsMixedAliases;
    }

    private void accumulate(Column column) {
        String tableName;
        Table fromTable = column.getTable();
        if (fromTable == null || fromTable.getName() == null) {
            if (mainTableAlias == null) {
                throw new IllegalArgumentException("you have to full qualify column names, ambiguos column is " + column);
            }
            tableName = mainTableAlias;
        } else {
            tableName = fromTable.getName();
        }
        String tableAlias = tableName;
        if (fromTable.getAlias() != null && fromTable.getAlias().getName() != null) {
            tableAlias = fromTable.getAlias().getName();
        }
        if (mainTableAlias == null) {
            mainTableAlias = tableAlias;
        } else if (!mainTableAlias.equals(tableAlias)) {
            containsMixedAliases = true;
            mainTableAlias = null;
        }
        List<Column> columnsForTable = columnsByTable.get(tableAlias);
        if (columnsForTable == null) {
            columnsForTable = new ArrayList<>();
            columnsByTable.put(tableAlias, columnsForTable);
        }
        columnsForTable.add(column);
    }

    @Override
    public void visit(NullValue nv) {

    }

    @Override
    public void visit(Function fnctn) {
        if (fnctn.getParameters() != null && fnctn.getParameters().getExpressions() != null) {
            fnctn.getParameters().getExpressions().forEach(e -> e.accept(this));
        }
    }

    @Override
    public void visit(SignedExpression se) {
        se.getExpression().accept(this);
    }

    @Override
    public void visit(JdbcParameter jp) {
    }

    @Override
    public void visit(JdbcNamedParameter jnp) {

    }

    @Override
    public void visit(DoubleValue dv) {

    }

    @Override
    public void visit(LongValue lv) {

    }

    @Override
    public void visit(HexValue hv) {

    }

    @Override
    public void visit(DateValue dv) {

    }

    @Override
    public void visit(TimeValue tv) {

    }

    @Override
    public void visit(TimestampValue tv) {

    }

    @Override
    public void visit(Parenthesis prnths) {
        prnths.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue sv) {

    }

    private void acceptBinaryExpression(BinaryExpression b) {
        b.getLeftExpression().accept(this);
        b.getRightExpression().accept(this);
    }

    @Override
    public void visit(Addition adtn) {
        acceptBinaryExpression(adtn);
    }

    @Override
    public void visit(Division dvsn) {
        acceptBinaryExpression(dvsn);
    }

    @Override
    public void visit(Multiplication m) {
        acceptBinaryExpression(m);
    }

    @Override
    public void visit(Subtraction s) {
        acceptBinaryExpression(s);
    }

    @Override
    public void visit(AndExpression ae) {
        acceptBinaryExpression(ae);
    }

    public ColumnReferencesDiscovery(Expression expression) {
        this.expression = expression;
    }

    public ColumnReferencesDiscovery(Expression expression, String mainTableAlias) {
        this.expression = expression;
        this.mainTableAlias = mainTableAlias;
    }

    @Override
    public void visit(OrExpression oe) {
        acceptBinaryExpression(oe);
    }

    @Override
    public void visit(Between btwn) {
        btwn.getLeftExpression().accept(this);
        btwn.getBetweenExpressionStart().accept(this);
        btwn.getBetweenExpressionEnd().accept(this);
    }

    @Override
    public void visit(EqualsTo et) {
        acceptBinaryExpression(et);
    }

    @Override
    public void visit(GreaterThan gt) {
        acceptBinaryExpression(gt);
    }

    @Override
    public void visit(GreaterThanEquals gte) {
        acceptBinaryExpression(gte);
    }

    @Override
    public void visit(InExpression ie) {
        ie.getLeftExpression().accept(this);
        ie.getLeftItemsList().accept(this);
        ie.getRightItemsList().accept(this);
    }

    @Override
    public void visit(IsNullExpression ine) {
        ine.getLeftExpression().accept(this);
    }

    @Override
    public void visit(LikeExpression le) {
        acceptBinaryExpression(le);
    }

    @Override
    public void visit(MinorThan mt) {
        acceptBinaryExpression(mt);
    }

    @Override
    public void visit(MinorThanEquals mte) {
        acceptBinaryExpression(mte);
    }

    @Override
    public void visit(NotEqualsTo net) {
        acceptBinaryExpression(net);
    }

    @Override
    public void visit(Column column) {
        accumulate(column);
    }

    @Override
    public void visit(SubSelect ss) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(CaseExpression ce) {
        if (ce.getElseExpression() != null) {
            ce.getElseExpression().accept(this);
        }
        if (ce.getSwitchExpression() != null) {
            ce.getSwitchExpression().accept(this);
        }
        ce.getWhenClauses().forEach(e -> e.accept(this));
    }

    @Override
    public void visit(WhenClause wc) {
        if (wc.getThenExpression() != null) {
            wc.getThenExpression().accept(this);
        }
        if (wc.getWhenExpression() != null) {
            wc.getWhenExpression().accept(this);
        }
    }

    @Override
    public void visit(ExistsExpression ee
    ) {
        ee.getRightExpression().accept(this);
    }

    @Override
    public void visit(AllComparisonExpression ace
    ) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void visit(AnyComparisonExpression ace
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Concat concat
    ) {
        acceptBinaryExpression(concat);

    }

    @Override
    public void visit(Matches mtchs
    ) {
        acceptBinaryExpression(mtchs);
    }

    @Override
    public void visit(BitwiseAnd ba
    ) {
        acceptBinaryExpression(ba);
    }

    @Override
    public void visit(BitwiseOr bo
    ) {
        acceptBinaryExpression(bo);
    }

    @Override
    public void visit(BitwiseXor bx
    ) {
        acceptBinaryExpression(bx);
    }

    @Override
    public void visit(CastExpression ce
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(Modulo modulo
    ) {
        acceptBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression ae
    ) {

    }

    @Override
    public void visit(WithinGroupExpression wge
    ) {

    }

    @Override
    public void visit(ExtractExpression ee
    ) {

    }

    @Override
    public void visit(IntervalExpression ie
    ) {

    }

    @Override
    public void visit(OracleHierarchicalExpression ohe
    ) {

    }

    @Override
    public void visit(RegExpMatchOperator remo
    ) {
        acceptBinaryExpression(remo);
    }

    @Override
    public void visit(JsonExpression je
    ) {

    }

    @Override
    public void visit(RegExpMySQLOperator rmsql
    ) {

    }

    @Override
    public void visit(UserVariable uv
    ) {

    }

    @Override
    public void visit(NumericBind nb
    ) {

    }

    @Override
    public void visit(KeepExpression ke
    ) {

    }

    @Override
    public void visit(MySQLGroupConcat msqlgc
    ) {

    }

    @Override
    public void visit(RowConstructor rc
    ) {

    }

    @Override
    public void visit(OracleHint oh
    ) {

    }

    @Override
    public void visit(TimeKeyExpression tke
    ) {

    }

    @Override
    public void visit(DateTimeLiteralExpression dtle
    ) {

    }

    @Override
    public void visit(ExpressionList el
    ) {
        el.getExpressions().forEach(e -> e.accept(this));
    }

    @Override
    public void visit(MultiExpressionList mel
    ) {
        mel.getExprList().forEach(e -> e.accept(this));
    }

    @Override
    public void visit(JsonOperator jo) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(NotExpression ne) {
        ne.getExpression().accept(this);
    }

}
