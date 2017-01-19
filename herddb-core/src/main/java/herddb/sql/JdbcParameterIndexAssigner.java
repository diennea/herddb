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

import herddb.utils.IntHolder;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
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
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

/**
 * Assigns an index to every JdbcParameter
 *
 * @author enrico.olivelli
 */
class JdbcParameterIndexAssigner implements StatementVisitor,
    SelectVisitor,
    OrderByVisitor,
    ExpressionVisitor,
    SelectItemVisitor,
    ItemsListVisitor,
    FromItemVisitor {

    private final IntHolder currentPosition = new IntHolder(0);

    JdbcParameterIndexAssigner() {
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        if (jdbcParameter.getIndex() == null) {
            int pos = currentPosition.value++;
            jdbcParameter.setIndex(pos);
        }
    }

    private void visitLimit(Limit limit) {
        if (limit.isOffsetJdbcParameter()) {
            currentPosition.value++;
        }
        if (limit.isRowCountJdbcParameter()) {
            currentPosition.value++;
        }
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Select select) {
        if (select.getSelectBody() != null) {
            select.getSelectBody().accept(this);
        }
        if (select.getWithItemsList() != null) {
            select.getWithItemsList().forEach(
                i -> {
                    if (i != null) {
                        i.accept(this);
                    }
                }
            );
        }
    }

    @Override
    public void visit(Delete s) {
        if (s.getOrderByElements() != null) {
            s.getOrderByElements().forEach(o -> {
                o.accept(this);
            });
        }
        if (s.getWhere() != null) {
            s.getWhere().accept(this);
        }
    }

    @Override
    public void visit(Update s) {
        if (s.getColumns() != null) {
            s.getColumns().forEach(c -> {
                c.accept(this);
            });
        }
        if (s.getExpressions() != null) {
            s.getExpressions().forEach(e -> e.accept(this));
        }
        if (s.getOrderByElements() != null) {
            s.getOrderByElements().forEach(o -> {
                o.accept(this);
            });
        }
        if (s.getWhere() != null) {
            s.getWhere().accept(this);
        }
        if (s.getReturningExpressionList() != null) {
            s.getReturningExpressionList().forEach(o -> {
                o.accept(this);
            });

        }
    }

    @Override
    public void visit(Insert s) {
        if (s.getColumns() != null) {
            s.getColumns().forEach(c -> {
                c.accept(this);
            });
        }
        if (s.getItemsList() != null) {
            s.getItemsList().accept(this);
        }
        if (s.getSelect() != null) {
            s.getSelect().accept(this);
        }
        if (s.getReturningExpressionList() != null) {
            s.getReturningExpressionList().forEach(o -> {
                o.accept(this);
            });
        }
    }

    @Override
    public void visit(Replace replace) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Drop drop) {

    }

    @Override
    public void visit(Truncate truncate) {

    }

    @Override
    public void visit(CreateIndex createIndex) {

    }

    @Override
    public void visit(CreateTable createTable) {

    }

    @Override
    public void visit(CreateView createView) {

    }

    @Override
    public void visit(AlterView alterView) {

    }

    @Override
    public void visit(Alter alter) {

    }

    @Override
    public void visit(Statements stmts) {
        stmts.getStatements().forEach(s -> s.accept(this));
    }

    @Override
    public void visit(Execute execute) {
        if (execute.getExprList() != null) {
            execute.getExprList().accept(this);
        }
    }

    @Override
    public void visit(SetStatement set) {
        set.getExpression().accept(this);
    }

    @Override
    public void visit(Merge merge) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void visitJoin(Join j) {
        if (j.getOnExpression() != null) {
            j.getOnExpression().accept(this);
        };
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        if (plainSelect.getDistinct() != null && plainSelect.getDistinct().getOnSelectItems() != null) {
            plainSelect.getDistinct().getOnSelectItems().forEach(s -> s.accept(this));
        }
        if (plainSelect.getFromItem() != null) {
            plainSelect.getFromItem().accept(this);
        }
        if (plainSelect.getGroupByColumnReferences() != null) {
            plainSelect.getGroupByColumnReferences().forEach(s -> s.accept(this));
        }
        if (plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(this);
        }
        if (plainSelect.getJoins() != null) {
            plainSelect.getJoins().forEach(
                j -> {
                    visitJoin(j);
                }
            );
        }
        if (plainSelect.getOrderByElements() != null) {
            plainSelect.getOrderByElements().forEach(s -> s.accept(this));
        }
        if (plainSelect.getSelectItems() != null) {
            plainSelect.getSelectItems().forEach(s -> s.accept(this));
        }
        if (plainSelect.getTop() != null) {
            plainSelect.getTop().getExpression().accept(this);
        }
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }
        if (plainSelect.getLimit() != null) {
            visitLimit(plainSelect.getLimit());
        }

    }

    @Override
    public void visit(SetOperationList setOpList) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(WithItem withItem) {
        if (withItem.getSelectBody() != null) {
            withItem.getSelectBody().accept(this);
        }
        if (withItem.getWithItemList() != null) {
            withItem.getWithItemList().forEach(s -> s.accept(this));
        }
    }

    @Override
    public void visit(OrderByElement orderBy) {
        if (orderBy.getExpression() != null) {
            orderBy.getExpression().accept(this);
        }
    }

    @Override
    public void visit(NullValue nullValue) {

    }

    @Override
    public void visit(Function function) {
        if (function.getParameters() != null) {
            function.getParameters().accept(this);
        }
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        signedExpression.getExpression().accept(this);
    }

    @Override
    public void visit(DoubleValue doubleValue) {

    }

    @Override
    public void visit(LongValue longValue) {

    }

    @Override
    public void visit(HexValue hexValue) {

    }

    @Override
    public void visit(DateValue dateValue) {

    }

    @Override
    public void visit(TimeValue timeValue) {

    }

    @Override
    public void visit(TimestampValue timestampValue) {

    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {

    }

    private void visitBinaryExpression(BinaryExpression e) {
        e.getLeftExpression().accept(this);
        e.getRightExpression().accept(this);
    }

    @Override
    public void visit(Addition addition) {
        visitBinaryExpression(addition);
    }

    @Override
    public void visit(Division division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(Multiplication multiplication) {
        visitBinaryExpression(multiplication);
    }

    @Override
    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryExpression(andExpression);
    }

    @Override
    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    @Override
    public void visit(Between between) {
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
        between.getLeftExpression().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        inExpression.getLeftExpression().accept(this);
        if (inExpression.getLeftItemsList() != null) {
            inExpression.getLeftItemsList().accept(this);
        }
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        isNullExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    @Override
    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(Column tableColumn) {

    }

    @Override
    public void visit(SubSelect subSelect) {
        if (subSelect.getSelectBody() != null) {
            subSelect.getSelectBody().accept(this);
        }
        if (subSelect.getWithItemsList() != null) {
            subSelect.getWithItemsList().forEach(e -> e.accept(this));
        }
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        if (caseExpression.getSwitchExpression() != null) {
            caseExpression.getSwitchExpression().accept(this);
        }
        if (caseExpression.getWhenClauses() != null) {
            caseExpression.getWhenClauses().forEach(e -> e.accept(this));
        }
        if (caseExpression.getElseExpression() != null) {
            caseExpression.getElseExpression().accept(this);
        }
    }

    @Override
    public void visit(WhenClause whenClause) {
        whenClause.getWhenExpression().accept(this);
        whenClause.getThenExpression().accept(this);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        existsExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        visit(allComparisonExpression.getSubSelect());

    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        visit(anyComparisonExpression.getSubSelect());
    }

    @Override
    public void visit(Concat concat) {
        visitBinaryExpression(concat);
    }

    @Override
    public void visit(Matches matches) {
        visitBinaryExpression(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        visitBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        visitBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        visitBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(CastExpression cast) {
        cast.getLeftExpression().accept(this);
    }

    @Override
    public void visit(Modulo modulo) {
        visitBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(WithinGroupExpression wgexpr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        visitBinaryExpression(rexpr);
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(UserVariable var) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(NumericBind bind) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(KeepExpression aexpr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(OracleHint hint) {
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {

    }

    @Override
    public void visit(AllColumns allColumns) {

    }

    @Override
    public void visit(AllTableColumns allTableColumns) {

    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(this);
    }

    @Override
    public void visit(ExpressionList expressionList) {
        expressionList.getExpressions().forEach(e -> e.accept(this));
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        multiExprList.getExprList().forEach(e -> visit(e));
    }

    @Override
    public void visit(Table tableName) {
    }

    @Override
    public void visit(SubJoin subjoin) {
        subjoin.getLeft().accept(this);
        visitJoin(subjoin.getJoin());
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(ValuesList valuesList) {
        visit(valuesList.getMultiExpressionList());
    }

    @Override
    public void visit(TableFunction tableFunction) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    int getParametersCount() {
        return this.currentPosition.value;
    }

}
