/**
 * Copyright 2016 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.core.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import com.srotya.sidewinder.core.operators.AndOperator;
import com.srotya.sidewinder.core.operators.GreaterThan;
import com.srotya.sidewinder.core.operators.GreaterThanEquals;
import com.srotya.sidewinder.core.operators.LessThan;
import com.srotya.sidewinder.core.operators.LessThanEquals;
import com.srotya.sidewinder.core.operators.NumericEquals;
import com.srotya.sidewinder.core.operators.Operator;
import com.srotya.sidewinder.core.operators.OrOperator;

/**
 * @author ambud
 */
public class SidewinderTable extends AbstractQueryableTable implements TranslatableTable {

	private RelDataType types;
	private AbstractStorageEngine engine;
	private String seriesName;

	public SidewinderTable(String seriesName, AbstractStorageEngine engine) {
		super(Object[].class);
		this.seriesName = seriesName;
		this.engine = engine;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		types = typeFactory.builder().add("timestamp", SqlTypeName.TIMESTAMP).add("value", SqlTypeName.DOUBLE)
				.add("tags", SqlTypeName.MAP).build();
		return types;
	}

	@Override
	public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * @Override public Enumerable<Object[]> scan(DataContext root,
	 * List<RexNode> filters) { // TODO Auto-generated method stub
	 * List<Operator<Long>> timeSeriesOperators = new ArrayList<>(); for
	 * (Iterator<RexNode> iterator = filters.iterator(); iterator.hasNext();) {
	 * RexNode filter = iterator.next(); if (extractReductionFilter(filter,
	 * timeSeriesOperators)) { iterator.remove(); } } return null; }
	 * 
	 * public static <E> Operator<E> buildOperator(Class<E> type, RexNode
	 * filter, boolean negated) { boolean isNegated = negated; if
	 * (filter.isA(SqlKind.NOT)) { filter = ((RexCall) filter).operands.get(0);
	 * isNegated = true; } else { isNegated = false; }
	 * 
	 * final RexCall call = (RexCall) filter; if (filter.isA(SqlKind.AND) ||
	 * filter.isA(SqlKind.OR)) { List<Operator<E>> operators = new
	 * ArrayList<>(); call.operands.forEach(operand -> {
	 * operators.add(buildOperator(type, operand, false)); }); if
	 * (filter.isA(SqlKind.AND)) { return new AndOperator<>(operators); } else {
	 * return new OrOperator<>(operators); } } else { RexNode left =
	 * call.getOperands().get(0); if (left.isA(SqlKind.CAST)) { left =
	 * ((RexCall) left).operands.get(0); } if (left instanceof RexInputRef) {
	 * RexInputRef ref = (RexInputRef) left; } }
	 * 
	 * return null; }
	 * 
	 * public static Operator<Number> buildNumericOperator(RexNode filter,
	 * RexCall call) { Number value = (Number) ((RexLiteral)
	 * call.getOperands().get(1)).getValue(); boolean isFloat =
	 * value.doubleValue() == value.longValue(); switch (filter.getKind()) {
	 * case EQUALS: return new NumericEquals(isFloat, value); case GREATER_THAN:
	 * return new GreaterThan(isFloat, value); case GREATER_THAN_OR_EQUAL:
	 * return new GreaterThanEquals(isFloat, value); case LESS_THAN: return new
	 * LessThan(isFloat, value); case LESS_THAN_OR_EQUAL: return new
	 * LessThanEquals(isFloat, value); default: return null; } }
	 * 
	 * public static boolean extractReductionFilter(RexNode filter,
	 * List<Operator<Long>> timeSeriesOperators) { // Extract comparison filter
	 * boolean negateCondition = false; if (filter.isA(SqlKind.NOT)) { filter =
	 * ((RexCall) filter).operands.get(0); } if (filter.isA(SqlKind.COMPARISON))
	 * { final RexCall call = (RexCall) filter; RexNode left =
	 * call.getOperands().get(0); if (left.isA(SqlKind.CAST)) { left =
	 * ((RexCall) left).operands.get(0); } if (left instanceof RexInputRef) {
	 * RexInputRef ref = (RexInputRef) left; if
	 * (ref.getName().equalsIgnoreCase("timestamp")) { // time based filter if
	 * (call.getOperands().get(1) instanceof RexLiteral) { switch
	 * (filter.getKind()) { case EQUALS: break; case GREATER_THAN: break; case
	 * GREATER_THAN_OR_EQUAL: break; case LESS_THAN: break; case
	 * LESS_THAN_OR_EQUAL: break; } } else if (call.getOperands().get(1)
	 * instanceof RexCall) {
	 * 
	 * } else { // unknown operator } } else if
	 * (ref.getName().equalsIgnoreCase("value")) { // value filtering } else if
	 * (filter.isA(SqlKind.IN)) { // for tag filtering } else {
	 * 
	 * } } } else if (filter.isA(SqlKind.BETWEEN)) { final RexCall call =
	 * (RexCall) filter; RexNode left = call.getOperands().get(0); if
	 * (left.isA(SqlKind.CAST)) { left = ((RexCall) left).operands.get(0); } if
	 * (left instanceof RexInputRef) { RexInputRef ref = (RexInputRef) left; if
	 * (ref.getName().equalsIgnoreCase("timestamp")) { // time based filter if
	 * (call.getOperands().get(SqlBetweenOperator.VALUE_OPERAND) instanceof
	 * RexLiteral) {
	 * 
	 * } else if (call.getOperands().get(SqlBetweenOperator.VALUE_OPERAND)
	 * instanceof RexCall) {
	 * 
	 * } else { // unknown operator } } else if
	 * (ref.getName().equalsIgnoreCase("value")) { // value filtering } else {
	 * // invalid filtering case } } } return false; }
	 * 
	 * public class SeriesEnumerator implements Enumerator<Object[]> {
	 * 
	 * @Override public void close() { // TODO Auto-generated method stub
	 * 
	 * }
	 * 
	 * @Override public Object[] current() { return null; }
	 * 
	 * @Override public boolean moveNext() { return false; }
	 * 
	 * @Override public void reset() { // TODO Auto-generated method stub
	 * 
	 * }
	 * 
	 * }
	 */

}
