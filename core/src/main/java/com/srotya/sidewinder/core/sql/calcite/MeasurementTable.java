/**
 * Copyright Ambud Sharma
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
package com.srotya.sidewinder.core.sql.calcite;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class MeasurementTable extends AbstractTable implements ProjectableFilterableTable {

	private static final String TAGS = "TAGS";
	StorageEngine engine;
	private List<String> fieldNames;
	String measurementName;
	String dbName;
	private List<RelDataType> types;
	private List<String> names;
	private Set<String> tagKeys;

	public MeasurementTable(StorageEngine engine, String dbName, String measurementName, Collection<String> fieldNames,
			Set<String> tagKeys) {
		this.engine = engine;
		this.dbName = dbName;
		this.measurementName = measurementName;
		this.tagKeys = tagKeys;
		this.fieldNames = new ArrayList<>(fieldNames);
		this.fieldNames.addAll(tagKeys);
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		types = new ArrayList<>();
		names = new ArrayList<>();

		for (String field : fieldNames) {
			names.add(field.toUpperCase());
			if (tagKeys.contains(field)) {
				types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
			} else {
				// value field; should be cast by the query to different type
				types.add(typeFactory.createSqlType(SqlTypeName.BIGINT));
			}
		}

		names.add(TAGS);
		types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));

		return typeFactory.createStructType(types, names);
	}

	@Override
	public TableType getJdbcTableType() {
		return TableType.TABLE;
	}

	@Override
	public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
		List<String> fields;
		if (projects != null) {
			fields = new ArrayList<>();
			for (int i = 0; i < projects.length; i++) {
				fields.add(fieldNames.get(projects[i]));
			}
		} else {
			fields = fieldNames;
		}

		Entry<Long, Long> findTimeRange = null;
		findTimeRange = extractAndBuildTimeRangeFilter(filters, findTimeRange);

		final Entry<Long, Long> range = findTimeRange;
		// System.out.println("Range filer:" + range.getKey() + " " + range.getValue());

		return new AbstractEnumerable<Object[]>() {
			public Enumerator<Object[]> enumerator() {
				return new EnumeratorImplementation(MeasurementTable.this, range, fields);
			}
		};
	}

	private Entry<Long, Long> extractAndBuildTimeRangeFilter(List<RexNode> filters, Entry<Long, Long> findTimeRange) {
		for (RexNode filter : filters) {
			Entry<Long, Long> temp = findTimeRange(filter);
			if (temp != null) {
				findTimeRange = temp;
				if (findTimeRange.getKey() > findTimeRange.getValue()) {
					// swap
					findTimeRange = new AbstractMap.SimpleEntry<Long, Long>(findTimeRange.getValue(),
							findTimeRange.getKey());
				}
			}
			System.err.println("Time range filter:" + findTimeRange);
		}

		if (findTimeRange == null) {
			// by default get last 1hr's data if no time range filter is
			// specified
			// System.currentTimeMillis() - (3600_000)
			findTimeRange = new AbstractMap.SimpleEntry<Long, Long>(0L, System.currentTimeMillis() + 10000);
		} else if (findTimeRange.getKey().equals(findTimeRange.getValue())) {
			findTimeRange = new AbstractMap.SimpleEntry<Long, Long>(findTimeRange.getKey(), System.currentTimeMillis());
		}
		return findTimeRange;
	}

	private Entry<Long, Long> findTimeRange(RexNode filter) {
		List<Entry<Long, Long>> vals = new ArrayList<>();
		if (filter.isA(SqlKind.AND) || filter.isA(SqlKind.OR)) {
			RexCall call = (RexCall) filter;
			for (RexNode rexNode : call.getOperands()) {
				Entry<Long, Long> val = findTimeRange(rexNode);
				if (val != null) {
					vals.add(val);
				}
			}
			if (vals.size() > 0) {
				long min = Long.MAX_VALUE, max = 1;
				for (Entry<Long, Long> val : vals) {
					if (min > val.getKey()) {
						System.err.println("Swapping min value:" + min + "\t" + val.getValue());
						min = val.getKey();
					}
					if (max < val.getValue()) {
						System.err.println("Swapping max value:" + max + "\t" + val.getValue());
						max = val.getValue();
					}
				}
				return new AbstractMap.SimpleEntry<Long, Long>(min, max);
			} else {
				return null;
			}
		} else {
			RexCall call = ((RexCall) filter);
			RexNode left = call.getOperands().get(0);
			RexNode right = call.getOperands().get(call.getOperands().size() - 1);
			if (left.isA(SqlKind.CAST)) {
				left = ((RexCall) left).operands.get(0);
			} else if (left.isA(SqlKind.FUNCTION)) {
				left = ((RexCall) left).operands.get(0);
				if (names.get(((RexInputRef) left).getIndex()).equals(Series.TS)) {
					// resolve the function
				}
			}

			// only timestamp field is filtered
			if (!names.get(((RexInputRef) left).getIndex()).equals(Series.TS)) {
				return null;
			}

			long val = 10;
			if (right.isA(SqlKind.CAST)) {
				right = ((RexCall) right).operands.get(0);
			} else if (right.isA(SqlKind.FUNCTION)) {
				if (((RexCall) right).operands.size() > 0) {
					right = ((RexCall) right).operands.get(0);
					val = (long) ((RexLiteral) right).getValue2();
					System.out.println("\n\nFunction parsed \n\n");
				} else {
					System.err.println("Funtion:" + ((RexCall) right).op.getName() + "\t" + filter.getKind());
					if (((RexCall) right).op.getName().equals("now")) {
						val = System.currentTimeMillis();
					}
				}
			} else {
				@SuppressWarnings("rawtypes")
				Comparable value = ((RexLiteral) right).getValue();
				if (value instanceof Number) {
					val = ((Number) value).longValue();
				}
			}
			if (filter.isA(Arrays.asList(SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL))) {
				return new AbstractMap.SimpleEntry<Long, Long>(val, Long.MAX_VALUE);
			} else if (filter.isA(Arrays.asList(SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL))) {
				return new AbstractMap.SimpleEntry<Long, Long>(0L, val);
			} else {
				return new AbstractMap.SimpleEntry<Long, Long>(Long.MIN_VALUE, Long.MAX_VALUE);
			}
		}
	}

	// @SuppressWarnings("unused")
	// private static boolean addFilter(RexNode filter, Object[] filterValues) {
	// if (filter.isA(SqlKind.EQUALS)) {
	// final RexCall call = (RexCall) filter;
	// RexNode left = call.getOperands().get(0);
	// if (left.isA(SqlKind.CAST)) {
	// left = ((RexCall) left).operands.get(0);
	// }
	// final RexNode right = call.getOperands().get(1);
	// if (left instanceof RexInputRef && right instanceof RexLiteral) {
	// final int index = ((RexInputRef) left).getIndex();
	// if (filterValues[index] == null) {
	// filterValues[index] = ((RexLiteral) right).getValue2().toString();
	// return true;
	// }
	// }
	// }
	// return false;
	// }

}