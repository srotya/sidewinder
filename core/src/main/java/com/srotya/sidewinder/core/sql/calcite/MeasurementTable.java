/**
 * Copyright 2017 Ambud Sharma
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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

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
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.gorilla.MemStorageEngine;
import com.srotya.sidewinder.core.storage.gorilla.Reader;

/**
 * @author ambud
 */
public class MeasurementTable extends AbstractTable implements FilterableTable {

	private MemStorageEngine engine;
	private String fieldName;
	private boolean isFp;
	private String measurementName;
	private String dbName;
	private List<RelDataType> types;
	private List<String> names;

	public MeasurementTable(MemStorageEngine engine, String dbName, String measurementName, String fieldName,
			boolean isFp) {
		this.engine = engine;
		this.dbName = dbName;
		this.measurementName = measurementName;
		this.fieldName = fieldName;
		this.isFp = isFp;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		types = new ArrayList<>();
		names = new ArrayList<>();

		// value field
		if (isFp) {
			names.add(measurementName + "_" + fieldName);
			types.add(typeFactory.createSqlType(SqlTypeName.DOUBLE, 10));
		} else {
			names.add(measurementName + "_" + fieldName);
			types.add(typeFactory.createSqlType(SqlTypeName.INTEGER, 10));
		}

		names.add("time_stamp");
		types.add(typeFactory.createSqlType(SqlTypeName.TIMESTAMP));

		names.add("tags");
		types.add(typeFactory.createSqlType(SqlTypeName.CHAR));

		return typeFactory.createStructType(types, names);
	}

	@Override
	public TableType getJdbcTableType() {
		return TableType.TABLE;
	}

	@Override
	public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
		// final String[] filterValues = new String[types.size()];
		System.err.println("Filters:" + filters);
		Entry<Long, Long> findTimeRange = null;
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
			findTimeRange = new AbstractMap.SimpleEntry<Long, Long>(System.currentTimeMillis() - (3600 * 1000),
					System.currentTimeMillis());
		} else if (findTimeRange.getKey().equals(findTimeRange.getValue())) {
			findTimeRange = new AbstractMap.SimpleEntry<Long, Long>(findTimeRange.getKey(),
					System.currentTimeMillis());
		}

		final Entry<Long, Long> range = findTimeRange;

		return new AbstractEnumerable<Object[]>() {
			public Enumerator<Object[]> enumerator() {
				return new Enumerator<Object[]>() {

					private List<Reader> readers;
					private int i;
					private DataPoint dataPoint;

					@Override
					public void reset() {
					}

					@Override
					public boolean moveNext() {
						if (readers == null) {
							try {
								readers = new ArrayList<>();
								readers.addAll(engine.queryReaders(dbName, measurementName, fieldName,
										range.getKey(), range.getValue()));
								System.err.println("Received readers:" + readers);
							} catch (Exception e) {
								e.printStackTrace();
								return false;
							}
						}
						if (i < readers.size()) {
							try {
								dataPoint = readers.get(i).readPair();
							} catch (IOException e) {
								i++;
								if (i < readers.size()) {
									return true;
								} else {
									return false;
								}
							}
							return true;
						} else {
							return false;
						}
					}

					@Override
					public Object[] current() {
						return new Object[] { dataPoint.getValue(), dataPoint.getTimestamp(), dataPoint.getTags().toString() };
					}

					@Override
					public void close() {
						readers = null;
					}
				};
			}
		};
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
				if (names.get(((RexInputRef) left).getIndex()).equals("time_stamp")) {
					// resolve the function
				}
			}

			if (!names.get(((RexInputRef) left).getIndex()).equals("time_stamp")) {
				return null;
			}

			long val = 10;
			if (right.isA(SqlKind.CAST)) {
				right = ((RexCall) right).operands.get(0);
			} else if (right.isA(SqlKind.FUNCTION)) {
				if (((RexCall) right).operands.size() > 0) {
					right = ((RexCall) right).operands.get(0);
					val = (long) ((RexLiteral) right).getValue2();
				} else {
					System.err.println("Funtion:" + ((RexCall) right).op.getName() + "\t" + filter.getKind());
					if (((RexCall) right).op.getName().equals("now")) {
						val = System.currentTimeMillis();
					}
				}
			}
			if (filter.isA(Arrays.asList(SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL))) {
				return new AbstractMap.SimpleEntry<Long, Long>(val, val);
			} else if (filter.isA(Arrays.asList(SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL))) {
				return new AbstractMap.SimpleEntry<Long, Long>(Long.MAX_VALUE, val);
			} else {
				return new AbstractMap.SimpleEntry<Long, Long>(Long.MAX_VALUE, 0L);
			}
		}
	}

	@SuppressWarnings("unused")
	private static boolean addFilter(RexNode filter, Object[] filterValues) {
		if (filter.isA(SqlKind.EQUALS)) {
			final RexCall call = (RexCall) filter;
			RexNode left = call.getOperands().get(0);
			if (left.isA(SqlKind.CAST)) {
				left = ((RexCall) left).operands.get(0);
			}
			final RexNode right = call.getOperands().get(1);
			if (left instanceof RexInputRef && right instanceof RexLiteral) {
				final int index = ((RexInputRef) left).getIndex();
				if (filterValues[index] == null) {
					filterValues[index] = ((RexLiteral) right).getValue2().toString();
					return true;
				}
			}
		}
		return false;
	}

}