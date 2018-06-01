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
package com.srotya.sidewinder.core.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.srotya.sidewinder.core.predicates.GreaterThanEqualsPredicate;
import com.srotya.sidewinder.core.predicates.GreaterThanPredicate;
import com.srotya.sidewinder.core.predicates.LessThanPredicate;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.compression.CompressionFactory;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;
import com.srotya.sidewinder.core.storage.compression.Writer;

public class TestSeries {

	private MockMeasurement measurement;

	@Before
	public void before() {
		measurement = new MockMeasurement(1024, 100);
		TimeField.compressionClass = CompressionFactory.getTimeClassByName("byzantine");
		ValueField.compressionClass = CompressionFactory.getValueClassByName("byzantine");
	}

	@Test
	public void testInit() throws IOException {
		Series series = new Series(new ByteString("idasdasda"), 0);
		assertNotNull(series.getSeriesId());
		assertNotNull(series.getLock());
		assertNotNull(series.getReadLock());
		assertNotNull(series.getWriteLock());
		assertNotNull(series.getBucketMap());
	}

	@Test
	public void testAddAndReadPointsAsIndividualSeries() throws IOException {
		measurement.setTimebucket(4096);
		Series series = new Series(new ByteString("idasdasda"), 0);
		long ts = 1497720652566L;
		for (int i = 0; i < 1000; i++) {
			Point dp = Point.newBuilder().setTimestamp(ts + i * 1000).addValueFieldName("f1").addFp(false).addValue(i)
					.addValueFieldName("f2").addFp(true).addValue(Double.doubleToLongBits(i * 1.1)).build();
			series.addPoint(dp, measurement);
		}

		Map<String, List<DataPoint>> query = series.queryDataPoints(measurement, Arrays.asList("f1", "f2"), 0,
				Long.MAX_VALUE, null);
		assertEquals(2, query.size());
		assertEquals(1000, query.get("f1").size());
		assertEquals(1000, query.get("f2").size());

		List<DataPoint> dps = query.get("f1");
		for (int i = 0; i < 1000; i++) {
			assertEquals(ts + i * 1000, dps.get(i).getTimestamp());
			assertEquals(i, dps.get(i).getLongValue(), 0);
		}

		dps = query.get("f2");
		for (int i = 0; i < 1000; i++) {
			assertEquals(ts + i * 1000, dps.get(i).getTimestamp());
			assertEquals(i * 1.1, dps.get(i).getValue(), 0);
		}

		// test predicates
		query = series.queryDataPoints(measurement, Arrays.asList("f1", "f2"), 0, Long.MAX_VALUE, Arrays.asList(
				new GreaterThanEqualsPredicate(12), new GreaterThanEqualsPredicate(Double.doubleToLongBits(10))));
		dps = query.get("f1");
		assertEquals(988, dps.size());
		for (int i = 0; i < 988; i++) {
			int d = 12 + i;
			assertEquals("Failed: i:" + i + " d:" + d + " t:" + ((ts + d * 1000) - dps.get(i).getTimestamp()),
					ts + d * 1000, dps.get(i).getTimestamp());
			assertEquals(d, dps.get(i).getLongValue(), 0);
		}

		dps = query.get("f2");
		assertEquals(990, dps.size());
		for (int i = 0; i < 990; i++) {
			int d = 10 + i;
			assertEquals(ts + d * 1000, dps.get(i).getTimestamp());
			assertEquals(d * 1.1, dps.get(i).getValue(), 0);
		}
	}

	@Test
	public void testAddAndReadPointsAsTuples() throws IOException {
		measurement.setTimebucket(4096);
		Series series = new Series(new ByteString("idasdasda"), 0);
		long ts = 1497720652566L;
		for (int i = 0; i < 1000; i++) {
			Point dp = Point.newBuilder().setTimestamp(ts + i * 1000).addValueFieldName("f1").addFp(false).addValue(i)
					.addValueFieldName("f2").addFp(true).addValue(Double.doubleToLongBits(i * 1.1)).build();
			series.addPoint(dp, measurement);
		}
		List<long[]> queryTuples = series.queryTuples(measurement, Arrays.asList("f1", "f2"), 0, Long.MAX_VALUE, null);
		assertEquals(1000, queryTuples.size());
		for (int i = 0; i < 1000; i++) {
			assertEquals(3, queryTuples.get(i).length);
			assertEquals(ts + i * 1000, queryTuples.get(i)[0]);
			assertEquals(i, queryTuples.get(i)[1]);
			assertEquals(Double.doubleToLongBits(i * 1.1), queryTuples.get(i)[2]);
		}

		// check field ordering
		queryTuples = series.queryTuples(measurement, Arrays.asList("f2", "f1"), 0, Long.MAX_VALUE, null);
		assertEquals(1000, queryTuples.size());
		for (int i = 0; i < 1000; i++) {
			assertEquals(3, queryTuples.get(i).length);
			assertEquals(ts + i * 1000, queryTuples.get(i)[0]);
			assertEquals(i, queryTuples.get(i)[2]);
			assertEquals(Double.doubleToLongBits(i * 1.1), queryTuples.get(i)[1]);
		}

		// check specific fields
		queryTuples = series.queryTuples(measurement, Arrays.asList("f1"), 0, Long.MAX_VALUE, null);
		assertEquals(1000, queryTuples.size());
		for (int i = 0; i < 1000; i++) {
			assertEquals(2, queryTuples.get(i).length);
			assertEquals(ts + i * 1000, queryTuples.get(i)[0]);
			assertEquals(i, queryTuples.get(i)[1]);
		}

	}

	@Test
	public void testInvalidFields() throws IOException {
		measurement.setTimebucket(4096);
		Series series = new Series(new ByteString("idasdasda"), 0);
		long ts = 1497720652566L;
		for (int i = 0; i < 1000; i++) {
			Point dp = Point.newBuilder().setTimestamp(ts + i * 1000).addValueFieldName("f1").addFp(false).addValue(i)
					.addValueFieldName("f2").addFp(true).addValue(Double.doubleToLongBits(i * 1.1)).build();
			series.addPoint(dp, measurement);
		}

		List<long[]> queryTuples = series.queryTuples(measurement, Arrays.asList("f2", "f1", "f3"), 0, Long.MAX_VALUE,
				null);
		assertEquals(1000, queryTuples.size());
		for (int i = 0; i < 1000; i++) {
			assertEquals(4, queryTuples.get(i).length);
			assertEquals(ts + i * 1000, queryTuples.get(i)[0]);
			assertEquals(i, queryTuples.get(i)[2]);
			assertEquals(Double.doubleToLongBits(i * 1.1), queryTuples.get(i)[1]);
			assertEquals(0, queryTuples.get(i)[3]);
		}

		queryTuples = series.queryTuples(measurement, Arrays.asList("f2", "f1", "f3"), 1497720652566L + 1000 * 100,
				1497720652566L + 1000 * 899, null);
		assertEquals(800, queryTuples.size());
		for (int i = 100; i < 900; i++) {
			long[] point = queryTuples.get(i - 100);
			assertEquals(4, point.length);
			assertEquals(ts + i * 1000, point[0]);
			assertEquals(i, point[2]);
			assertEquals(Double.doubleToLongBits(i * 1.1), point[1]);
			assertEquals(0, point[3]);
		}

		queryTuples = series.queryTuples(measurement, Arrays.asList("f2", "f1", "f3"), 1497720652566L + 1000 * 100,
				1497720652566L + 1000 * 899, Arrays.asList(null, new GreaterThanEqualsPredicate(200), null));
		assertEquals(700, queryTuples.size());
		for (int i = 200; i < 900; i++) {
			long[] point = queryTuples.get(i - 200);
			assertEquals(4, point.length);
			assertEquals(ts + i * 1000, point[0]);
			assertEquals(i, point[2]);
			assertEquals(Double.doubleToLongBits(i * 1.1), point[1]);
			assertEquals(0, point[3]);
		}
	}

	@Test
	public void testTimeFilteredReads() throws IOException {
		measurement.setTimebucket(1024);
		Series series = new Series(new ByteString("idasdasda"), 0);
		long ts = 1497720652566L;
		for (int i = 0; i < 1000; i++) {
			Point dp = Point.newBuilder().setTimestamp(ts + i * 1000).addValueFieldName("f1").addFp(false).addValue(i)
					.addValueFieldName("f2").addFp(true).addValue(Double.doubleToLongBits(i * 1.1)).build();
			series.addPoint(dp, measurement);
		}

		List<DataPoint> list = series
				.queryDataPoints(measurement, Arrays.asList("f1"), 1497720652566L + 1000 * 10, Long.MAX_VALUE, null)
				.get("f1");
		assertEquals(990, list.size());
		for (int i = 10; i < 1000; i++) {
			long expected = ts + i * 1000;
			long timestamp = list.get(i - 10).getTimestamp();
			assertEquals("Delta:" + (expected - timestamp) + " i:" + i, expected, timestamp);
		}

		list = series.queryDataPoints(measurement, Arrays.asList("f1"), 1497720652566L + 1000 * 100,
				1497720652566L + 1000 * 899, null).get("f1");
		assertEquals(800, list.size());
		for (int i = 100; i < 900; i++) {
			long expected = ts + i * 1000;
			long timestamp = list.get(i - 100).getTimestamp();
			assertEquals("Delta:" + (expected - timestamp) + " i:" + i, expected, timestamp);
		}
	}

	@Test
	public void testIteratorTypes() throws IOException {
		measurement.setTimebucket(1024);
		Series series = new Series(new ByteString("idasdasda"), 0);
		long ts = 1497720652566L;
		for (int i = 0; i < 1000; i++) {
			Point dp = Point.newBuilder().setTimestamp(ts + i * 1000).addValueFieldName("f1").addFp(false).addValue(i)
					.addValueFieldName("f2").addFp(true).addValue(Double.doubleToLongBits(i * 1.1)).build();
			series.addPoint(dp, measurement);
		}

		Map<String, List<DataPoint>> queryDataPoints = series.queryDataPoints(measurement, Arrays.asList("f1", "f2"), 0,
				Long.MAX_VALUE, null);
		assertEquals(2, queryDataPoints.size());
		for (Entry<String, List<DataPoint>> entry : queryDataPoints.entrySet()) {
			List<DataPoint> value = entry.getValue();
			assertEquals(1000, value.size());
		}

		for (int i = 0; i < 1000; i++) {
			assertEquals(ts + i * 1000, queryDataPoints.get("f2").get(i).getTimestamp());
			assertEquals(Double.doubleToLongBits(i * 1.1), queryDataPoints.get("f2").get(i).getLongValue());
		}

		for (int i = 0; i < 1000; i++) {
			assertEquals(ts + i * 1000, queryDataPoints.get("f1").get(i).getTimestamp());
			assertEquals(i, queryDataPoints.get("f1").get(i).getLongValue());
		}

		List<long[]> queryTuples = series.queryTuples(measurement, Arrays.asList("f2", "f1", "f3"), 0, Long.MAX_VALUE,
				null);
		assertEquals(1000, queryTuples.size());
		for (int i = 0; i < 1000; i++) {
			assertEquals(4, queryTuples.get(i).length);
			assertEquals(ts + i * 1000, queryTuples.get(i)[0]);
			assertEquals(i, queryTuples.get(i)[2]);
			assertEquals(Double.doubleToLongBits(i * 1.1), queryTuples.get(i)[1]);
			assertEquals(0, queryTuples.get(i)[3]);
		}

		FieldReaderIterator[] queryTupleReaders = series.queryIterators(measurement, Arrays.asList("f2", "f1"), 0,
				Long.MAX_VALUE);
		assertEquals(3, queryTupleReaders.length);
		for (int i = 0; i < 1000; i++) {
			assertEquals(ts + i * 1000, queryTupleReaders[2].next());
			assertEquals(i, queryTupleReaders[1].next());
		}

		queryTupleReaders = series.queryIterators(measurement, Arrays.asList("f2", "f1", "TS"), 0, Long.MAX_VALUE);
		assertEquals(3, queryTupleReaders.length);
		for (int i = 0; i < 1000; i++) {
			assertEquals(ts + i * 1000, queryTupleReaders[2].next());
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testIterator() throws IOException {
		measurement.setTimebucket(1024);
		Series series = new Series(new ByteString("idasdasda"), 0);
		long ts = 1497720652566L;
		for (int i = 0; i < 10000; i++) {
			Point dp = Point.newBuilder().setTimestamp(ts + i * 200).addValueFieldName("f1").addFp(false).addValue(i)
					.addValueFieldName("f2").addFp(true).addValue(Double.doubleToLongBits(i * 1.1)).build();
			series.addPoint(dp, measurement);
		}
		// check time buckets
		assertEquals(3, series.getBucketMap().size());
		// query iterators
		FieldReaderIterator[] queryIterators = series.queryIterators(measurement, Arrays.asList("f1", "f2"),
				Long.MAX_VALUE, Long.MIN_VALUE);
		assertEquals(3, queryIterators.length);

		// must respond even when there is nothing selectable in time range
		queryIterators = series.queryIterators(measurement, Arrays.asList("f1", "f2"), Long.MAX_VALUE, Long.MAX_VALUE);
		assertEquals(3, queryIterators.length);

		// must respond even when there is nothing selectable in time range
		queryIterators = series.queryIterators(measurement, Arrays.asList("f1", "TS"), Long.MAX_VALUE, Long.MAX_VALUE);
		assertEquals(2, queryIterators.length);

		// no fields should result in no iterators
		queryIterators = series.queryIterators(measurement, Arrays.asList(), Long.MAX_VALUE, Long.MAX_VALUE);
		assertEquals(0, queryIterators.length);

		final List<Writer> compactedWriters = series.compact(measurement);
		assertTrue(compactedWriters.size() > 0);
	}

	@Test
	public void testIteratorPredicates() throws IOException {
		measurement.setTimebucket(1024);
		Series series = new Series(new ByteString("idasdasda"), 0);
		long ts = 1497720652566L;
		for (int i = 0; i < 10000; i++) {
			Point dp = Point.newBuilder().setTimestamp(ts + i * 200).addValueFieldName("f1").addFp(false).addValue(i)
					.addValueFieldName("f2").addFp(false).addValue(i).build();
			series.addPoint(dp, measurement);
		}

		FieldReaderIterator[] queryIterators = series.queryIterators(measurement, Arrays.asList("f1", "f2"),
				Arrays.asList(new GreaterThanPredicate(100), null), Long.MIN_VALUE, Long.MAX_VALUE);
		assertEquals(3, queryIterators.length);
		for (int i = 0; i < 10000; i++) {
			if (i <= 100) {
				try {
					FieldReaderIterator.extracted(queryIterators);
					fail(i + " must throw filtered value exception since predicate is being used");
				} catch (FilteredValueException e) {
				}
			} else {
				try {
					long[] extracted = FieldReaderIterator.extracted(queryIterators);
					assertEquals(ts + i * 200, extracted[2]);
					assertEquals(i, extracted[0]);
					assertEquals(i, extracted[1]);
				} catch (FilteredValueException e) {
					fail(i + " must NOT throw filtered value exception since predicate is being used");
				}
			}
		}

		queryIterators = series.queryIterators(measurement, Arrays.asList("f1", "f2"),
				Arrays.asList(new GreaterThanPredicate(100), new LessThanPredicate(110)), 0, Long.MAX_VALUE - 1000000);
		assertEquals(3, queryIterators.length);
		for (int i = 0; i < 10000; i++) {
			if (i <= 100 || i >= 110) {
				try {
					FieldReaderIterator.extracted(queryIterators);
					fail(i + " must throw filtered value exception since predicate is being used");
				} catch (FilteredValueException e) {
				}
			} else {
				try {
					long[] extracted = FieldReaderIterator.extracted(queryIterators);
					assertEquals(ts + i * 200, extracted[2]);
					assertEquals(i, extracted[0]);
					assertEquals(i, extracted[1]);
				} catch (FilteredValueException e) {
					fail(i + " must NOT throw filtered value exception since predicate is being used");
				}
			}
		}
	}
}
