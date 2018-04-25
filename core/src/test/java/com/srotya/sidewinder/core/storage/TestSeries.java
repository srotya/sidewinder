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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.srotya.sidewinder.core.predicates.GreaterThanEqualsPredicate;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.compression.CompressionFactory;

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
		assertNotNull(series.getLock());
		assertNotNull(series.getReadLock());
		assertNotNull(series.getWriteLock());
		assertNotNull(series.getBucketMap());
		assertNotNull(series.getFieldTypeMap());
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
			assertEquals("Failed:" + i + " " + d + " " + ((ts + d * 1000) - dps.get(i).getTimestamp()), ts + d * 1000,
					dps.get(i).getTimestamp());
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
	}

}
