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
package com.srotya.sidewinder.core.storage.gorilla;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.srotya.sidewinder.core.filters.AndFilter;
import com.srotya.sidewinder.core.filters.ContainsFilter;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.filters.OrFilter;
import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * Unit tests for {@link MemStorageEngine}
 * 
 * @author ambud
 */
public class TestMemStorageEngine {

	private Map<String, String> conf = new HashMap<>();

	@Test
	public void testWritePerformance() throws Exception {
		final MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>());
		long timeMillis = System.currentTimeMillis();
		int tcount = 12;
		ExecutorService es = Executors.newCachedThreadPool();
		int count = 1000000;
		final int modulator = 100;
		final AtomicInteger rejects = new AtomicInteger(0);
		for (int k = 0; k < tcount; k++) {
			final int j = k;
			es.submit(new Thread() {
				@Override
				public void run() {
					long ts = System.currentTimeMillis();
					for (int i = 0; i < count; i++) {
						if (isInterrupted()) {
							break;
						}
						try {
							DataPoint dp = new DataPoint("test" + j, "cpu" + (i % modulator), "value",
									Arrays.asList("test2"), ts + i, i * 1.1);
							engine.writeDataPoint(dp);
						} catch (IOException e) {
							rejects.incrementAndGet();
						}
					}
				}
			});
		}
		es.shutdown();
		es.awaitTermination(120, TimeUnit.SECONDS);
		int backOff = 30;
		while (!es.isTerminated()) {
			backOff = backOff * 2;
			Thread.sleep(backOff);
		}
		System.out.println("Write throughput direct " + tcount + "x" + count + ":"
				+ (System.currentTimeMillis() - timeMillis) + "ms with " + rejects.get() + " rejects using " + tcount
				+ "\nWriting " + tcount + " each with " + (modulator) + " measurements");
		assertEquals(tcount, engine.getDatabases().size());
		for (int i = 0; i < tcount; i++) {
			String dbName = "test" + i;
			Set<String> allMeasurementsForDb = engine.getAllMeasurementsForDb(dbName);
			assertEquals(modulator, allMeasurementsForDb.size());
			for (String measurementName : allMeasurementsForDb) {
				Map<String, List<DataPoint>> queryDataPoints = engine.queryDataPoints(dbName, measurementName, "value",
						timeMillis - (3600_000 * 2000), timeMillis + (3600_000 * 2000), Arrays.asList("test2"), null);
				assertEquals(1, queryDataPoints.size());
				assertEquals(count / modulator, queryDataPoints.entrySet().iterator().next().getValue().size());
			}
		}
	}

	// @Test
	// public void testWritePerformanceStress() throws Exception {
	// final MemStorageEngine engine = new MemStorageEngine();
	// engine.configure(new HashMap<>());
	// long timeMillis = System.currentTimeMillis();
	// int tcount = 64;
	// ExecutorService es = Executors.newCachedThreadPool();
	// int count = 10000000;
	// final int modulator = 100;
	// final AtomicInteger rejects = new AtomicInteger(0);
	// for (int k = 0; k < tcount; k++) {
	// final int j = k;
	// es.submit(new Thread() {
	// @Override
	// public void run() {
	// long ts = System.currentTimeMillis();
	// for (int i = 0; i < count; i++) {
	// if (isInterrupted()) {
	// break;
	// }
	// try {
	// DataPoint dp = new DataPoint(ts + i, i * 1.1);
	// dp.setDbName("test"+j);
	// dp.setMeasurementName("cpu");
	// dp.setTags(Arrays.asList("test2", String.valueOf( + (i % modulator))));
	// dp.setValueFieldName("value");
	// engine.writeDataPoint(dp);
	// } catch (IOException e) {
	// rejects.incrementAndGet();
	// }
	// }
	// }
	// });
	// }
	// es.shutdown();
	// es.awaitTermination(120, TimeUnit.SECONDS);
	// int backOff = 30;
	// while (!es.isTerminated()) {
	// backOff = backOff * 2;
	// Thread.sleep(backOff);
	// }
	// System.out.println("Write throughput direct " + tcount + "x" + count +
	// ":"
	// + (System.currentTimeMillis() - timeMillis) + "ms with " + rejects.get()
	// + " rejects"
	// + "\nWriting " + tcount + " each with " + (count/modulator) + " series");
	// }

	@Test
	public void testTagEncodeDecode() throws IOException {
		MemTagIndex table = new MemTagIndex();
		String encodedStr = MemStorageEngine.encodeTagsToString(table, Arrays.asList("host", "value", "test"));
		List<String> decodedStr = MemStorageEngine.decodeStringToTags(table, encodedStr);
		assertEquals(Arrays.asList("host", "value", "test"), decodedStr);
	}

	@Test
	public void testConfigure() {
		StorageEngine engine = new MemStorageEngine();
		try {
			engine.writeDataPoint(
					new DataPoint("test", "ss", "value", Arrays.asList("te"), System.currentTimeMillis(), 2.2));
			fail("Engine not initialized, shouldn't be able to write a datapoint");
		} catch (Exception e) {
		}

		try {
			engine.configure(new HashMap<>());
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			engine.writeDataPoint(
					new DataPoint("test", "ss", "value", Arrays.asList("te"), System.currentTimeMillis(), 2.2));
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
	}

	@Test
	public void testQueryDataPoints() throws IOException, ItemNotFoundException {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(conf);
		long ts = System.currentTimeMillis();
		Map<String, SortedMap<String, TimeSeries>> db = engine.getOrCreateDatabase("test", 24);
		assertEquals(0, db.size());
		engine.writeDataPoint(new DataPoint("test", "cpu", "value", Arrays.asList("test"), ts, 1));
		engine.writeDataPoint(new DataPoint("test", "cpu", "value", Arrays.asList("test"), ts + (400 * 60000), 4));
		assertEquals(1, engine.getOrCreateMeasurement("test", "cpu").size());
		Map<String, List<DataPoint>> queryDataPoints = engine.queryDataPoints("test", "cpu", "value", ts,
				ts + (400 * 60000), null, null);
		assertTrue(!engine.isMeasurementFieldFP("test", "cpu", "value"));
		try {
			engine.isMeasurementFieldFP("test", "test", "test");
			fail("Measurement should not exist");
		} catch (Exception e) {
		}
		assertEquals(2, queryDataPoints.values().iterator().next().size());
		assertEquals(ts, queryDataPoints.values().iterator().next().get(0).getTimestamp());
		assertEquals(ts + (400 * 60000), queryDataPoints.values().iterator().next().get(1).getTimestamp());
		try {
			engine.dropDatabase("test");
		} catch (Exception e) {
		}
		assertEquals(0, engine.getOrCreateMeasurement("test", "cpu").size());
	}

	@Test
	public void testGetMeasurementsLike() throws IOException {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(conf);
		engine.writeDataPoint(
				new DataPoint("test", "cpu", "value", Arrays.asList("test"), System.currentTimeMillis(), 2L));
		engine.writeDataPoint(
				new DataPoint("test", "mem", "value", Arrays.asList("test"), System.currentTimeMillis() + 10, 3L));
		engine.writeDataPoint(
				new DataPoint("test", "netm", "value", Arrays.asList("test"), System.currentTimeMillis() + 20, 5L));
		Set<String> result = engine.getMeasurementsLike("test", " ");
		assertEquals(3, result.size());

		result = engine.getMeasurementsLike("test", "c");
		assertEquals(1, result.size());

		result = engine.getMeasurementsLike("test", "m");
		assertEquals(2, result.size());
	}

	@Test
	public void testSeriesToDataPointConversion() throws RejectException {
		List<DataPoint> points = new ArrayList<>();
		long headerTimestamp = System.currentTimeMillis();
		TimeSeriesBucket timeSeries = new TimeSeriesBucket(4096, headerTimestamp);
		timeSeries.addDataPoint(headerTimestamp, 1L);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test"), points, timeSeries, null, null, false);
		assertEquals(1, points.size());
		points.clear();

		Predicate timepredicate = new BetweenPredicate(Long.MAX_VALUE, Long.MAX_VALUE);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test"), points, timeSeries, timepredicate, null, false);
		assertEquals(0, points.size());
	}

	@Test
	public void testSeriesBucketLookups() throws IOException, ItemNotFoundException {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>());
		engine.connect();
		String dbName = "test1";
		String measurementName = "cpu";
		List<String> tags = Arrays.asList("test");

		long ts = 1483923600000L;
		System.out.println("Base timestamp=" + new Date(ts));

		for (int i = 0; i < 100; i++) {
			engine.writeDataPoint(new DataPoint(dbName, measurementName, "value", tags, ts + (i * 60000), 2.2));
		}
		System.out.println("Buckets:" + engine.getSeriesMap(dbName, measurementName).size());
		long endTs = ts + 99 * 60000;

		// validate all points are returned with a full range query
		Map<String, List<DataPoint>> points = engine.queryDataPoints(dbName, measurementName, "value", ts, endTs, tags,
				null);
		assertEquals(ts, points.values().iterator().next().get(0).getTimestamp());
		assertEquals(endTs,
				points.values().iterator().next().get(points.values().iterator().next().size() - 1).getTimestamp());

		// validate ts-1 yields the same result
		points = engine.queryDataPoints(dbName, measurementName, "value", ts - 1, endTs, tags, null);
		assertEquals(ts, points.values().iterator().next().get(0).getTimestamp());
		System.out.println("Value count:" + points.values().iterator().next().size());
		assertEquals(endTs,
				points.values().iterator().next().get(points.values().iterator().next().size() - 1).getTimestamp());

		// validate ts+1 yields correct result
		points = engine.queryDataPoints(dbName, measurementName, "value", ts + 1, endTs, tags, null);
		assertEquals(ts + 60000, points.values().iterator().next().get(0).getTimestamp());
		assertEquals(endTs,
				points.values().iterator().next().get(points.values().iterator().next().size() - 1).getTimestamp());

		// validate that points have been written to 2 different buckets
		assertTrue(TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, ts, 4096) != TimeUtils
				.getTimeBucket(TimeUnit.MILLISECONDS, endTs, 4096));
		// calculate base timestamp for the second bucket
		long baseTs2 = ((long) TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTs, 4096)) * 1000;
		System.out.println("Bucket2 base timestamp=" + new Date(baseTs2));

		// validate random seek with deliberate time offset
		points = engine.queryDataPoints(dbName, measurementName, "value", ts, baseTs2, tags, null);
		assertEquals("Invalid first entry:" + new Date(points.values().iterator().next().get(0).getTimestamp()), ts,
				points.values().iterator().next().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + (baseTs2 - ts), (baseTs2 / 60000) * 60000,
				points.values().iterator().next().get(points.values().iterator().next().size() - 1).getTimestamp());

		points = engine.queryDataPoints(dbName, measurementName, "value", baseTs2, endTs, tags, null);
		assertEquals("Invalid first entry:" + new Date(points.values().iterator().next().get(0).getTimestamp()),
				(baseTs2 - ts), (baseTs2 / 60000) * 60000, points.values().iterator().next().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + endTs, endTs,
				points.values().iterator().next().get(points.values().iterator().next().size() - 1).getTimestamp());

		// validate correct results when time range is incorrectly swapped i.e.
		// end time is smaller than start time
		points = engine.queryDataPoints(dbName, measurementName, "value", endTs - 1, baseTs2, tags, null);
		assertEquals("Invalid first entry:" + new Date(points.values().iterator().next().get(0).getTimestamp()),
				(baseTs2 - ts), (baseTs2 / 60000) * 60000, points.values().iterator().next().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + endTs, endTs - 60000,
				points.values().iterator().next().get(points.values().iterator().next().size() - 1).getTimestamp());
	}

	@Test
	public void testBaseTimeSeriesWrites() throws Exception {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>());
		engine.connect();

		final long ts1 = System.currentTimeMillis();
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 500; k++) {
			final int p = k;
			es.submit(() -> {
				long ts = System.currentTimeMillis();
				for (int i = 0; i < 1000; i++) {
					try {
						engine.writeDataPoint(
								new DataPoint("test", "helo" + p, "value", Arrays.asList(""), ts + i * 60, ts + i));
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
		}
		es.shutdown();
		es.awaitTermination(10, TimeUnit.SECONDS);

		System.out.println("Write time:" + (System.currentTimeMillis() - ts1) + "\tms");
	}

	@Test
	public void testAddAndReaderDataPoints() throws Exception {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>());
		long curr = System.currentTimeMillis();

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		try {
			engine.writeDataPoint(new DataPoint(dbName, measurementName, valueFieldName, null, curr, 2.2 * 0));
			fail("Must reject the above datapoint due to missing tags");
		} catch (RejectException e) {
		}
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(
					new DataPoint(dbName, measurementName, valueFieldName, Arrays.asList(dbName), curr + i, 2.2 * i));
		}
		assertEquals(1, engine.getAllMeasurementsForDb(dbName).size());
		LinkedHashMap<Reader, Boolean> readers = engine.queryReaders(dbName, measurementName, valueFieldName, curr,
				curr + 3);
		int count = 0;
		for (Entry<Reader, Boolean> entry : readers.entrySet()) {
			assertTrue(entry.getValue());
			while (true) {
				try {
					DataPoint readPair = entry.getKey().readPair();
					assertEquals(2.2 * (count + 1), readPair.getValue(), 0.01);
					count++;
				} catch (RejectException e) {
					break;
				}
			}
		}
		assertEquals(3, count);
		assertTrue(engine.checkIfExists(dbName, measurementName));
		try {
			engine.checkIfExists(dbName + "1");
		} catch (Exception e) {
		}
		engine.dropMeasurement(dbName, measurementName);
		assertEquals(0, engine.getAllMeasurementsForDb(dbName).size());
	}

	@Test
	public void testTagFiltering() throws Exception {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>());
		long curr = System.currentTimeMillis();
		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";

		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(new DataPoint(dbName, measurementName, valueFieldName,
					Arrays.asList(String.valueOf(i), String.valueOf(i + 7)), curr, 2 * i));
		}

		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(new DataPoint(dbName, measurementName, valueFieldName + "2",
					Arrays.asList(String.valueOf(i), String.valueOf(i + 12)), curr, 2 * i));
		}
		Set<String> tags = engine.getTagsForMeasurement(dbName, measurementName);
		assertEquals(9, tags.size());
		Set<String> series = engine.getSeriesIdsWhereTags(dbName, measurementName, Arrays.asList(String.valueOf(1)));
		assertEquals(2, series.size());

		Filter<List<String>> tagFilterTree = new OrFilter<>(Arrays.asList(new ContainsFilter<String, List<String>>("1"),
				new ContainsFilter<String, List<String>>("2")));
		series = engine.getFilteredSeriesTags(dbName, measurementName, valueFieldName, tagFilterTree,
				Arrays.asList("1", "2"));
		assertEquals(2, series.size());

		System.out.println(engine.getTagsForMeasurement(dbName, measurementName));
		tagFilterTree = new AndFilter<>(Arrays.asList(new ContainsFilter<String, List<String>>("1"),
				new ContainsFilter<String, List<String>>("8")));
		series = engine.getFilteredSeriesTags(dbName, measurementName, valueFieldName, tagFilterTree,
				Arrays.asList("1", "8"));
		System.out.println("Series::"+series);
		assertEquals(1, series.size());
	}

	@Test
	public void testAddAndReadDataPoints() throws Exception {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>());
		long curr = System.currentTimeMillis();

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		try {
			engine.writeDataPoint(new DataPoint(dbName, measurementName, valueFieldName, null, curr, 2 * 0));
			fail("Must reject the above datapoint due to missing tags");
		} catch (RejectException e) {
		}
		String tag = "host123123";
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(
					new DataPoint(dbName, measurementName, valueFieldName, Arrays.asList(tag), curr + i, 2 * i));
		}
		assertEquals(1, engine.getAllMeasurementsForDb(dbName).size());
		Map<String, List<DataPoint>> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName,
				curr, curr + 3, Arrays.asList(tag), null);
		assertEquals(1, queryDataPoints.size());
		int i = 1;
		assertEquals(3, queryDataPoints.values().iterator().next().size());
		for (List<DataPoint> list : queryDataPoints.values()) {
			for (DataPoint dataPoint : list) {
				assertEquals(curr + i, dataPoint.getTimestamp());
				i++;
			}
		}
		Set<String> tags = engine.getTagsForMeasurement(dbName, measurementName);
		assertEquals(new HashSet<>(Arrays.asList(tag)), tags);
		Set<String> fieldsForMeasurement = engine.getFieldsForMeasurement(dbName, measurementName);
		assertEquals(new HashSet<>(Arrays.asList(valueFieldName)), fieldsForMeasurement);

		try {
			engine.getTagsForMeasurement(dbName + "1", measurementName);
			fail("This measurement should not exist");
		} catch (Exception e) {
		}

		try {
			engine.getTagsForMeasurement(dbName, measurementName + "1");
			fail("This measurement should not exist");
		} catch (Exception e) {
		}

		try {
			engine.getFieldsForMeasurement(dbName + "1", measurementName);
			fail("This measurement should not exist");
		} catch (Exception e) {
		}

		try {
			engine.getFieldsForMeasurement(dbName, measurementName + "1");
			fail("This measurement should not exist");
		} catch (Exception e) {
		}
	}

	/*
	 * Test used for performance assessment of the old mechnism for data writes
	 * using more parameters public void testWritePerformance() throws
	 * IOException, InterruptedException { final MemStorageEngine engine = new
	 * MemStorageEngine(); engine.configure(new HashMap<>()); long timeMillis =
	 * System.currentTimeMillis(); int tcount = 8; ExecutorService es =
	 * Executors.newFixedThreadPool(tcount); int count = 1000000; final
	 * AtomicInteger rejects = new AtomicInteger(0); for (int k = 0; k < tcount;
	 * k++) { final int j = k; es.submit(() -> { long ts =
	 * System.currentTimeMillis(); for (int i = 0; i < count; i++) { try {
	 * engine.writeDataPoint("test", new DataPoint("test", j + "cpu" + (i %
	 * 1000000), "value", Arrays.asList("test", "test2"), ts + i, i * 1.1)); }
	 * catch (IOException e) { rejects.incrementAndGet(); } } }); }
	 * es.shutdown(); es.awaitTermination(10, TimeUnit.SECONDS);
	 * System.out.println("Write throughput object " + tcount + "x" + count +
	 * ":" + (System.currentTimeMillis() - timeMillis) + "ms with " +
	 * rejects.get() + " rejects using " + tcount); }
	 */
}
