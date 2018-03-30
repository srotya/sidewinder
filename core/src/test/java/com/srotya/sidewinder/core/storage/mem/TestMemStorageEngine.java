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
package com.srotya.sidewinder.core.storage.mem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.filters.ComplexTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.filters.ComplexTagFilter.ComplexFilterType;
import com.srotya.sidewinder.core.filters.SimpleTagFilter.FilterType;
import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.SeriesFieldMap;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * Unit tests for {@link MemStorageEngine}
 * 
 * @author ambud
 */
public class TestMemStorageEngine {

	public static ScheduledExecutorService bgTasks;

	@BeforeClass
	public static void before() {
		bgTasks = Executors.newScheduledThreadPool(1, new BackgrounThreadFactory("bgt"));
	}

	@AfterClass
	public static void after() {
		bgTasks.shutdown();
	}

	private Map<String, String> conf = new HashMap<>();

	public void testWritePerformance() throws Exception {
		final MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgTasks);
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
							Point dp = MiscUtils.buildDataPoint("test" + j, "cpu" + (i % modulator),
									Arrays.asList("value"),
									Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()), ts + i,
									Arrays.asList(Double.doubleToLongBits(i * 1.1)), Arrays.asList(true));
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
				List<Series> queryDataPoints = engine.queryDataPoints(dbName, measurementName, "value",
						timeMillis - (3600_000 * 2000), timeMillis + (3600_000 * 2000), null);
				assertEquals(1, queryDataPoints.size());
				assertEquals(count / modulator, queryDataPoints.iterator().next().getDataPoints().size());
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
	// DataPoint dp = MiscUtils.buildDataPoint(ts + i, i * 1.1);
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
	public void testConfigure() {
		StorageEngine engine = new MemStorageEngine();
		try {
			engine.writeDataPoint(MiscUtils.buildDataPoint("test", "ss", Arrays.asList("value"),
					Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()),
					System.currentTimeMillis(), Arrays.asList(Double.doubleToLongBits(2.2)), Arrays.asList(true)));
		} catch (Exception e) {
		}

		try {
			engine.configure(new HashMap<>(), bgTasks);
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			engine.writeDataPoint(MiscUtils.buildDataPoint("test", "ss", Arrays.asList("value"),
					Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()),
					System.currentTimeMillis(), Arrays.asList(Double.doubleToLongBits(2.2)), Arrays.asList(true)));
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
	}

	@Test
	public void testConfigureTimeBuckets() throws ItemNotFoundException, IOException {
		StorageEngine engine = new MemStorageEngine();
		HashMap<String, String> conf = new HashMap<>();
		long ts = System.currentTimeMillis();
		conf.put(StorageEngine.DEFAULT_BUCKET_SIZE, String.valueOf(4096 * 10));
		try {
			engine.configure(conf, bgTasks);
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			for (int i = 0; i < 10; i++) {
				engine.writeDataPoint(MiscUtils.buildDataPoint("test", "ss", Arrays.asList("value"),
						Arrays.asList(Tag.newBuilder().setTagKey("te").setTagValue("2").build()),
						ts + (i * 4096 * 1000), Arrays.asList(Double.doubleToLongBits(2.2)), Arrays.asList(true)));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
		List<Series> queryDataPoints = engine.queryDataPoints("test", "ss", "value", ts, ts + (4096 * 100 * 1000) + 1,
				null);
		assertTrue(queryDataPoints.size() >= 1);
	}

	@Test
	public void testQueryDataPoints() throws IOException, ItemNotFoundException {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(conf, bgTasks);
		long ts = System.currentTimeMillis();
		Map<String, Measurement> db = engine.getOrCreateDatabase("test", 24);
		assertEquals(0, db.size());

		engine.writeDataPoint(MiscUtils.buildDataPoint("test", "cpu", Arrays.asList("value"),
				Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build()), ts, Arrays.asList(1L),
				Arrays.asList(false)));
		engine.writeDataPoint(MiscUtils.buildDataPoint("test", "cpu", Arrays.asList("value"),
				Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build()), ts + (400 * 60000),
				Arrays.asList(4L), Arrays.asList(false)));
		List<Series> queryDataPoints = engine.queryDataPoints("test", "cpu", "value", ts, ts + (400 * 60000), null,
				null);
		assertEquals(2, queryDataPoints.iterator().next().getDataPoints().size());
		assertEquals(ts, queryDataPoints.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals(ts + (400 * 60000), queryDataPoints.iterator().next().getDataPoints().get(1).getTimestamp());
		assertTrue(!engine.isMeasurementFieldFP("test", "cpu", "value"));
		try {
			engine.isMeasurementFieldFP("test", "test", "test");
			fail("Measurement should not exist");
		} catch (Exception e) {
		}
		try {
			engine.dropDatabase("test");
		} catch (Exception e) {
		}
		assertEquals(0, engine.getOrCreateMeasurement("test", "cpu").getTimeSeries().size());
	}

	@Test
	public void testGetMeasurementsLike() throws Exception {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(conf, bgTasks);
		engine.writeDataPoint(MiscUtils.buildDataPoint("test", "cpu", Arrays.asList("value"),
				Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()), System.currentTimeMillis(),
				Arrays.asList(2L), Arrays.asList(false)));
		engine.writeDataPoint(MiscUtils.buildDataPoint("test", "mem", Arrays.asList("value"),
				Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()),
				System.currentTimeMillis() + 10, Arrays.asList(3L), Arrays.asList(false)));
		engine.writeDataPoint(MiscUtils.buildDataPoint("test", "netm", Arrays.asList("value"),
				Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()),
				System.currentTimeMillis() + 20, Arrays.asList(5L), Arrays.asList(false)));

		Set<String> result = engine.getMeasurementsLike("test", " ");
		assertEquals(3, result.size());

		result = engine.getMeasurementsLike("test", "c.*");
		assertEquals(1, result.size());

		result = engine.getMeasurementsLike("test", ".*m.*");
		assertEquals(2, result.size());
	}

	@Test
	public void testSeriesToDataPointConversion() throws IOException {
		List<DataPoint> points = new ArrayList<>();
		long headerTimestamp = System.currentTimeMillis();
		ByteBuffer buf = ByteBuffer.allocate(1024);
		Writer timeSeries = new ByzantineWriter();
		timeSeries.configure(buf, true, 1, false);
		timeSeries.setHeaderTimestamp(headerTimestamp);
		timeSeries.addValue(headerTimestamp, 1L);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test=2"), points, timeSeries, null, null, false);
		assertEquals(1, points.size());
		points.clear();

		Predicate timepredicate = new BetweenPredicate(Long.MAX_VALUE, Long.MAX_VALUE);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test=2"), points, timeSeries, timepredicate, null, false);
		assertEquals(0, points.size());
	}

	@Test
	public void testSeriesBucketLookups() throws IOException, ItemNotFoundException {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgTasks);
		engine.connect();
		String dbName = "test1";
		String measurementName = "cpu";
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1212").build());

		long ts = 1483923600000L;
		System.out.println("Base timestamp=" + new Date(ts));

		for (int i = 0; i < 100; i++) {

			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList("value"), tags,
					ts + (i * 60000), Arrays.asList(Double.doubleToLongBits(2.2)), Arrays.asList(true)));
		}
		System.out.println("Buckets:" + engine.getMeasurement(dbName, measurementName).getTimeSeries().size());
		long endTs = ts + 99 * 60000;

		// validate all points are returned with a full range query
		List<Series> points = engine.queryDataPoints(dbName, measurementName, "value", ts, endTs, null);
		assertEquals(ts, points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals(endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate ts-1 yields the same result
		points = engine.queryDataPoints(dbName, measurementName, "value", ts - 1, endTs, null);
		assertEquals(ts, points.iterator().next().getDataPoints().get(0).getTimestamp());
		System.out.println("Value count:" + points.iterator().next().getDataPoints().size());
		assertEquals(endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate ts+1 yields correct result
		points = engine.queryDataPoints(dbName, measurementName, "value", ts + 1, endTs, null);
		assertEquals(ts + 60000, points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals(endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate that points have been written to 2 different buckets
		assertTrue(TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, ts, 4096) != TimeUtils
				.getTimeBucket(TimeUnit.MILLISECONDS, endTs, 4096));
		// calculate base timestamp for the second bucket
		long baseTs2 = ((long) TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTs, 4096)) * 1000;
		System.out.println("Bucket2 base timestamp=" + new Date(baseTs2));

		// validate random seek with deliberate time offset
		points = engine.queryDataPoints(dbName, measurementName, "value", ts, baseTs2, null);
		assertEquals("Invalid first entry:" + new Date(points.iterator().next().getDataPoints().get(0).getTimestamp()),
				ts, points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + (baseTs2 - ts), (baseTs2 / 60000) * 60000, points.iterator().next()
				.getDataPoints().get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		points = engine.queryDataPoints(dbName, measurementName, "value", baseTs2, endTs, null);
		assertEquals("Invalid first entry:" + new Date(points.iterator().next().getDataPoints().get(0).getTimestamp()),
				(baseTs2 - ts), (baseTs2 / 60000) * 60000,
				points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + endTs, endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate correct results when time range is incorrectly swapped i.e.
		// end time is smaller than start time
		points = engine.queryDataPoints(dbName, measurementName, "value", endTs - 1, baseTs2, null);
		assertEquals("Invalid first entry:" + new Date(points.iterator().next().getDataPoints().get(0).getTimestamp()),
				(baseTs2 - ts), (baseTs2 / 60000) * 60000,
				points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + endTs, endTs - 60000, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());
	}

	@Test
	public void testBaseTimeSeriesWrites() throws Exception {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgTasks);
		engine.connect();

		final long ts1 = System.currentTimeMillis();
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 500; k++) {
			final int p = k;
			es.submit(() -> {
				long ts = System.currentTimeMillis();
				for (int i = 0; i < 1000; i++) {
					try {
						;
						engine.writeDataPoint(MiscUtils.buildDataPoint("test", "helo" + p, Arrays.asList("value"),
								Arrays.asList(Tag.newBuilder().setTagKey("k").setTagValue("2").build()), ts + i * 60,
								Arrays.asList(ts + i), Arrays.asList(false)));
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
		engine.configure(new HashMap<>(), bgTasks);
		long curr = System.currentTimeMillis();

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		try {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName), null,
					curr, Arrays.asList(Double.doubleToLongBits(2.2 * 0)), Arrays.asList(true)));
			fail("Must reject the above datapoint due to missing tags");
		} catch (Exception e) {
		}
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName),
					Arrays.asList(Tag.newBuilder().setTagKey(dbName).setTagValue("2").build()), curr + i,
					Arrays.asList(Double.doubleToLongBits(2.2 * i)), Arrays.asList(true)));
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
		engine.configure(new HashMap<>(), bgTasks);
		long curr = System.currentTimeMillis();
		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";

		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName),
					Arrays.asList(Tag.newBuilder().setTagKey("p").setTagValue(String.valueOf(i)).build(),
							Tag.newBuilder().setTagKey("k").setTagValue(String.valueOf(i + 7)).build()),
					curr, Arrays.asList(2L * i), Arrays.asList(false)));
		}

		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName),
					Arrays.asList(Tag.newBuilder().setTagKey("p").setTagValue(String.valueOf(i)).build(),
							Tag.newBuilder().setTagKey("k").setTagValue(String.valueOf(i + 12)).build()),
					curr, Arrays.asList(2L * i), Arrays.asList(false)));
		}
		Set<String> tags = engine.getTagKeysForMeasurement(dbName, measurementName);
		assertEquals(2, tags.size());
		// Set<String> series = engine.getSeriesIdsWhereTags(dbName, measurementName,
		// Arrays.asList("p=" + String.valueOf(1)));
		// assertEquals(2, series.size());

		TagFilter tagFilterTree = new ComplexTagFilter(ComplexFilterType.OR, Arrays.asList(
				new SimpleTagFilter(FilterType.EQUALS, "p", "1"), new SimpleTagFilter(FilterType.EQUALS, "p", "2")));
		Set<String> series = engine.getTagFilteredRowKeys(dbName, measurementName, tagFilterTree);
		assertEquals(4, series.size());

		System.out.println(engine.getTagKeysForMeasurement(dbName, measurementName));
		tagFilterTree = new ComplexTagFilter(ComplexFilterType.AND, Arrays.asList(
				new SimpleTagFilter(FilterType.EQUALS, "p", "1"), new SimpleTagFilter(FilterType.EQUALS, "k", "8")));
		series = engine.getTagFilteredRowKeys(dbName, measurementName, tagFilterTree);
		System.out.println("Series::" + series);
		assertEquals(1, series.size());
	}

	@Test
	public void testAddAndReadDataPointsWithTagFilters() throws Exception {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgTasks);
		long curr = System.currentTimeMillis();

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		String tag = "host";

		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(dbName, measurementName, valueFieldName,
					Arrays.asList(Tag.newBuilder().setTagKey(tag).setTagValue("123123" + i).build(),
							Tag.newBuilder().setTagKey(tag).setTagValue("123123" + (i + 1)).build()),
					curr + i, 2 * i);
		}
		assertEquals(1, engine.getAllMeasurementsForDb(dbName).size());

		SimpleTagFilter filter1 = new SimpleTagFilter(FilterType.EQUALS, "host", "1231231");
		SimpleTagFilter filter2 = new SimpleTagFilter(FilterType.EQUALS, "host", "1231232");

		List<Series> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, curr - 1,
				curr + 4, new ComplexTagFilter(ComplexFilterType.OR, Arrays.asList(filter1, filter2)), null, null);
		assertEquals(2, queryDataPoints.size());
		assertEquals(1, queryDataPoints.iterator().next().getDataPoints().size());
		Collections.sort(queryDataPoints);
		int i = 1;
		for (Series list : queryDataPoints) {
			List<DataPoint> dataPoints = list.getDataPoints();
			DataPoint dataPoint = dataPoints.get(0);
			assertEquals(i + curr, dataPoint.getTimestamp());
			i++;
		}
		Set<String> tagKeys = engine.getTagKeysForMeasurement(dbName, measurementName);
		assertEquals(new HashSet<>(Arrays.asList("host")), tagKeys);
		assertEquals(new HashSet<>(Arrays.asList("1231231", "1231232", "1231233", "1231234")),
				engine.getTagValuesForMeasurement(dbName, measurementName, "host"));
		Set<String> fieldsForMeasurement = engine.getFieldsForMeasurement(dbName, measurementName);
		assertEquals(new HashSet<>(Arrays.asList(valueFieldName)), fieldsForMeasurement);

		try {
			engine.getTagKeysForMeasurement(dbName + "1", measurementName);
			fail("This measurement should not exist");
		} catch (Exception e) {
		}

		try {
			engine.getTagKeysForMeasurement(dbName, measurementName + "1");
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

	@Test
	public void testAddAndReadDataPoints() throws Exception {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgTasks);
		long curr = System.currentTimeMillis();

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		try {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName), null,
					curr, Arrays.asList(2L * 0), Arrays.asList(false)));
			fail("Must reject the above datapoint due to missing tags");
		} catch (Exception e) {
		}
		String tag = "host";
		String tagValue = "123123";
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName),
					Arrays.asList(Tag.newBuilder().setTagKey(tag).setTagValue(tagValue).build()), curr + i,
					Arrays.asList(2L * 0), Arrays.asList(false)));
		}
		assertEquals(1, engine.getAllMeasurementsForDb(dbName).size());
		List<Series> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, curr, curr + 3,
				null, null);
		assertEquals(1, queryDataPoints.size());
		int i = 1;
		assertEquals(3, queryDataPoints.iterator().next().getDataPoints().size());
		for (Series list : queryDataPoints) {
			for (DataPoint dataPoint : list.getDataPoints()) {
				assertEquals(curr + i, dataPoint.getTimestamp());
				i++;
			}
		}
		Set<String> tags = engine.getTagKeysForMeasurement(dbName, measurementName);
		assertEquals(new HashSet<>(Arrays.asList("host")), tags);
		Set<String> fieldsForMeasurement = engine.getFieldsForMeasurement(dbName, measurementName);
		assertEquals(new HashSet<>(Arrays.asList(valueFieldName)), fieldsForMeasurement);

		try {
			engine.getTagKeysForMeasurement(dbName + "1", measurementName);
			fail("This measurement should not exist");
		} catch (Exception e) {
		}

		try {
			engine.getTagKeysForMeasurement(dbName, measurementName + "1");
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

		assertEquals(1, engine.getFieldsForMeasurement(dbName, "c.*").size());
		assertEquals(1, engine.getFieldsForMeasurement(dbName, ".*").size());
		try {
			engine.getFieldsForMeasurement(dbName, "d.*");
			fail("This measurement should not exist");
		} catch (Exception e) {
		}
	}

	@Test
	public void testCompaction() throws IOException, InterruptedException {
		MemStorageEngine engine = new MemStorageEngine();
		HashMap<String, String> conf2 = new HashMap<>();
		conf2.put("default.bucket.size", "409600");
		conf2.put("compaction.enabled", "true");
		conf2.put("use.query.pool", "false");
		conf2.put("compaction.codec", "gorilla");
		conf2.put("compaction.delay", "1");
		conf2.put("compaction.frequency", "1");
		engine.configure(conf2, bgTasks);
		final long curr = 1497720652566L;

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("host").setTagValue("123123").build());
		for (int i = 1; i <= 10000; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName), tags,
					curr + i * 1000, Arrays.asList(Double.doubleToLongBits(i * 1.1)), Arrays.asList(true)));
		}

		long ts = System.nanoTime();
		List<Series> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, curr - 1000,
				curr + 10000 * 1000 + 1, null, null);
		ts = System.nanoTime() - ts;
		System.out.println("Before compaction:" + ts / 1000 + "us");
		assertEquals(1, queryDataPoints.size());
		assertEquals(10000, queryDataPoints.iterator().next().getDataPoints().size());
		List<DataPoint> dataPoints = queryDataPoints.iterator().next().getDataPoints();
		for (int i = 1; i <= 10000; i++) {
			DataPoint dp = dataPoints.get(i - 1);
			assertEquals("Bad ts:" + i, curr + i * 1000, dp.getTimestamp());
			assertEquals(dp.getValue(), i * 1.1, 0.001);
		}
		Measurement m = engine.getOrCreateMeasurement(dbName, measurementName);
		SeriesFieldMap t = m.getOrCreateSeriesFieldMap(tags);
		TimeSeries series = t.getOrCreateSeries(valueFieldName, 409600, false, m);
		SortedMap<Integer, List<Writer>> bucketRawMap = series.getBucketRawMap();
		assertEquals(1, bucketRawMap.size());
		int size = bucketRawMap.values().iterator().next().size();
		assertTrue(series.getCompactionSet().size() < size);
		assertTrue(size > 2);
		Thread.sleep(2000);
		ts = System.nanoTime();
		queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, curr - 1,
				curr + 20000 * 1000 + 1, null, null);
		ts = System.nanoTime() - ts;
		System.out.println("After compaction:" + ts / 1000 + "us");
		bucketRawMap = series.getBucketRawMap();
		assertEquals(2, bucketRawMap.values().iterator().next().size());
		assertEquals(10000, queryDataPoints.iterator().next().getDataPoints().size());
		dataPoints = queryDataPoints.iterator().next().getDataPoints();
		for (int i = 1; i <= 10000; i++) {
			DataPoint dp = dataPoints.get(i - 1);
			assertEquals("Bad ts:" + i, curr + i * 1000, dp.getTimestamp());
			assertEquals(dp.getValue(), i * 1.1, 0.001);
		}
	}

	@Test
	public void testCompactionThreadSafety() throws IOException, InterruptedException {
		MemStorageEngine engine = new MemStorageEngine();
		HashMap<String, String> conf2 = new HashMap<>();
		conf2.put("default.bucket.size", "409600");
		conf2.put("compaction.enabled", "true");
		conf2.put("use.query.pool", "false");
		engine.configure(conf2, bgTasks);
		final long curr = 1497720652566L;

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("host").setTagValue("123123").build());
		for (int i = 1; i <= 10000; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName), tags,
					curr + i * 1000, Arrays.asList(Double.doubleToLongBits(i * 1.1)), Arrays.asList(true)));
		}

		long ts = System.nanoTime();
		List<Series> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, curr - 1000,
				curr + 10000 * 1000 + 1, null, null);
		ts = System.nanoTime() - ts;
		System.out.println("Before compaction:" + ts / 1000 + "us");
		assertEquals(1, queryDataPoints.size());
		assertEquals(10000, queryDataPoints.iterator().next().getDataPoints().size());
		List<DataPoint> dataPoints = queryDataPoints.iterator().next().getDataPoints();
		for (int i = 1; i <= 10000; i++) {
			DataPoint dp = dataPoints.get(i - 1);
			assertEquals("Bad ts:" + i, curr + i * 1000, dp.getTimestamp());
			assertEquals(dp.getValue(), i * 1.1, 0.001);
		}
		Measurement m = engine.getOrCreateMeasurement(dbName, measurementName);
		SeriesFieldMap t = m.getOrCreateSeriesFieldMap(tags);
		final TimeSeries series = t.getOrCreateSeries(valueFieldName, 409600, false, m);
		SortedMap<Integer, List<Writer>> bucketRawMap = series.getBucketRawMap();
		assertEquals(1, bucketRawMap.size());
		int size = bucketRawMap.values().iterator().next().size();
		assertTrue(series.getCompactionSet().size() < size);
		assertTrue(size > 2);
		final AtomicBoolean bool = new AtomicBoolean(false);
		bgTasks.execute(() -> {
			while (!bool.get()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					break;
				}
			}
			try {
				series.addDataPoint(TimeUnit.MILLISECONDS, curr + 1000 * 10001, 1.11);
				bool.set(false);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
		});
		series.compact(l -> {
			bool.set(true);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (!bool.get()) {
				throw new RuntimeException("Synchronized block failed");
			}
		});
		Thread.sleep(100);
		assertTrue(!bool.get());
	}

}
