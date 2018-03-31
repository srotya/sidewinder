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
package com.srotya.sidewinder.core.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.google.gson.Gson;
import com.srotya.sidewinder.core.filters.ComplexTagFilter;
import com.srotya.sidewinder.core.filters.ComplexTagFilter.ComplexFilterType;
import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter.FilterType;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;
import com.srotya.sidewinder.core.storage.disk.DiskMalloc;
import com.srotya.sidewinder.core.storage.disk.DiskStorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * Unit tests for {@link DiskStorageEngine}
 * 
 * @author ambud
 */
@RunWith(Parameterized.class)
public class TestStorageEngine {

	public static ScheduledExecutorService bgTasks;
	private Map<String, String> conf;
	@Parameter
	public Class<StorageEngine> clazz;
	private StorageEngine engine;

	@BeforeClass
	public static void beforeClass() {
		bgTasks = Executors.newScheduledThreadPool(1, new BackgrounThreadFactory("te1"));
	}

	@SuppressWarnings("rawtypes")
	@Parameters(name = "{index}: Storage Engine Impl Class: {0}")
	public static Collection classes() {
		List<Object[]> implementations = new ArrayList<>();
		implementations.add(new Object[] { DiskStorageEngine.class });
		implementations.add(new Object[] { MemStorageEngine.class });
		return implementations;
	}

	@AfterClass
	public static void afterClass() {
		bgTasks.shutdown();
	}

	@Before
	public void before() throws InstantiationException, IllegalAccessException, IOException {
		conf = new HashMap<>();
		conf.put("data.dir", "target/se-common-test/data");
		conf.put("index.dir", "target/se-common-test/index");
		engine = clazz.newInstance();
	}

	@After
	public void after() throws IOException {
		engine.disconnect();
		MiscUtils.delete(new File("target/se-common-test/"));
	}

	@Test
	public void testInvalidArchiver() throws IOException {
		conf.put(StorageEngine.ARCHIVER_CLASS, "asdasd");
		engine.configure(conf, bgTasks);
	}

	@Test
	public void testUpdateTimeSeriesRetention() throws IOException {
		engine.configure(conf, bgTasks);
		engine.getOrCreateMeasurement("db1", "m1");
		engine.updateDefaultTimeSeriesRetentionPolicy("db1", 10);
		assertEquals(10, engine.getDbMetadataMap().get("db1").getRetentionHours());
		Measurement m = engine.getOrCreateMeasurement("db1", "m1");
		SeriesFieldMap s = m.getOrCreateSeriesFieldMap(
				Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build()), false);
		TimeSeries ts = s.getOrCreateSeriesLocked("vf1", 4096, false, m);
		int buckets = ts.getRetentionBuckets();
		engine.updateDefaultTimeSeriesRetentionPolicy("db1", 30);
		engine.updateTimeSeriesRetentionPolicy("db1", 30);
		engine.updateTimeSeriesRetentionPolicy("db1", "m1", 40);
		engine.updateTimeSeriesRetentionPolicy("db1", "m1", "vf1",
				Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build()), 60);
		assertTrue(buckets != ts.getRetentionBuckets());
	}

	@Test
	public void testMetadataOperations() throws Exception {
		engine.configure(conf, bgTasks);
		Measurement m = engine.getOrCreateMeasurement("db1", "m1");
		SeriesFieldMap s = m.getOrCreateSeriesFieldMap(
				Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build()), false);
		s.getOrCreateSeriesLocked("vf1", 4096, false, m);

		assertEquals(1, engine.getAllMeasurementsForDb("db1").size());
		assertEquals(1, engine.getTagKeysForMeasurement("db1", "m1").size());
		assertEquals(1, engine.getTagsForMeasurement("db1", "m1", "vf1").size());
		try {
			engine.getAllMeasurementsForDb("db2");
			fail("Exception must be thrown");
		} catch (ItemNotFoundException e) {
		}
		try {
			engine.getTagKeysForMeasurement("db1", "m2");
			fail("Exception must be thrown");
		} catch (ItemNotFoundException e) {
		}
		assertEquals(1, engine.getTagsForMeasurement("db1", "m1", "vf2").size());
	}

	@Test
	public void testMeasurementsLike() throws Exception {
		engine.configure(conf, bgTasks);
		Measurement m = engine.getOrCreateMeasurement("db1", "m1");
		SeriesFieldMap s = m.getOrCreateSeriesFieldMap(
				Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build()), false);
		s.getOrCreateSeriesLocked("vf1", 4096, false, m);
		m = engine.getOrCreateMeasurement("db1", "t1");
		s = m.getOrCreateSeriesFieldMap(Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build()), false);
		s.getOrCreateSeriesLocked("vf1", 4096, false, m);

		Set<String> measurementsLike = engine.getMeasurementsLike("db1", "m.*");
		assertEquals(1, measurementsLike.size());
		assertEquals(2, engine.getAllMeasurementsForDb("db1").size());
	}

	@Test
	public void testConcurrentOperations() throws Exception {
		engine.configure(conf, bgTasks);
		final long ts = System.currentTimeMillis();
		ExecutorService es = Executors.newFixedThreadPool(2, new BackgrounThreadFactory("wr1"));
		String measurementName = "mmm2";
		String valueFieldName = "v1";
		String dbName = "db9";
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("h").setTagValue("1").build());
		for (int k = 0; k < 2; k++) {
			final int p = k;
			es.submit(() -> {
				long t = ts + p;
				for (int i = 0; i < 100; i++) {
					Point dp = MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, tagd, t + i * 1000, i);
					try {
						engine.writeDataPointLocked(dp, false);
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
				System.err.println("Completed writes:" + 100 + " data points");
			});
		}
		es.shutdown();
		es.awaitTermination(100, TimeUnit.SECONDS);
		assertEquals(1, engine.getAllMeasurementsForDb(dbName).size());
		assertEquals(1, engine.getMeasurementMap().size());
		try {
			TimeSeries timeSeries = engine.getTimeSeries(dbName, measurementName, valueFieldName, tagd);
			assertNotNull(timeSeries);
		} catch (ItemNotFoundException e) {
			fail("Time series must exist");
		}
		List<Series> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, ts,
				ts + 220 * 1000, null);
		assertEquals(1, queryDataPoints.size());
		Series next = queryDataPoints.iterator().next();
		assertEquals(200, next.getDataPoints().size());
	}

	@Test
	public void testConfigureTimeBuckets() throws ItemNotFoundException, IOException {
		long ts = System.currentTimeMillis();
		conf.put(StorageEngine.DEFAULT_BUCKET_SIZE, String.valueOf(4096 * 10));
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("e").build());
		try {
			engine.configure(conf, bgTasks);
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			for (int i = 0; i < 10; i++) {
				engine.writeDataPointLocked(
						MiscUtils.buildDataPoint("test", "ss", "value", tagd, ts + (i * 4096 * 1000), 2.2), false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
		List<Series> queryDataPoints = engine.queryDataPoints("test", "ss", "value", ts, ts + (4096 * 100 * 1000) + 1,
				new SimpleTagFilter(FilterType.EQUALS, "t", "e"));
		assertTrue(queryDataPoints.size() >= 1);
	}

	@Test
	public void testConfigure() throws IOException {
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("e").build());
		try {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint("test", "ss", "value", tagd, System.currentTimeMillis(), 2.2), false);
			fail("Engine not initialized, shouldn't be able to write a datapoint");
		} catch (Exception e) {
		}

		try {
			// FileUtils.forceDelete(new File("target/db2/"));
			MiscUtils.delete(new File("targer/db2/"));
			HashMap<String, String> map = new HashMap<>();
			// map.put("metadata.dir", "target/db2/mdq");
			map.put("index.dir", "target/db2/index");
			map.put("data.dir", "target/db2/data");
			map.put(StorageEngine.PERSISTENCE_DISK, "true");
			map.put("default.series.retention.hours", "32");
			engine.configure(map, bgTasks);
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint("test", "ss", "value", tagd, System.currentTimeMillis(), 2.2), false);
			String md = new String(Files.readAllBytes(new File("target/db2/data/test/.md").toPath()),
					Charset.forName("utf8"));
			DBMetadata metadata = new Gson().fromJson(md, DBMetadata.class);
			assertEquals(32, metadata.getRetentionHours());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
		engine.disconnect();
	}

	@Test
	public void testQueryDataPoints() throws IOException, ItemNotFoundException {
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("e").build());

		MiscUtils.delete(new File("target/db15/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db15/mdq");
		map.put("index.dir", "target/db15/index");
		map.put("data.dir", "target/db15/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		map.put("disk.compression.class", ByzantineWriter.class.getName());
		engine.configure(map, bgTasks);
		long ts = System.currentTimeMillis();
		Map<String, Measurement> db = engine.getOrCreateDatabase("test3", 24);
		assertEquals(0, db.size());
		engine.writeDataPointLocked(MiscUtils.buildDataPoint("test3", "cpu", "value", tagd, ts, 1), false);
		engine.writeDataPointLocked(MiscUtils.buildDataPoint("test3", "cpu", "value", tagd, ts + (400 * 60000), 4),
				false);
		assertEquals(1, engine.getOrCreateMeasurement("test3", "cpu").getSeriesKeys().size());
		List<Series> queryDataPoints = null;
		try {
			queryDataPoints = engine.queryDataPoints("test3", "cpu", "value", ts, ts + (400 * 60000), null, null);
		} catch (Exception e) {

		}
		try {
			engine.queryDataPoints("test123", "cpu", "value", ts, ts + (400 * 60000), null, null);
		} catch (ItemNotFoundException e) {
		}
		try {
			engine.queryDataPoints("test3", "123cpu", "value", ts, ts + (400 * 60000), null, null);
		} catch (ItemNotFoundException e) {
		}
		assertTrue(!engine.isMeasurementFieldFP("test3", "cpu", "value"));
		try {
			engine.isMeasurementFieldFP("test3", "test", "test");
			fail("Measurement should not exist");
		} catch (Exception e) {
		}
		assertEquals(2, queryDataPoints.iterator().next().getDataPoints().size());
		assertEquals(ts, queryDataPoints.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals(ts + (400 * 60000), queryDataPoints.iterator().next().getDataPoints().get(1).getTimestamp());
		try {
			engine.dropDatabase("test3");
		} catch (Exception e) {
		}
		assertEquals(0, engine.getOrCreateMeasurement("test3", "cpu").getSeriesKeys().size());
		engine.disconnect();
	}

	@Test
	public void testGarbageCollector() throws Exception {
		conf.put(StorageEngine.GC_DELAY, "1");
		conf.put(StorageEngine.GC_FREQUENCY, "10");
		conf.put(StorageEngine.PERSISTENCE_DISK, "true");
		conf.put(StorageEngine.DEFAULT_BUCKET_SIZE, "4096");
		conf.put(DiskMalloc.CONF_MEASUREMENT_INCREMENT_SIZE, "4096");
		conf.put(DiskMalloc.CONF_MEASUREMENT_FILE_INCREMENT, "10240");
		conf.put(DiskMalloc.CONF_MEASUREMENT_FILE_MAX, String.valueOf(1024 * 100));
		conf.put(StorageEngine.RETENTION_HOURS, "28");
		engine.configure(conf, bgTasks);
		long base = 1497720452566L;
		long ts = base;
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build());
		for (int i = 320; i >= 0; i--) {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint("test", "cpu2", "value", tagd, base - (3600_000 * i), 2L), false);
		}
		engine.getMeasurementMap().get("test").get("cpu2").collectGarbage(null);
		List<Series> queryDataPoints = engine.queryDataPoints("test", "cpu2", "value", ts - (3600_000 * 320), ts, null,
				null);
		assertEquals(27, queryDataPoints.iterator().next().getDataPoints().size());
		assertTrue(!engine.isMeasurementFieldFP("test", "cpu2", "value"));
	}

	@Test
	public void testGetMeasurementsLike() throws Exception {
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("e").build());
		engine.configure(conf, bgTasks);
		engine.writeDataPointLocked(
				MiscUtils.buildDataPoint("test", "cpu", "value", tagd, System.currentTimeMillis(), 2L), false);
		engine.writeDataPointLocked(
				MiscUtils.buildDataPoint("test", "mem", "value", tagd, System.currentTimeMillis() + 10, 3L), false);
		engine.writeDataPointLocked(
				MiscUtils.buildDataPoint("test", "netm", "value", tagd, System.currentTimeMillis() + 20, 5L), false);
		Set<String> result = engine.getMeasurementsLike("test", " ");
		assertEquals(3, result.size());

		result = engine.getMeasurementsLike("test", "c.*");
		assertEquals(1, result.size());

		result = engine.getMeasurementsLike("test", ".*m.*");
		assertEquals(2, result.size());
		engine.disconnect();
	}

	@Test
	public void testSeriesToDataPointConversion() throws IOException {
		List<DataPoint> points = new ArrayList<>();
		long headerTimestamp = System.currentTimeMillis();
		ByteBuffer buf = ByteBuffer.allocate(100);
		Writer timeSeries = new ByzantineWriter();
		timeSeries.configure(buf, true, 1, false);
		timeSeries.setHeaderTimestamp(headerTimestamp);
		timeSeries.addValueLocked(headerTimestamp, 1L);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test=1"), points, timeSeries, null, null, false);
		assertEquals(1, points.size());
		points.clear();

		Predicate timepredicate = new BetweenPredicate(Long.MAX_VALUE, Long.MAX_VALUE);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test=1"), points, timeSeries, timepredicate, null, false);
		assertEquals(0, points.size());
	}

	@Test
	public void testSeriesBucketLookups() throws IOException, ItemNotFoundException {
		engine.configure(conf, bgTasks);
		engine.connect();
		String dbName = "test1";
		String measurementName = "cpu";
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build());

		long ts = 1483923600000L;
		System.out.println("Base timestamp=" + new Date(ts));

		for (int i = 0; i < 100; i++) {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint(dbName, measurementName, "value", tagd, ts + (i * 60000), 2.2), false);
		}
		long endTs = ts + 99 * 60000;

		// validate all points are returned with a full range query
		List<Series> points = engine.queryDataPoints(dbName, measurementName, "value", ts, endTs,
				new SimpleTagFilter(FilterType.EQUALS, "test", "1"));
		assertEquals(ts, points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals(endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate ts-1 yields the same result
		points = engine.queryDataPoints(dbName, measurementName, "value", ts - 1, endTs,
				new SimpleTagFilter(FilterType.EQUALS, "test", "1"));
		assertEquals(ts, points.iterator().next().getDataPoints().get(0).getTimestamp());
		System.out.println("Value count:" + points.iterator().next().getDataPoints().size());
		assertEquals(endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate ts+1 yields correct result
		points = engine.queryDataPoints(dbName, measurementName, "value", ts + 1, endTs,
				new SimpleTagFilter(FilterType.EQUALS, "test", "1"));
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
		points = engine.queryDataPoints(dbName, measurementName, "value", ts, baseTs2,
				new SimpleTagFilter(FilterType.EQUALS, "test", "1"));
		assertEquals("Invalid first entry:" + new Date(points.iterator().next().getDataPoints().get(0).getTimestamp()),
				ts, points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + (baseTs2 - ts), (baseTs2 / 60000) * 60000, points.iterator().next()
				.getDataPoints().get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		points = engine.queryDataPoints(dbName, measurementName, "value", baseTs2, endTs,
				new SimpleTagFilter(FilterType.EQUALS, "test", "1"));
		assertEquals("Invalid first entry:" + new Date(points.iterator().next().getDataPoints().get(0).getTimestamp()),
				(baseTs2 - ts), (baseTs2 / 60000) * 60000,
				points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + endTs, endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate correct results when time range is incorrectly swapped i.e.
		// end time is smaller than start time
		points = engine.queryDataPoints(dbName, measurementName, "value", endTs - 1, baseTs2,
				new SimpleTagFilter(FilterType.EQUALS, "test", "1"));
		assertEquals("Invalid first entry:" + new Date(points.iterator().next().getDataPoints().get(0).getTimestamp()),
				(baseTs2 - ts), (baseTs2 / 60000) * 60000,
				points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + endTs, endTs - 60000, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());
		engine.disconnect();
	}

	@Test
	public void testBaseTimeSeriesWrites() throws Exception {
		engine.configure(conf, bgTasks);
		engine.connect();

		final List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("p").setTagValue("1").build());
		final long ts1 = System.currentTimeMillis();
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 10; k++) {
			final int p = k;
			es.submit(() -> {
				long ts = System.currentTimeMillis();
				for (int i = 0; i < 1000; i++) {
					try {
						engine.writeDataPointLocked(
								MiscUtils.buildDataPoint("test", "helo" + p, "value", tagd, ts + i * 60, ts + i),
								false);
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
		File file = new File("target/db8/");
		if (file.exists()) {
			MiscUtils.delete(file);
		}
		engine.configure(conf, bgTasks);
		long curr = 1497720452566L;

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		try {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, null, curr, 2.2 * 0), false);
			fail("Must reject the above datapoint due to missing tags");
		} catch (Exception e) {
		}
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey(dbName).setTagValue("1").build());
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, tagd, curr + i, 2.2 * i), false);
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
		engine.disconnect();
	}

	@Test
	public void testTagFiltering() throws Exception {
		engine.configure(conf, bgTasks);
		long curr = 1497720452566L;
		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";

		for (int i = 1; i <= 3; i++) {
			List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("p").setTagValue(String.valueOf(i)).build(),
					Tag.newBuilder().setTagKey("k").setTagValue(String.valueOf(i + 7)).build());
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, tagd, curr, 2 * i), false);
		}

		for (int i = 1; i <= 3; i++) {
			List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("p").setTagValue(String.valueOf(i)).build(),
					Tag.newBuilder().setTagKey("k").setTagValue(String.valueOf(i + 12)).build());
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName + "2", tagd, curr, 2 * i), false);
		}
		Set<String> tags = engine.getTagKeysForMeasurement(dbName, measurementName);
		System.out.println("Tags:" + tags);
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
		engine.disconnect();
	}

	@Test
	public void testAddAndReadDataPointsWithTagFilters() throws Exception {
		engine.configure(conf, bgTasks);
		long curr = 1497720452566L;
		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		String tag = "host123123";

		for (int i = 1; i <= 3; i++) {
			List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey(tag).setTagValue(String.valueOf(i)).build(),
					Tag.newBuilder().setTagKey(tag).setTagValue(String.valueOf(i + 1)).build());
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, tagd, curr + i, 2 * i), false);
		}
		assertEquals(1, engine.getAllMeasurementsForDb(dbName).size());

		SimpleTagFilter filter1 = new SimpleTagFilter(FilterType.EQUALS, "host123123", "1");
		SimpleTagFilter filter2 = new SimpleTagFilter(FilterType.EQUALS, "host123123", "2");

		List<Series> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, curr, curr + 3,
				new ComplexTagFilter(ComplexFilterType.OR, Arrays.asList(filter1, filter2)), null, null);
		assertEquals(2, queryDataPoints.size());
		int i = 1;
		assertEquals(1, queryDataPoints.iterator().next().getDataPoints().size());
		queryDataPoints.sort(new Comparator<Series>() {

			@Override
			public int compare(Series o1, Series o2) {
				return o1.getTags().toString().compareTo(o2.getTags().toString());
			}
		});
		for (Series list : queryDataPoints) {
			for (DataPoint dataPoint : list.getDataPoints()) {
				assertEquals(curr + i, dataPoint.getTimestamp());
				i++;
			}
		}
		Set<String> tags = engine.getTagKeysForMeasurement(dbName, measurementName);
		assertEquals(new HashSet<>(Arrays.asList(tag)), tags);
		assertEquals(new HashSet<>(Arrays.asList("1", "2", "3", "4")),
				engine.getTagValuesForMeasurement(dbName, measurementName, tag));
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
		engine.disconnect();
	}

	@Test
	public void testAddAndReadDataPoints() throws Exception {
		engine.configure(conf, bgTasks);
		long curr = System.currentTimeMillis();

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		try {
			engine.writeDataPoint(dbName, measurementName, valueFieldName, null, curr, 2 * 0, false);
			fail("Must reject the above datapoint due to missing tags");
		} catch (Exception e) {
		}
		Tag tag = Tag.newBuilder().setTagKey("host").setTagValue("123123").build();
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPointLocked(MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName,
					Arrays.asList(tag), curr + i, 2 * i), false);
		}
		assertEquals(1, engine.getAllMeasurementsForDb(dbName).size());
		List<Series> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, curr, curr + 3,
				null);
		assertEquals(1, queryDataPoints.size());
		int i = 1;
		assertEquals(3, queryDataPoints.iterator().next().getDataPoints().size());
		List<List<DataPoint>> output = new ArrayList<>();
		for (Series series : queryDataPoints) {
			output.add(series.getDataPoints());
		}
		for (List<DataPoint> list : output) {
			for (DataPoint dataPoint : list) {
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
		engine.disconnect();
	}

	@Test
	public void testCompaction() throws IOException, InterruptedException {
		conf.put("default.bucket.size", "409600");
		conf.put("compaction.enabled", "true");
		conf.put("use.query.pool", "false");
		conf.put("compaction.codec", "gorilla");
		conf.put("compaction.delay", "1");
		conf.put("compaction.frequency", "1");
		engine.configure(conf, bgTasks);
		final long curr = 1497720652566L;

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("host").setTagValue("123123").build());
		for (int i = 1; i <= 10000; i++) {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName), tags,
							curr + i * 1000, Arrays.asList(Double.doubleToLongBits(i * 1.1)), Arrays.asList(true)),
					false);
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
		SeriesFieldMap t = m.getOrCreateSeriesFieldMap(tags, false);
		TimeSeries series = t.getOrCreateSeriesLocked(valueFieldName, 409600, false, m);
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
		conf.put("default.bucket.size", "409600");
		conf.put("compaction.enabled", "true");
		conf.put("use.query.pool", "false");
		engine.configure(conf, bgTasks);
		final long curr = 1497720652566L;

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("host").setTagValue("123123").build());
		for (int i = 1; i <= 10000; i++) {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint(dbName, measurementName, Arrays.asList(valueFieldName), tags,
							curr + i * 1000, Arrays.asList(Double.doubleToLongBits(i * 1.1)), Arrays.asList(true)),
					false);
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
		SeriesFieldMap t = m.getOrCreateSeriesFieldMap(tags, false);
		final TimeSeries series = t.getOrCreateSeriesLocked(valueFieldName, 409600, false, m);
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
				series.addDataPointUnlocked(TimeUnit.MILLISECONDS, curr + 1000 * 10001, 1.11);
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
