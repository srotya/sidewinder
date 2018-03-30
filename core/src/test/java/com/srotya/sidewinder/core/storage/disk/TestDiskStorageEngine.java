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
package com.srotya.sidewinder.core.storage.disk;

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
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
import com.srotya.sidewinder.core.storage.DBMetadata;
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

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Unit tests for {@link DiskStorageEngine}
 * 
 * @author ambud
 */
public class TestDiskStorageEngine {

	public static ScheduledExecutorService bgTasks;

	@BeforeClass
	public static void before() {
		bgTasks = Executors.newScheduledThreadPool(1, new BackgrounThreadFactory("te1"));
	}

	@AfterClass
	public static void after() {
		bgTasks.shutdown();
	}

	@Test
	public void testInvalidArchiver() throws IOException {
		MiscUtils.delete(new File("target/dst-1/data"));
		StorageEngine engine = new DiskStorageEngine();
		Map<String, String> conf = new HashMap<>();
		conf.put("data.dir", "target/dst-1/data");
		conf.put("data.dir", "target/dst-1/index");
		conf.put(StorageEngine.ARCHIVER_CLASS, "asdasd");
		engine.configure(conf, bgTasks);
	}

	@Test
	public void testUpdateTimeSeriesRetention() throws IOException {
		MiscUtils.delete(new File("target/dst-2/"));
		DiskStorageEngine engine = new DiskStorageEngine();
		Map<String, String> conf = new HashMap<>();
		conf.put("data.dir", "target/dst-2/data");
		conf.put("index.dir", "target/dst-2/index");
		engine.configure(conf, bgTasks);
		engine.getOrCreateMeasurement("db1", "m1");
		engine.updateDefaultTimeSeriesRetentionPolicy("db1", 10);
		assertEquals(10, engine.getDbMetadataMap().get("db1").getRetentionHours());
		Measurement m = engine.getOrCreateMeasurement("db1", "m1");
		SeriesFieldMap s = m.getOrCreateSeriesFieldMap(Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build()));
		TimeSeries ts = s.getOrCreateSeries("vf1", 4096, false, m);
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
		MiscUtils.delete(new File("target/dst-3/"));
		StorageEngine engine = new DiskStorageEngine();
		Map<String, String> conf = new HashMap<>();
		conf.put("data.dir", "target/dst-3/data");
		conf.put("index.dir", "target/dst-3/index");
		engine.configure(conf, bgTasks);
		
		Measurement m = engine.getOrCreateMeasurement("db1", "m1");
		SeriesFieldMap s = m.getOrCreateSeriesFieldMap(Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build()));
		s.getOrCreateSeries("vf1", 4096, false, m);
		
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
		MiscUtils.delete(new File("target/dst-4/data"));
		StorageEngine engine = new DiskStorageEngine();
		Map<String, String> conf = new HashMap<>();
		conf.put("data.dir", "target/dst-4/data");
		conf.put("index.dir", "target/dst-4/index");
		engine.configure(conf, bgTasks);
		Measurement m = engine.getOrCreateMeasurement("db1", "m1");
		SeriesFieldMap s = m.getOrCreateSeriesFieldMap(Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build()));
		s.getOrCreateSeries("vf1", 4096, false, m);
		m = engine.getOrCreateMeasurement("db1", "t1");
		s = m.getOrCreateSeriesFieldMap(Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build()));
		s.getOrCreateSeries("vf1", 4096, false, m);
		
		Set<String> measurementsLike = engine.getMeasurementsLike("db1", "m.*");
		assertEquals(1, measurementsLike.size());
		assertEquals(2, engine.getAllMeasurementsForDb("db1").size());
	}

	@Test
	public void testConcurrentOperations() throws Exception {
		MiscUtils.delete(new File("target/dst-6/data"));
		final StorageEngine engine = new DiskStorageEngine();
		Map<String, String> conf = new HashMap<>();
		conf.put("data.dir", "target/dst-6/data");
		conf.put("index.dir", "target/dst-6/index");
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
						engine.writeDataPoint(dp);
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
	public void testMultipleDrives() throws ItemNotFoundException, IOException {
		StorageEngine engine = new DiskStorageEngine();
		HashMap<String, String> map = new HashMap<>();
		MiscUtils.delete(new File("targer/db10221/"));
		map.put("index.dir", "target/db10221/index");
		map.put("data.dir", "target/db10221/data1, target/db10221/data2");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("e").build());
		try {
			engine.configure(map, bgTasks);
		} catch (IOException e) {
			e.printStackTrace();
			fail("No IOException should be thrown");
		}
		long ts = System.currentTimeMillis();
		try {
			for (int i = 0; i < 10; i++) {
				engine.writeDataPoint(MiscUtils.buildDataPoint("test" + i, "ss", "value", tagd, ts, 2.2));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
		assertEquals(5, new File("target/db10221/data1").listFiles().length);
		assertEquals(5, new File("target/db10221/data2").listFiles().length);
	}

	@Test
	public void testConfigureTimeBuckets() throws ItemNotFoundException, IOException {
		StorageEngine engine = new DiskStorageEngine();
		HashMap<String, String> map = new HashMap<>();
		MiscUtils.delete(new File("targer/db101/"));
		map.put("index.dir", "target/db101/index");
		map.put("data.dir", "target/db101/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		long ts = System.currentTimeMillis();
		map.put(StorageEngine.DEFAULT_BUCKET_SIZE, String.valueOf(4096 * 10));
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("e").build());
		try {
			engine.configure(map, bgTasks);
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			for (int i = 0; i < 10; i++) {
				engine.writeDataPoint(
						MiscUtils.buildDataPoint("test", "ss", "value", tagd, ts + (i * 4096 * 1000), 2.2));
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
		StorageEngine engine = new DiskStorageEngine();
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("e").build());
		try {
			engine.writeDataPoint(
					MiscUtils.buildDataPoint("test", "ss", "value", tagd, System.currentTimeMillis(), 2.2));
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
			engine.writeDataPoint(
					MiscUtils.buildDataPoint("test", "ss", "value", tagd, System.currentTimeMillis(), 2.2));
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
	public void testQueryDataPointsRecovery() throws Exception {
		try {
			List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("e").build());
			DiskStorageEngine engine = new DiskStorageEngine();
			File file = new File("target/db201/");
			MiscUtils.delete(file);
			MiscUtils.ls(file);
			Map<String, String> map = new HashMap<>();
			map.put("index.dir", "target/db201/index");
			map.put("data.dir", "target/db201/data");
			map.put("disk.compression.class", ByzantineWriter.class.getName());
			engine.configure(map, bgTasks);
			long ts = System.currentTimeMillis();
			Map<String, Measurement> db = engine.getOrCreateDatabase("test3", 24);
			assertEquals(0, db.size());
			engine.writeDataPoint(MiscUtils.buildDataPoint("test3", "cpu", "value", tagd, ts, 1));
			engine.writeDataPoint(MiscUtils.buildDataPoint("test3", "cpu", "value", tagd, ts + (400 * 60000), 4));
			Measurement measurement = engine.getOrCreateMeasurement("test3", "cpu");
			assertEquals(1, measurement.getSeriesKeys().size());
			MiscUtils.ls(file);
			engine = new DiskStorageEngine();
			engine.configure(map, bgTasks);

			assertTrue(!engine.isMeasurementFieldFP("test3", "cpu", "value"));
			List<Series> queryDataPoints = engine.queryDataPoints("test3", "cpu", "value", ts, ts + (400 * 60000), null,
					null);
			try {
				engine.isMeasurementFieldFP("test3", "test", "test");
				fail("Measurement should not exist");
			} catch (Exception e) {
			}
			assertEquals(1, queryDataPoints.size());
			assertEquals(2, queryDataPoints.iterator().next().getDataPoints().size());
			assertEquals(ts, queryDataPoints.iterator().next().getDataPoints().get(0).getTimestamp());
			assertEquals(ts + (400 * 60000), queryDataPoints.iterator().next().getDataPoints().get(1).getTimestamp());
			try {
				engine.dropDatabase("test3");
			} catch (Exception e) {
				e.printStackTrace();
				fail("Database delete must succeed");
			}
			assertTrue(!new File("target/db201/data/test3").exists());
			assertEquals(0, engine.getOrCreateMeasurement("test3", "cpu").getSeriesKeys().size());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Test
	public void testQueryDataPoints() throws IOException, ItemNotFoundException {
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("e").build());
		StorageEngine engine = new DiskStorageEngine();
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
		engine.writeDataPoint(MiscUtils.buildDataPoint("test3", "cpu", "value", tagd, ts, 1));
		engine.writeDataPoint(MiscUtils.buildDataPoint("test3", "cpu", "value", tagd, ts + (400 * 60000), 4));
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
		StorageEngine engine = new DiskStorageEngine();
		MiscUtils.delete(new File("target/dbgc/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("index.dir", "target/dbgc/index");
		map.put("data.dir", "target/dbgc/data");
		map.put(StorageEngine.GC_DELAY, "1");
		map.put(StorageEngine.GC_FREQUENCY, "10");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		map.put(StorageEngine.DEFAULT_BUCKET_SIZE, "4096");
		map.put(DiskMalloc.CONF_MEASUREMENT_INCREMENT_SIZE, "4096");
		map.put(DiskMalloc.CONF_MEASUREMENT_FILE_INCREMENT, "10240");
		map.put(DiskMalloc.CONF_MEASUREMENT_FILE_MAX, String.valueOf(1024 * 100));
		map.put(StorageEngine.RETENTION_HOURS, "28");
		engine.configure(map, bgTasks);
		long base = 1497720452566L;
		long ts = base;
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build());
		for (int i = 320; i >= 0; i--) {
			engine.writeDataPoint(MiscUtils.buildDataPoint("test", "cpu2", "value", tagd, base - (3600_000 * i), 2L));
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
		StorageEngine engine = new DiskStorageEngine();
		MiscUtils.delete(new File("target/db1/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db1/mdq");
		map.put("index.dir", "target/db1/index");
		map.put("data.dir", "target/db1/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
		engine.writeDataPoint(MiscUtils.buildDataPoint("test", "cpu", "value", tagd, System.currentTimeMillis(), 2L));
		engine.writeDataPoint(
				MiscUtils.buildDataPoint("test", "mem", "value", tagd, System.currentTimeMillis() + 10, 3L));
		engine.writeDataPoint(
				MiscUtils.buildDataPoint("test", "netm", "value", tagd, System.currentTimeMillis() + 20, 5L));
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
		timeSeries.addValue(headerTimestamp, 1L);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test=1"), points, timeSeries, null, null, false);
		assertEquals(1, points.size());
		points.clear();

		Predicate timepredicate = new BetweenPredicate(Long.MAX_VALUE, Long.MAX_VALUE);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test=1"), points, timeSeries, timepredicate, null, false);
		assertEquals(0, points.size());
	}

	@Test
	public void testSeriesBucketLookups() throws IOException, ItemNotFoundException {
		MiscUtils.delete(new File("targer/db1/"));
		DiskStorageEngine engine = new DiskStorageEngine();
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db1/mdq");
		map.put("index.dir", "target/db1/index");
		map.put("data.dir", "target/db1/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
		engine.connect();
		String dbName = "test1";
		String measurementName = "cpu";
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build());

		long ts = 1483923600000L;
		System.out.println("Base timestamp=" + new Date(ts));

		for (int i = 0; i < 100; i++) {
			engine.writeDataPoint(
					MiscUtils.buildDataPoint(dbName, measurementName, "value", tagd, ts + (i * 60000), 2.2));
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
		String pathname = "target/bas-t-writer/";
		MiscUtils.delete(new File(pathname));
		DiskStorageEngine engine = new DiskStorageEngine();
		HashMap<String, String> map = new HashMap<>();
		map.put("compression.class", ByzantineWriter.class.getName());
		map.put("data.dir", pathname + "/data");
		map.put("index.dir", pathname + "/index");
		engine.configure(map, bgTasks);
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
						engine.writeDataPoint(
								MiscUtils.buildDataPoint("test", "helo" + p, "value", tagd, ts + i * 60, ts + i));
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
		DiskStorageEngine engine = new DiskStorageEngine();
		File file = new File("target/db8/");
		if (file.exists()) {
			MiscUtils.delete(file);
		}
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db8/mdq");
		map.put("index.dir", "target/db8/index");
		map.put("data.dir", "target/db8/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
		long curr = 1497720452566L;

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		try {
			engine.writeDataPoint(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, null, curr, 2.2 * 0));
			fail("Must reject the above datapoint due to missing tags");
		} catch (Exception e) {
		}
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey(dbName).setTagValue("1").build());
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, tagd, curr + i, 2.2 * i));
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
		DiskStorageEngine engine = new DiskStorageEngine();
		MiscUtils.delete(new File("target/db121/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("index.dir", "target/db121/index");
		map.put("data.dir", "target/db121/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
		long curr = 1497720452566L;
		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";

		for (int i = 1; i <= 3; i++) {
			List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("p").setTagValue(String.valueOf(i)).build(),
					Tag.newBuilder().setTagKey("k").setTagValue(String.valueOf(i + 7)).build());
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, tagd, curr, 2 * i));
		}

		for (int i = 1; i <= 3; i++) {
			List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("p").setTagValue(String.valueOf(i)).build(),
					Tag.newBuilder().setTagKey("k").setTagValue(String.valueOf(i + 12)).build());
			engine.writeDataPoint(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName + "2", tagd, curr, 2 * i));
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
		DiskStorageEngine engine = new DiskStorageEngine();
		MiscUtils.delete(new File("target/db5/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db5/mdq");
		map.put("index.dir", "target/db5/index");
		map.put("data.dir", "target/db5/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
		long curr = 1497720452566L;
		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		String tag = "host123123";

		for (int i = 1; i <= 3; i++) {
			List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey(tag).setTagValue(String.valueOf(i)).build(),
					Tag.newBuilder().setTagKey(tag).setTagValue(String.valueOf(i + 1)).build());
			engine.writeDataPoint(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, tagd, curr + i, 2 * i));
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
		DiskStorageEngine engine = new DiskStorageEngine();
		MiscUtils.delete(new File("target/db19/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db19/mdq");
		map.put("index.dir", "target/db19/index");
		map.put("data.dir", "target/db19/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
		long curr = System.currentTimeMillis();

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		try {
			engine.writeDataPoint(dbName, measurementName, valueFieldName, null, curr, 2 * 0);
			fail("Must reject the above datapoint due to missing tags");
		} catch (Exception e) {
		}
		Tag tag = Tag.newBuilder().setTagKey("host").setTagValue("123123").build();
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, Arrays.asList(tag),
					curr + i, 2 * i));
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
	public void testXXHash() {
		XXHashFactory hash = XXHashFactory.fastestInstance();
		XXHash32 h = hash.hash32();
		for (int i = 0; i < 10_000_000; i++) {
			String str = "hostname=host_" + String.valueOf(i % 10000)
					+ ",region=us-west-2,datacenter=us-west-2a,rack=26,os=Ubuntu16.10,arch=x64,team=CHI,service=15,service_version=0,service_environment=staging"
					+ i;
			byte[] bytes = str.getBytes();
			h.hash(bytes, 0, bytes.length, 51);
			str.hashCode();
		}
	}

}
