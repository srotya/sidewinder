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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.srotya.sidewinder.core.filters.AndFilter;
import com.srotya.sidewinder.core.filters.ContainsFilter;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.filters.OrFilter;
import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.SeriesQueryOutput;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.TimeSeriesBucket;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;
import com.srotya.sidewinder.core.utils.MiscUtils;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * Unit tests for {@link DiskStorageEngine}
 * 
 * @author ambud
 */
public class TestDiskStorageEngine {

	public static ScheduledExecutorService bgTasks;

	@BeforeClass
	public static void before() {
		bgTasks = Executors.newScheduledThreadPool(1);
	}

	@AfterClass
	public static void after() {
		bgTasks.shutdown();
	}

	public void testWritePerformance() throws Exception {
		final StorageEngine engine = new DiskStorageEngine();
		// FileUtils.forceDelete(new File("target/db3/"));
		MiscUtils.delete(new File("targer/db3/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db3/mdq");
		map.put("index.dir", "target/db3/index");
		map.put("data.dir", "target/db3/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
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
							DataPoint dp = MiscUtils.buildDataPoint("test" + j, "cpu" + (i % modulator), "value",
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
				Set<SeriesQueryOutput> queryDataPoints = engine.queryDataPoints(dbName, measurementName, "value",
						timeMillis - (3600_000 * 2000), timeMillis + (3600_000 * 2000), Arrays.asList("test2"), null);
				assertEquals(1, queryDataPoints.size());
				assertEquals(count / modulator, queryDataPoints.iterator().next().getDataPoints().size());
			}
		}
		engine.disconnect();
	}

	@Test
	public void testMultipleDrives() throws ItemNotFoundException, IOException {
		StorageEngine engine = new DiskStorageEngine();
		HashMap<String, String> map = new HashMap<>();
		MiscUtils.delete(new File("targer/db10221/"));
		map.put("index.dir", "target/db10221/index");
		map.put("data.dir", "target/db10221/data1, target/db10221/data2");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		try {
			engine.configure(map, bgTasks);
		} catch (IOException e) {
			e.printStackTrace();
			fail("No IOException should be thrown");
		}
		long ts = System.currentTimeMillis();
		try {
			for (int i = 0; i < 10; i++) {
				engine.writeDataPoint(
						MiscUtils.buildDataPoint("test" + i, "ss", "value", Arrays.asList("te"), ts, 2.2));
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
		try {
			engine.configure(map, bgTasks);
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			for (int i = 0; i < 10; i++) {
				engine.writeDataPoint(MiscUtils.buildDataPoint("test", "ss", "value", Arrays.asList("te"),
						ts + (i * 4096 * 1000), 2.2));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
		Set<SeriesQueryOutput> queryDataPoints = engine.queryDataPoints("test", "ss", "value", ts,
				ts + (4096 * 100 * 1000) + 1, Arrays.asList("te"), null);
		assertTrue(queryDataPoints.size() >= 1);
	}

	@Test
	public void testConfigure() throws IOException {
		StorageEngine engine = new DiskStorageEngine();
		try {
			engine.writeDataPoint(MiscUtils.buildDataPoint("test", "ss", "value", Arrays.asList("te"),
					System.currentTimeMillis(), 2.2));
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
			engine.writeDataPoint(MiscUtils.buildDataPoint("test", "ss", "value", Arrays.asList("te"),
					System.currentTimeMillis(), 2.2));
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
	public void testQueryDataPointsRecovery() throws IOException, ItemNotFoundException {
		try {
			DiskStorageEngine engine = new DiskStorageEngine();
			MiscUtils.delete(new File("target/db1/"));
			HashMap<String, String> map = new HashMap<>();
			map.put("metadata.dir", "target/db1/mdq");
			map.put("index.dir", "target/db1/index");
			map.put("data.dir", "target/db1/data");
			map.put("disk.compression.class", ByzantineWriter.class.getName());
			engine.configure(map, bgTasks);
			long ts = System.currentTimeMillis();
			Map<String, Measurement> db = engine.getOrCreateDatabase("test3", 24);
			assertEquals(0, db.size());
			engine.writeDataPoint(MiscUtils.buildDataPoint("test3", "cpu", "value", Arrays.asList("test"), ts, 1));
			engine.writeDataPoint(
					MiscUtils.buildDataPoint("test3", "cpu", "value", Arrays.asList("test"), ts + (400 * 60000), 4));
			Measurement measurement = engine.getOrCreateMeasurement("test3", "cpu");
			assertEquals(1, measurement.getTimeSeriesMap().size());

			engine = new DiskStorageEngine();
			engine.configure(map, bgTasks);

			assertTrue(!engine.isMeasurementFieldFP("test3", "cpu", "value"));
			Set<SeriesQueryOutput> queryDataPoints = engine.queryDataPoints("test3", "cpu", "value", ts,
					ts + (400 * 60000), null, null);
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
			}
			assertEquals(0, engine.getOrCreateMeasurement("test3", "cpu").getTimeSeriesMap().size());

			engine = new DiskStorageEngine();
			engine.configure(map, bgTasks);

			assertEquals(0, engine.getOrCreateMeasurement("test3", "cpu").getTimeSeriesMap().size());
			engine.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Test
	public void testQueryDataPoints() throws IOException, ItemNotFoundException {
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
		engine.writeDataPoint(MiscUtils.buildDataPoint("test3", "cpu", "value", Arrays.asList("test"), ts, 1));
		engine.writeDataPoint(
				MiscUtils.buildDataPoint("test3", "cpu", "value", Arrays.asList("test"), ts + (400 * 60000), 4));
		assertEquals(1, engine.getOrCreateMeasurement("test3", "cpu").getTimeSeriesMap().size());
		Set<SeriesQueryOutput> queryDataPoints = engine.queryDataPoints("test3", "cpu", "value", ts, ts + (400 * 60000),
				null, null);
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
		assertEquals(0, engine.getOrCreateMeasurement("test3", "cpu").getTimeSeriesMap().size());
		engine.disconnect();
	}

	@Test
	public void testGarbageCollector() throws Exception {
		StorageEngine engine = new DiskStorageEngine();
		MiscUtils.delete(new File("target/db99/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("index.dir", "target/db99/index");
		map.put("data.dir", "target/db99/data");
		map.put(StorageEngine.GC_DELAY, "10");
		map.put(StorageEngine.GC_FREQUENCY, "100");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
		long base = 1497720452566L;
		long ts = base;
		for (int i = 32; i >= 0; i--) {
			engine.writeDataPoint(
					MiscUtils.buildDataPoint("test", "cpu", "value", Arrays.asList("test"), base - (3600_000 * i), 2L));
		}
		Thread.sleep(1000);

		Set<SeriesQueryOutput> queryDataPoints = engine.queryDataPoints("test", "cpu", "value", ts - (3600_000 * 32),
				ts, null, null);
		assertTrue(!engine.isMeasurementFieldFP("test", "cpu", "value"));
		assertEquals(27, queryDataPoints.iterator().next().getDataPoints().size());
	}

	@Test
	public void testGetMeasurementsLike() throws Exception {
		StorageEngine engine = new DiskStorageEngine();
		MiscUtils.delete(new File("target/db1/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db1/mdq");
		map.put("index.dir", "target/db1/index");
		map.put("data.dir", "target/db1/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
		engine.writeDataPoint(MiscUtils.buildDataPoint("test", "cpu", "value", Arrays.asList("test"),
				System.currentTimeMillis(), 2L));
		engine.writeDataPoint(MiscUtils.buildDataPoint("test", "mem", "value", Arrays.asList("test"),
				System.currentTimeMillis() + 10, 3L));
		engine.writeDataPoint(MiscUtils.buildDataPoint("test", "netm", "value", Arrays.asList("test"),
				System.currentTimeMillis() + 20, 5L));
		Set<String> result = engine.getMeasurementsLike("test", " ");
		assertEquals(3, result.size());

		result = engine.getMeasurementsLike("test", "c");
		assertEquals(1, result.size());

		result = engine.getMeasurementsLike("test", "m");
		assertEquals(2, result.size());
		engine.disconnect();
	}

	@Test
	public void testSeriesToDataPointConversion() throws IOException {
		List<DataPoint> points = new ArrayList<>();
		long headerTimestamp = System.currentTimeMillis();
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db1/mdq");
		map.put("index.dir", "target/db1/index");
		map.put("data.dir", "target/db1/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		ByteBuffer buf = ByteBuffer.allocate(100);
		TimeSeriesBucket timeSeries = new TimeSeriesBucket(ByzantineWriter.class.getName(), headerTimestamp, map, buf,
				true);
		timeSeries.addDataPoint(headerTimestamp, 1L);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test"), points, timeSeries, null, null, false);
		assertEquals(1, points.size());
		points.clear();

		Predicate timepredicate = new BetweenPredicate(Long.MAX_VALUE, Long.MAX_VALUE);
		TimeSeries.seriesToDataPoints("value", Arrays.asList("test"), points, timeSeries, timepredicate, null, false);
		assertEquals(0, points.size());
		timeSeries.close();
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
		List<String> tags = Arrays.asList("test");

		long ts = 1483923600000L;
		System.out.println("Base timestamp=" + new Date(ts));

		for (int i = 0; i < 100; i++) {
			engine.writeDataPoint(
					MiscUtils.buildDataPoint(dbName, measurementName, "value", tags, ts + (i * 60000), 2.2));
		}
		System.out.println("Buckets:" + engine.getSeriesMap(dbName, measurementName).size());
		long endTs = ts + 99 * 60000;

		// validate all points are returned with a full range query
		Set<SeriesQueryOutput> points = engine.queryDataPoints(dbName, measurementName, "value", ts, endTs, tags, null);
		assertEquals(ts, points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals(endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate ts-1 yields the same result
		points = engine.queryDataPoints(dbName, measurementName, "value", ts - 1, endTs, tags, null);
		assertEquals(ts, points.iterator().next().getDataPoints().get(0).getTimestamp());
		System.out.println("Value count:" + points.iterator().next().getDataPoints().size());
		assertEquals(endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate ts+1 yields correct result
		points = engine.queryDataPoints(dbName, measurementName, "value", ts + 1, endTs, tags, null);
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
		points = engine.queryDataPoints(dbName, measurementName, "value", ts, baseTs2, tags, null);
		assertEquals("Invalid first entry:" + new Date(points.iterator().next().getDataPoints().get(0).getTimestamp()),
				ts, points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + (baseTs2 - ts), (baseTs2 / 60000) * 60000, points.iterator().next()
				.getDataPoints().get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		points = engine.queryDataPoints(dbName, measurementName, "value", baseTs2, endTs, tags, null);
		assertEquals("Invalid first entry:" + new Date(points.iterator().next().getDataPoints().get(0).getTimestamp()),
				(baseTs2 - ts), (baseTs2 / 60000) * 60000,
				points.iterator().next().getDataPoints().get(0).getTimestamp());
		assertEquals("Invalid first entry:" + endTs, endTs, points.iterator().next().getDataPoints()
				.get(points.iterator().next().getDataPoints().size() - 1).getTimestamp());

		// validate correct results when time range is incorrectly swapped i.e.
		// end time is smaller than start time
		points = engine.queryDataPoints(dbName, measurementName, "value", endTs - 1, baseTs2, tags, null);
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

		final long ts1 = System.currentTimeMillis();
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 10; k++) {
			final int p = k;
			es.submit(() -> {
				long ts = System.currentTimeMillis();
				for (int i = 0; i < 1000; i++) {
					try {
						engine.writeDataPoint(MiscUtils.buildDataPoint("test", "helo" + p, "value", Arrays.asList(""),
								ts + i * 60, ts + i));
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
		long curr = System.currentTimeMillis();

		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		try {
			engine.writeDataPoint(
					MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, null, curr, 2.2 * 0));
			fail("Must reject the above datapoint due to missing tags");
		} catch (RejectException e) {
		}
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName,
					Arrays.asList(dbName), curr + i, 2.2 * i));
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
		MiscUtils.delete(new File("target/db1/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("metadata.dir", "target/db1/mdq");
		map.put("index.dir", "target/db1/index");
		map.put("data.dir", "target/db1/data");
		map.put(StorageEngine.PERSISTENCE_DISK, "true");
		engine.configure(map, bgTasks);
		long curr = System.currentTimeMillis();
		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";

		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName,
					Arrays.asList(String.valueOf(i), String.valueOf(i + 7)), curr, 2 * i));
		}

		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName + "2",
					Arrays.asList(String.valueOf(i), String.valueOf(i + 12)), curr, 2 * i));
		}
		Set<String> tags = engine.getTagsForMeasurement(dbName, measurementName);
		assertEquals(9, tags.size());
		Set<String> series = engine.getSeriesIdsWhereTags(dbName, measurementName, Arrays.asList(String.valueOf(1)));
		assertEquals(2, series.size());

		Filter<List<String>> tagFilterTree = new OrFilter<>(Arrays.asList(new ContainsFilter<String, List<String>>("1"),
				new ContainsFilter<String, List<String>>("2")));
		series = engine.getTagFilteredRowKeys(dbName, measurementName, valueFieldName, tagFilterTree,
				Arrays.asList("1", "2"));
		assertEquals(2, series.size());

		System.out.println(engine.getTagsForMeasurement(dbName, measurementName));
		tagFilterTree = new AndFilter<>(Arrays.asList(new ContainsFilter<String, List<String>>("1"),
				new ContainsFilter<String, List<String>>("8")));
		series = engine.getTagFilteredRowKeys(dbName, measurementName, valueFieldName, tagFilterTree,
				Arrays.asList("1", "8"));
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
		long curr = System.currentTimeMillis();
		String dbName = "test";
		String measurementName = "cpu";
		String valueFieldName = "value";
		String tag = "host123123";

		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName,
					Arrays.asList(tag + i, tag + (i + 1)), curr + i, 2 * i));
		}
		assertEquals(1, engine.getAllMeasurementsForDb(dbName).size());

		ContainsFilter<String, List<String>> filter1 = new ContainsFilter<String, List<String>>(tag + 1);
		ContainsFilter<String, List<String>> filter2 = new ContainsFilter<String, List<String>>(tag + 2);

		Set<SeriesQueryOutput> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, curr,
				curr + 3, Arrays.asList(tag + 1, tag + 2), new OrFilter<>(Arrays.asList(filter1, filter2)), null, null);
		assertEquals(2, queryDataPoints.size());
		int i = 1;
		assertEquals(1, queryDataPoints.iterator().next().getDataPoints().size());
		for (SeriesQueryOutput list : queryDataPoints) {
			for (DataPoint dataPoint : list.getDataPoints()) {
				assertEquals(curr + i, dataPoint.getTimestamp());
				i++;
			}
		}
		Set<String> tags = engine.getTagsForMeasurement(dbName, measurementName);
		assertEquals(new HashSet<>(Arrays.asList(tag + 1, tag + 2, tag + 3, tag + 4)), tags);
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
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, null, curr, 2 * 0));
			fail("Must reject the above datapoint due to missing tags");
		} catch (RejectException e) {
		}
		String tag = "host123123";
		for (int i = 1; i <= 3; i++) {
			engine.writeDataPoint(MiscUtils.buildDataPoint(dbName, measurementName, valueFieldName, Arrays.asList(tag),
					curr + i, 2 * i));
		}
		assertEquals(1, engine.getAllMeasurementsForDb(dbName).size());
		Set<SeriesQueryOutput> queryDataPoints = engine.queryDataPoints(dbName, measurementName, valueFieldName, curr,
				curr + 3, Arrays.asList(tag), null);
		assertEquals(1, queryDataPoints.size());
		int i = 1;
		assertEquals(3, queryDataPoints.iterator().next().getDataPoints().size());
		List<List<DataPoint>> output = new ArrayList<>();
		for (SeriesQueryOutput series : queryDataPoints) {
			output.add(series.getDataPoints());
		}
		for (List<DataPoint> list : output) {
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
		engine.disconnect();
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
