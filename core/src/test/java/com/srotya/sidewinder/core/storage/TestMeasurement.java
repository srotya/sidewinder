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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.disk.DiskMalloc;
import com.srotya.sidewinder.core.storage.disk.PersistentMeasurement;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemoryMeasurement;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
@RunWith(Parameterized.class)
public class TestMeasurement {

	private Map<String, String> conf = new HashMap<>();
	private DBMetadata metadata = new DBMetadata(28);
	private static ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);
	private static String DBNAME = "test";
	@Parameter
	public Class<Measurement> clazz;
	private Measurement measurement;
	private String indexDir;
	private String dataDir;
	private StorageEngine engine;

	@BeforeClass
	public static void beforeClass() throws IOException {
	}

	@AfterClass
	public static void afterClass() throws IOException {
	}

	@SuppressWarnings("rawtypes")
	@Parameters(name = "{index}: Measurement Impl Class: {0}")
	public static Collection classes() {
		List<Object[]> implementations = new ArrayList<>();
		implementations.add(new Object[] { MemoryMeasurement.class });
		implementations.add(new Object[] { PersistentMeasurement.class });
		return implementations;
	}

	@Before
	public void before() throws InstantiationException, IllegalAccessException, IOException {
		MiscUtils.delete(new File("target/measurement-common"));
		engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgTaskPool);
		dataDir = "target/measurement-common/data";
		indexDir = "target/measurement-common/index";
		measurement = clazz.newInstance();
	}

	@After
	public void after() throws IOException {
		engine.shutdown();
		measurement.close();
		MiscUtils.delete(new File("target/measurement-common"));
	}

	@Test
	public void testConfigure() throws IOException {
		measurement.configure(conf, engine, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		assertTrue(measurement.getTagIndex() != null);
		// TimeSeries series = measurement.getOrCreateTimeSeries("v1",
		// Arrays.asList("test1"), 4096, false, conf);
		measurement.close();
	}

	@Test
	public void testTagEncodingPerformance() throws IOException {
		measurement.configure(conf, engine, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		assertTrue(measurement.getTagIndex() != null);
		for (int i = 0; i < 1_000_000; i++) {
			List<Tag> tags = Arrays.asList(
					Tag.newBuilder().setTagKey("test").setTagValue(String.valueOf("asdasd" + i)).build(),
					Tag.newBuilder().setTagKey("test").setTagValue("2").build());
			measurement.encodeTagsToString(tags);
		}
	}

	public static Point build(String valueFieldName, List<Tag> tags, long ts, long v) {
		return Point.newBuilder().setDbName("").setMeasurementName("").addAllTags(tags).setTimestamp(ts)
				.addValueFieldName(valueFieldName).addValue(v).addFp(false).build();
	}

	public static Point build(String valueFieldName, List<Tag> tags, long ts, double v) {
		return Point.newBuilder().setDbName("").setMeasurementName("").addAllTags(tags).setTimestamp(ts)
				.addValueFieldName(valueFieldName).addValue(Double.doubleToLongBits(v)).addFp(true).build();
	}

	@Test
	public void testDataPointsQuery() throws Exception {
		long ts = System.currentTimeMillis();
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		conf.put(DiskMalloc.CONF_MEASUREMENT_FILE_MAX, String.valueOf(2 * 1024 * 1024));
		measurement.configure(conf, null, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		int LIMIT = 1000;
		for (int i = 0; i < LIMIT; i++) {
			measurement.addPointLocked(build("value1", tags, ts + i * 1000, 1L), false);
		}
		for (int i = 0; i < LIMIT; i++) {
			measurement.addPointLocked(build("value2", tags, ts + i * 1000, 1L), false);
		}
		List<SeriesOutput> resultMap = new ArrayList<>();
		measurement.queryDataPoints("value.*$", ts, ts + 1000 * LIMIT, null, null, resultMap);
		assertEquals(2, resultMap.size());
		for (SeriesOutput s : resultMap) {
			for (int i = 0; i < s.getDataPoints().size(); i++) {
				DataPoint dataPoint = s.getDataPoints().get(i);
				assertEquals(ts + i * 1000, dataPoint.getTimestamp());
				assertEquals(1L, dataPoint.getLongValue());
			}
		}

		List<List<Tag>> tagsResult = measurement.getTagsForMeasurement();
		Collections.sort(tags, Measurement.TAG_COMPARATOR);
		for (List<Tag> list : tagsResult) {
			Set<Tag> hashSet = new HashSet<>(list);
			for (int i = 0; i < tags.size(); i++) {
				Tag tag = tags.get(i);
				assertTrue(hashSet.contains(tag));
			}
		}

		try {
			tagsResult = measurement.getTagsForMeasurement();
		} catch (IOException e) {
		}

		measurement.close();
	}

	@Test
	public void testOptimizationsLambdaInvoke() throws IOException {
		long ts = System.currentTimeMillis();
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		conf.put(DiskMalloc.CONF_MEASUREMENT_FILE_MAX, String.valueOf(2 * 1024 * 1024));
		measurement.configure(conf, null, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		int LIMIT = 1000;
		for (int i = 0; i < LIMIT; i++) {
			measurement.addPointLocked(build("value1", tags, ts + i * 1000, 1L), false);
		}
		measurement.runCleanupOperation("print", s -> {
			// don't cleanup anything
			return new ArrayList<>();
		});
	}

	@Test
	public void testCompactionEmptyLineValidation() throws IOException {
		final long ts = 1484788896586L;
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		conf.put(DiskMalloc.CONF_MEASUREMENT_FILE_MAX, String.valueOf(2 * 1024 * 1024));
		conf.put("malloc.ptrfile.increment", String.valueOf(256));
		conf.put("compaction.ratio", "1.2");
		measurement.configure(conf, null, 1024, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		int LIMIT = 34500;
		for (int i = 0; i < LIMIT; i++) {
			measurement.addPointLocked(build("value1", tags, ts + i * 100, 1.2), false);
		}
		// measurement.collectGarbage(null);
		System.err.println("Gc complete");
		measurement.compact();
		measurement.getSeriesList().iterator().next();

		measurement.close();

		measurement.configure(conf, null, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
	}

	@Test
	public void testCompaction() throws IOException {
		final long ts = 1484788896586L;
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test2").setTagValue("2").build());
		conf.put(DiskMalloc.CONF_MEASUREMENT_FILE_MAX, String.valueOf(2 * 1024 * 1024));
		conf.put("malloc.ptrfile.increment", String.valueOf(1024));
		TimeField.compactionRatio = 1.2;
		ValueField.compactionRatio = 1.2;
		measurement.configure(conf, null, 1024, DBNAME, "m4", indexDir, dataDir, metadata, bgTaskPool);
		int LIMIT = 12000;
		for (int i = 0; i < LIMIT; i++) {
			measurement.addPointLocked(build("value1", tags, ts + i, 1.2 * i), false);
		}
		assertEquals(1, measurement.getSeriesList().size());
		Series series = measurement.getSeriesList().iterator().next();
		System.out.println("Series:" + series);
		assertEquals(1, series.getBucketMap().size());
		assertEquals(5, MiscUtils.bucketCounter(series));
		assertEquals(2, series.getBucketMap().entrySet().iterator().next().getValue().size());
		// int maxDp = series.getBucketFieldMap().values().stream().flatMap(v ->
		// v.stream()).mapToInt(l -> l.getCount())
		// .max().getAsInt();
		// check and read datapoint count before
		Series seriesField = measurement.getSeriesField(tags);
		List<DataPoint> queryDataPoints = seriesField
				.queryDataPoints(measurement, Arrays.asList("value1"), ts, ts + LIMIT + 1, null).get("value1");
		assertEquals(LIMIT, queryDataPoints.size());
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = queryDataPoints.get(i);
			assertEquals(ts + i, dp.getTimestamp());
			assertEquals(i * 1.2, dp.getValue(), 0.01);
		}
		measurement.compact();

		assertEquals(3, MiscUtils.bucketCounter(series));
		assertEquals(2, series.getBucketMap().entrySet().iterator().next().getValue().size());
		// assertTrue(maxDp <= series.getBucketFieldMap().values().stream().flatMap(v ->
		// v.stream())
		// .mapToInt(l -> l.getCount()).max().getAsInt());
		// validate query after compaction
		seriesField = measurement.getSeriesField(tags);
		queryDataPoints = seriesField.queryDataPoints(measurement, Arrays.asList("value1"), ts, ts + LIMIT + 1, null)
				.get("value1");
		assertEquals(LIMIT, queryDataPoints.size());
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = queryDataPoints.get(i);
			assertEquals(ts + i, dp.getTimestamp());
			assertEquals(i * 1.2, dp.getValue(), 0.01);
		}
		measurement.close();
	}

	@Test
	public void testGCMemoryMapFree() throws IOException, InterruptedException {
		final long ts = 1484788896586L;
		DiskMalloc.debug = true;
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		Map<String, String> map = new HashMap<>();
		map.put("compression.class", "byzantine");
		map.put("compaction.class", "byzantine");
		map.put(DiskMalloc.CONF_MEASUREMENT_FILE_MAX, String.valueOf(512 * 1024));
		map.put("malloc.file.increment", String.valueOf(256 * 1024));
		map.put("malloc.buf.increment", String.valueOf(1024));
		map.put("default.series.retention.hours", String.valueOf(2));
		measurement.configure(map, null, 512, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		int LIMIT = 20000;
		for (int i = 0; i < LIMIT; i++) {
			for (int k = 0; k < 2; k++) {
				measurement.addPointLocked(build("value" + k, tags, ts + i * 10000, i * 1.2), false);
			}
		}

		Series s = measurement.getOrCreateSeries(tags, false);
		// System.out.println(measurement.getOrCreateSeriesFieldMap("value0", tags, 512,
		// false,
		// map).getBucketRawMap().size());
		measurement.collectGarbage(null);
		for (int k = 0; k < 2; k++) {
			List<DataPoint> dps = s
					.queryDataPoints(measurement, Arrays.asList("value" + k), ts, ts + LIMIT * 10000, null)
					.get("value" + k);
			assertEquals(10032, dps.size());
		}
		System.gc();
		Thread.sleep(200);
		for (int k = 0; k < 2; k++) {
			List<DataPoint> dps = s
					.queryDataPoints(measurement, Arrays.asList("value" + k), ts, ts + LIMIT * 10000, null)
					.get("value" + k);
			assertEquals(10032, dps.size());
		}
		measurement.close();
	}

	@Test
	public void testConstructRowKey() throws Exception {
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		measurement.configure(conf, engine, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		ByteString encodeTagsToString = measurement.encodeTagsToString(tags);
		ByteString key = measurement.constructSeriesId(tags);
		assertEquals(encodeTagsToString, key);
		assertEquals("Bad output:" + encodeTagsToString, new ByteString("test=1^test=2"), encodeTagsToString);
		measurement.close();
	}

	@Test
	public void testMeasurementRecovery() throws IOException {
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("t").setTagValue("2").build());
		measurement.configure(conf, engine, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		long t = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			measurement.addPointLocked(build("vf1", tags, t + i * 1000, i), false);
		}
		Series s = measurement.getOrCreateSeries(tags, false);
		List<DataPoint> dps = s.queryDataPoints(measurement, Arrays.asList("vf1"), t, t + 1000 * 100, null).get("vf1");

		assertEquals(100, dps.size());
		for (int i = 0; i < 100; i++) {
			DataPoint dp = dps.get(i);
			assertEquals(t + i * 1000, dp.getTimestamp());
			assertEquals(i, dp.getLongValue());
		}
		List<SeriesOutput> resultMap = new ArrayList<>();
		measurement.queryDataPoints("vf1", t, t + 1000 * 100, null, null, resultMap);
		assertEquals(1, resultMap.size());
		SeriesOutput next = resultMap.iterator().next();
		for (int i = 0; i < next.getDataPoints().size(); i++) {
			DataPoint dp = next.getDataPoints().get(i);
			assertEquals(t + i * 1000, dp.getTimestamp());
			assertEquals(i, dp.getLongValue());
		}
		ConcurrentMap<ByteString, FieldReaderIterator[]> readers = new ConcurrentHashMap<>();
		measurement.queryReaders(Arrays.asList("vf1"), null, false, t, t + 1000 * 100, null, readers);
		assertEquals(1, readers.size());
		for (Entry<ByteString, FieldReaderIterator[]> entry : readers.entrySet()) {
			assertEquals(100, entry.getValue()[0].getReaders().get(0).getCount());
			assertEquals(100, entry.getValue()[1].getReaders().get(0).getCount());
		}
		measurement.close();
	}

	@Test
	public void testLinearizability() throws IOException, InterruptedException {
		String valueFieldName = "vf2";
		for (int p = 0; p < 100; p++) {
			MiscUtils.delete(new File("target/measurement-common"));
			final long t1 = 1497720452566L;
			measurement.configure(conf, engine, 4096, DBNAME, "m2", indexDir, dataDir, metadata, bgTaskPool);
			ExecutorService es = Executors.newFixedThreadPool(2, new BackgrounThreadFactory("tlinear"));
			final List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build(),
					Tag.newBuilder().setTagKey("t").setTagValue("2").build());
			AtomicBoolean wait = new AtomicBoolean(false);
			for (int i = 0; i < 2; i++) {
				final int th = i;
				es.submit(() -> {
					while (!wait.get()) {
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
						}
					}
					long t = t1 + th * 3;
					for (int j = 0; j < 100; j++) {
						try {
							long timestamp = t + j * 1000;
							measurement.addPointLocked(build(valueFieldName, tags, timestamp, j), false);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
			es.shutdown();
			wait.set(true);
			es.awaitTermination(100, TimeUnit.SECONDS);
			Series s = measurement.getOrCreateSeries(tags, false);
			List<DataPoint> dps = s
					.queryDataPoints(measurement, Arrays.asList(valueFieldName), t1 - 120, t1 + 1000_000, null)
					.get(valueFieldName);
			assertEquals(200, dps.size());
			measurement.close();
		}
	}

	@Test
	public void testLinearizabilityWithRollOverBucket() throws IOException, InterruptedException {
		for (int p = 0; p < 2; p++) {
			MiscUtils.delete(new File("target/measurement-common"));
			final int LIMIT = 10000;
			final long t1 = 1497720452566L;
			final List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build(),
					Tag.newBuilder().setTagKey("t").setTagValue("2").build());
			measurement.configure(conf, engine, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
			ExecutorService es = Executors.newFixedThreadPool(2, new BackgrounThreadFactory("tlinear2"));
			AtomicBoolean wait = new AtomicBoolean(false);
			for (int i = 0; i < 2; i++) {
				final int th = i;
				es.submit(() -> {
					while (!wait.get()) {
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
						}
					}
					long t = t1 + th * 3;
					for (int j = 0; j < LIMIT; j++) {
						try {
							long timestamp = t + j * 1000;
							measurement.addPointLocked(build("vf1", tags, timestamp, j), false);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
			es.shutdown();
			wait.set(true);
			es.awaitTermination(10, TimeUnit.SECONDS);
			Series s = measurement.getOrCreateSeries(tags, false);
			List<DataPoint> dps = s.queryDataPoints(measurement, Arrays.asList("vf1"), t1 - 100, t1 + 1000_0000, null)
					.get("vf1");
			assertEquals(LIMIT * 2, dps.size(), 10);
			measurement.close();
		}
	}

}
