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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.storage.mem.SetIndex;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class TestPersistentMeasurement {

	private Map<String, String> conf = new HashMap<>();
	private DBMetadata metadata = new DBMetadata(28);
	private static ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);
	private static StorageEngine engine = new MemStorageEngine();
	private static String DBNAME = "test";

	@BeforeClass
	public static void before() throws IOException {
		engine.configure(new HashMap<>(), bgTaskPool);
	}

	@Test
	public void testConfigure() throws IOException {
		MiscUtils.delete(new File("target/pmeasurement1"));
		Measurement measurement = new PersistentMeasurement();
		measurement.configure(conf, engine, DBNAME, "m1", "target/pmeasurement1/idx", "target/pmeasurement1/data",
				metadata, bgTaskPool);
		assertTrue(measurement.getTagIndex() != null);
		// TimeSeries series = measurement.getOrCreateTimeSeries("v1",
		// Arrays.asList("test1"), 4096, false, conf);
		measurement.close();
	}

	@Test
	public void testDataPointsQuery() throws Exception {
		long ts = System.currentTimeMillis();
		MiscUtils.delete(new File("target/db41/"));
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> map = new HashMap<>();
		map.put("disk.compression.class", ByzantineWriter.class.getName());
		map.put("malloc.file.max", String.valueOf(2 * 1024 * 1024));
		m.configure(map, null, DBNAME, "m1", "target/db41/index", "target/db41/data", metadata, bgTaskPool);
		int LIMIT = 1000;
		for (int i = 0; i < LIMIT; i++) {
			TimeSeries t = m.getOrCreateTimeSeries("value1", tags, 4096, false, map);
			t.addDataPoint(TimeUnit.MILLISECONDS, ts + i * 1000, 1L);
		}
		for (int i = 0; i < LIMIT; i++) {
			TimeSeries t = m.getOrCreateTimeSeries("value2", tags, 4096, false, map);
			t.addDataPoint(TimeUnit.MILLISECONDS, ts + i * 1000, 1L);
		}
		List<Series> resultMap = new ArrayList<>();
		m.queryDataPoints("value.*$", ts, ts + 1000 * LIMIT, null, null, resultMap);
		assertEquals(2, resultMap.size());
		for (Series s : resultMap) {
			for (int i = 0; i < s.getDataPoints().size(); i++) {
				DataPoint dataPoint = s.getDataPoints().get(i);
				assertEquals(ts + i * 1000, dataPoint.getTimestamp());
				assertEquals(1L, dataPoint.getLongValue());
			}
		}

		List<List<Tag>> tagsResult = m.getTagsForMeasurement();
		Collections.sort(tags, Measurement.TAG_COMPARATOR);
		for (List<Tag> list : tagsResult) {
			Set<Tag> hashSet = new HashSet<>(list);
			for (int i = 0; i < tags.size(); i++) {
				Tag tag = tags.get(i);
				assertTrue(hashSet.contains(tag));
			}
		}

		try {
			tagsResult = m.getTagsForMeasurement();
		} catch (IOException e) {
		}

		m.close();
	}

	@Test
	public void testDataPointsRecovery() throws Exception {
		long ts = System.currentTimeMillis();
		MiscUtils.delete(new File("target/db132/"));
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> map = new HashMap<>();
		map.put("disk.compression.class", ByzantineWriter.class.getName());
		map.put("malloc.file.max", String.valueOf(1024 * 1024));
		try {
			m.configure(map, null, DBNAME, "m1", "target/db132/index", "target/db132/data", metadata, bgTaskPool);
			fail("Must throw invalid file max size exception");
		} catch (Exception e) {
		}
		map.put("malloc.file.max", String.valueOf(2 * 1024 * 1024));
		m.configure(map, null, DBNAME, "m1", "target/db132/index", "target/db132/data", metadata, bgTaskPool);
		int LIMIT = 100000;
		for (int i = 0; i < LIMIT; i++) {
			TimeSeries t = m.getOrCreateTimeSeries("value", tags, 4096, false, map);
			t.addDataPoint(TimeUnit.MILLISECONDS, ts + i * 1000, 1L);
		}
		m.close();

		m = new PersistentMeasurement();
		m.configure(map, null, DBNAME, "m1", "target/db132/index", "target/db132/data", metadata, bgTaskPool);
		List<Series> resultMap = new ArrayList<>();
		m.queryDataPoints("value", ts, ts + 1000 * LIMIT, null, null, resultMap);
		Iterator<Series> iterator = resultMap.iterator();
		assertEquals(LIMIT, iterator.next().getDataPoints().size());
		m.close();
	}

	@Test
	public void testDataPointsRecoveryPTR() throws Exception {
		long ts = System.currentTimeMillis();
		MiscUtils.delete(new File("target/db290/"));
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> map = new HashMap<>();
		map.put("disk.compression.class", ByzantineWriter.class.getName());
		map.put("malloc.file.max", String.valueOf(2 * 1024 * 1024));
		map.put("malloc.ptrfile.increment", String.valueOf(2 * 1024));
		m.configure(map, null, DBNAME, "m1", "target/db290/index", "target/db290/data", metadata, bgTaskPool);
		int LIMIT = 100;
		for (int i = 0; i < LIMIT; i++) {
			TimeSeries t = m.getOrCreateTimeSeries("value" + i, tags, 4096, false, map);
			t.addDataPoint(TimeUnit.MILLISECONDS, ts, 1L);
		}
		m.close();

		m = new PersistentMeasurement();
		m.configure(map, null, DBNAME, "m1", "target/db290/index", "target/db290/data", metadata, bgTaskPool);
		List<Series> resultMap = new ArrayList<>();
		m.queryDataPoints("value.*", ts, ts + 1000, null, null, resultMap);
		assertEquals(LIMIT, resultMap.size());
		m.close();
	}

	@Test
	public void testOptimizationsLambdaInvoke() throws IOException {
		long ts = System.currentTimeMillis();
		MiscUtils.delete(new File("target/db42/"));
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> map = new HashMap<>();
		map.put("disk.compression.class", ByzantineWriter.class.getName());
		map.put("malloc.file.max", String.valueOf(2 * 1024 * 1024));
		m.configure(map, null, DBNAME, "m1", "target/db42/index", "target/db42/data", metadata, bgTaskPool);
		int LIMIT = 1000;
		for (int i = 0; i < LIMIT; i++) {
			TimeSeries t = m.getOrCreateTimeSeries("value1", tags, 4096, false, map);
			t.addDataPoint(TimeUnit.MILLISECONDS, ts + i * 1000, 1L);
		}
		m.runCleanupOperation("print", s -> {
			// don't cleanup anything
			return new ArrayList<>();
		});
	}

	@Test
	public void testCompactionEmptyLineValidation() throws IOException {
		final long ts = 1484788896586L;
		MiscUtils.delete(new File("target/db46/"));
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> map = new HashMap<>();
		map.put("disk.compression.class", ByzantineWriter.class.getName());
		map.put("malloc.file.max", String.valueOf(2 * 1024 * 1024));
		map.put("malloc.ptrfile.increment", String.valueOf(256));
		map.put("compaction.ratio", "1.2");
		map.put("compaction.enabled", "true");
		m.configure(map, null, DBNAME, "m1", "target/db46/index", "target/db46/data", metadata, bgTaskPool);
		int LIMIT = 34500;
		for (int i = 0; i < LIMIT; i++) {
			TimeSeries t = m.getOrCreateTimeSeries("value1", tags, 1024, false, map);
			t.addDataPoint(TimeUnit.MILLISECONDS, ts + i * 100, i * 1.2);
		}
		m.collectGarbage(null);
		System.err.println("Gc complete");
		m.compact();
		m.getTimeSeries().iterator().next();
		m = new PersistentMeasurement();
		m.configure(map, null, DBNAME, "m1", "target/db46/index", "target/db46/data", metadata, bgTaskPool);
	}

	@Test
	public void testCompaction() throws IOException {
		final long ts = 1484788896586L;
		MiscUtils.delete(new File("target/db45/"));
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> map = new HashMap<>();
		map.put("disk.compression.class", ByzantineWriter.class.getName());
		map.put("malloc.file.max", String.valueOf(2 * 1024 * 1024));
		map.put("malloc.ptrfile.increment", String.valueOf(1024));
		map.put("compaction.ratio", "1.2");
		map.put("compaction.enabled", "true");
		m.configure(map, null, DBNAME, "m1", "target/db45/index", "target/db45/data", metadata, bgTaskPool);
		int LIMIT = 7000;
		for (int i = 0; i < LIMIT; i++) {
			TimeSeries t = m.getOrCreateTimeSeries("value1", tags, 1024, false, map);
			t.addDataPoint(TimeUnit.MILLISECONDS, ts + i, i * 1.2);
		}
		assertEquals(1, m.getTimeSeries().size());
		TimeSeries series = m.getTimeSeries().iterator().next();
		assertEquals(1, series.getBucketRawMap().size());
		assertEquals(3, series.getBucketCount());
		assertEquals(3, series.getBucketRawMap().entrySet().iterator().next().getValue().size());
		assertEquals(1, series.getCompactionSet().size());
		int maxDp = series.getBucketRawMap().values().stream().flatMap(v -> v.stream()).mapToInt(l -> l.getCount())
				.max().getAsInt();
		// check and read datapoint count before
		List<DataPoint> queryDataPoints = series.queryDataPoints("", ts, ts + LIMIT + 1, null);
		assertEquals(LIMIT, queryDataPoints.size());
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = queryDataPoints.get(i);
			assertEquals(ts + i, dp.getTimestamp());
			assertEquals(i * 1.2, dp.getValue(), 0.01);
		}
		m.compact();
		assertEquals(2, series.getBucketCount());
		assertEquals(2, series.getBucketRawMap().entrySet().iterator().next().getValue().size());
		assertEquals(0, series.getCompactionSet().size());
		assertTrue(maxDp <= series.getBucketRawMap().values().stream().flatMap(v -> v.stream())
				.mapToInt(l -> l.getCount()).max().getAsInt());
		// validate query after compaction
		queryDataPoints = series.queryDataPoints("", ts, ts + LIMIT + 1, null);
		assertEquals(LIMIT, queryDataPoints.size());
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = queryDataPoints.get(i);
			assertEquals(ts + i, dp.getTimestamp());
			assertEquals(i * 1.2, dp.getValue(), 0.01);
		}
		// test buffer recovery after compaction, validate count
		m = new PersistentMeasurement();
		m.configure(map, null, DBNAME, "m1", "target/db45/index", "target/db45/data", metadata, bgTaskPool);
		series = m.getTimeSeries().iterator().next();
		queryDataPoints = series.queryDataPoints("", ts, ts + LIMIT + 1, null);
		assertEquals(LIMIT, queryDataPoints.size());
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = queryDataPoints.get(i);
			assertEquals(ts + i, dp.getTimestamp());
			assertEquals(i * 1.2, dp.getValue(), 0.01);
		}
		for (int i = 0; i < LIMIT; i++) {
			TimeSeries t = m.getOrCreateTimeSeries("value1", tags, 1024, false, map);
			t.addDataPoint(TimeUnit.MILLISECONDS, LIMIT + ts + i, i * 1.2);
		}
		series.getBucketRawMap().entrySet().iterator().next().getValue().stream()
				.map(v -> "" + v.getCount() + ":" + v.isReadOnly() + ":" + (int) v.getRawBytes().get(1))
				.forEach(System.out::println);
		// test recovery again
		m = new PersistentMeasurement();
		m.configure(map, null, DBNAME, "m1", "target/db45/index", "target/db45/data", metadata, bgTaskPool);
		series = m.getTimeSeries().iterator().next();
		queryDataPoints = series.queryDataPoints("", ts - 1, ts + 2 + (LIMIT * 2), null);
		assertEquals(LIMIT * 2, queryDataPoints.size());
		for (int i = 0; i < LIMIT * 2; i++) {
			DataPoint dp = queryDataPoints.get(i);
			assertEquals("Error:" + i + " " + (dp.getTimestamp() - ts - i), ts + i, dp.getTimestamp());
		}
	}

	@Test
	public void testMemoryMapFree() throws IOException, InterruptedException {
		final long ts = 1484788896586L;
		DiskMalloc.debug = true;
		MiscUtils.delete(new File("target/db46/"));
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> map = new HashMap<>();
		map.put("compression.class", "byzantine");
		map.put("compaction.class", "byzantine");
		map.put("malloc.file.max", String.valueOf(512 * 1024));
		map.put("malloc.file.increment", String.valueOf(256 * 1024));
		map.put("malloc.buf.increment", String.valueOf(1024));
		map.put("default.series.retention.hours", String.valueOf(2));
		map.put("compaction.ratio", "1.2");
		map.put("compaction.enabled", "true");
		m.configure(map, null, DBNAME, "m1", "target/db46/index", "target/db46/data", metadata, bgTaskPool);
		int LIMIT = 20000;
		for (int i = 0; i < LIMIT; i++) {
			for (int k = 0; k < 2; k++) {
				TimeSeries t = m.getOrCreateTimeSeries("value" + k, tags, 512, false, map);
				t.addDataPoint(TimeUnit.MILLISECONDS, ts + i * 10000, i * 1.2);
			}
		}

		System.out.println(m.getOrCreateTimeSeries("value0", tags, 512, false, map).getBucketRawMap().size());
		m.collectGarbage(null);
		for (int k = 0; k < 2; k++) {
			TimeSeries t = m.getOrCreateTimeSeries("value" + k, tags, 512, false, map);
			List<DataPoint> dps = t.queryDataPoints("", ts, ts + LIMIT * 10000, null);
			assertEquals(10032, dps.size());
		}
		System.gc();
		Thread.sleep(200);
		for (int k = 0; k < 2; k++) {
			TimeSeries t = m.getOrCreateTimeSeries("value" + k, tags, 512, false, map);
			List<DataPoint> dps = t.queryDataPoints("", ts, ts + LIMIT * 10000, null);
			assertEquals(10032, dps.size());
		}
		m.close();
	}

	@Test
	public void testConstructRowKey() throws Exception {
		MiscUtils.delete(new File("target/db131/"));
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		Measurement m = new PersistentMeasurement();
		m.configure(conf, engine, DBNAME, "m1", "target/db131/index", "target/db131/data", metadata, bgTaskPool);
		TagIndex index = m.getTagIndex();
		ByteString encodeTagsToString = m.encodeTagsToString(index, tags);
		ByteString key = m.constructSeriesId(tags, index);
		assertEquals(encodeTagsToString, key);
		assertEquals("Bad output:"+encodeTagsToString, new ByteString("test=1^test=2"), encodeTagsToString);
		m.close();
	}

	@Test
	public void testMeasurementRecovery() throws IOException {
		MiscUtils.delete(new File("target/db141/"));
		PersistentMeasurement m = new PersistentMeasurement();
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("t").setTagValue("2").build());
		m.configure(conf, engine, DBNAME, "m1", "target/db141/index", "target/db141/data", metadata, bgTaskPool);
		TimeSeries ts = m.getOrCreateTimeSeries("vf1", tags, 4096, false, conf);
		long t = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			ts.addDataPoint(TimeUnit.MILLISECONDS, t + i * 1000, i);
		}
		List<DataPoint> dps = ts.queryDataPoints("vf1", t, t + 1000 * 100, null);
		assertEquals(100, dps.size());
		for (int i = 0; i < 100; i++) {
			DataPoint dp = dps.get(i);
			assertEquals(t + i * 1000, dp.getTimestamp());
			assertEquals(i, dp.getLongValue());
		}
		List<Series> resultMap = new ArrayList<>();
		m.queryDataPoints("vf1", t, t + 1000 * 100, null, null, resultMap);
		assertEquals(1, resultMap.size());
		Series next = resultMap.iterator().next();
		for (int i = 0; i < next.getDataPoints().size(); i++) {
			DataPoint dp = next.getDataPoints().get(i);
			assertEquals(t + i * 1000, dp.getTimestamp());
			assertEquals(i, dp.getLongValue());
		}
		LinkedHashMap<Reader, Boolean> readers = new LinkedHashMap<>();
		m.queryReaders("vf1", t, t + 1000 * 100, readers);
		for (Reader reader : readers.keySet()) {
			assertEquals(100, reader.getPairCount());
		}
		m.close();
	}

	@Test
	public void testLinearizability() throws IOException, InterruptedException {
		for (int p = 0; p < 100; p++) {
			MiscUtils.delete(new File("target/db134/"));
			final long t1 = 1497720452566L;
			Measurement m = new PersistentMeasurement();
			m.configure(conf, engine, DBNAME, "m1", "target/db134/index", "target/db134/data", metadata, bgTaskPool);
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
							TimeSeries ts = m.getOrCreateTimeSeries("vf1", tags, 4096, false, conf);
							long timestamp = t + j * 1000;
							ts.addDataPoint(TimeUnit.MILLISECONDS, timestamp, j);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
			es.shutdown();
			wait.set(true);
			es.awaitTermination(100, TimeUnit.SECONDS);
			TimeSeries ts = m.getOrCreateTimeSeries("vf1", tags, 4096, false, conf);
			List<DataPoint> dps = ts.queryDataPoints("vf1", t1 - 120, t1 + 1000_000, null);
			assertEquals(200, dps.size());
			assertEquals(1, ts.getBucketCount());
			m.close();
		}
	}

	@Test
	public void testLinearizabilityWithRollOverBucket() throws IOException, InterruptedException {
		for (int p = 0; p < 2; p++) {
			final int LIMIT = 10000;
			MiscUtils.delete(new File("target/db135/"));
			final long t1 = 1497720452566L;
			final List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build(),
					Tag.newBuilder().setTagKey("t").setTagValue("2").build());
			Measurement m = new PersistentMeasurement();
			m.configure(conf, engine, DBNAME, "m1", "target/db135/index", "target/db135/data", metadata, bgTaskPool);
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
							TimeSeries ts = m.getOrCreateTimeSeries("vf1", tags, 4096, false, conf);
							long timestamp = t + j * 1000;
							ts.addDataPoint(TimeUnit.MILLISECONDS, timestamp, j);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
			es.shutdown();
			wait.set(true);
			es.awaitTermination(10, TimeUnit.SECONDS);
			TimeSeries ts = m.getOrCreateTimeSeries("vf1", tags, 4096, false, conf);
			List<DataPoint> dps = ts.queryDataPoints("vf1", t1 - 100, t1 + 1000_0000, null);
			assertEquals(LIMIT * 2, dps.size(), 10);
			m.close();
		}
	}

	@Test
	public void testTagEncodeDecode() throws IOException {
		String indexDir = "target/test";
		MetricsRegistryService.getInstance(engine, bgTaskPool).getInstance("requests");
		final List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("host").setTagValue("2").build(),
				Tag.newBuilder().setTagKey("value").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("1").build());
		MiscUtils.delete(new File(indexDir));
		new File(indexDir).mkdirs();
		SetIndex table = new SetIndex(indexDir, "test2");
		Measurement measurement = new PersistentMeasurement();
		ByteString encodedStr = measurement.encodeTagsToString(table, tags);
		List<Tag> decodedStr = Measurement.decodeStringToTags(table, encodedStr.toString());
		List<Tag> list = tags;
		for (int i = 0; i < list.size(); i++) {
			assertEquals(list.get(i) + "", list.get(i), decodedStr.get(i));
		}
	}

}
