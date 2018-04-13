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
package com.srotya.sidewinder.core.storage.disk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class TestPersistentMeasurement {

	private static final String TARGET_MEASUREMENT_COMMON = "target/measurement-persistent";
	private Map<String, String> conf = new HashMap<>();
	private DBMetadata metadata = new DBMetadata(28);
	private String dataDir;
	private String indexDir;
	private PersistentMeasurement measurement;
	private static ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);
	private static StorageEngine engine = new MemStorageEngine();
	private static String DBNAME = "test";

	@BeforeClass
	public static void beforeClass() throws IOException {
		engine.configure(new HashMap<>(), bgTaskPool);
	}

	@Before
	public void before() throws InstantiationException, IllegalAccessException, IOException {
		dataDir = TARGET_MEASUREMENT_COMMON + "/data";
		indexDir = TARGET_MEASUREMENT_COMMON + "/index";
		measurement = new PersistentMeasurement();
	}

	@After
	public void after() throws IOException {
		MiscUtils.delete(new File(TARGET_MEASUREMENT_COMMON));
	}

	@Test
	public void testDataPointsRecoveryPTR() throws Exception {
		long ts = System.currentTimeMillis();
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		Map<String, String> conf = new HashMap<>();
		conf.put("disk.compression.class", ByzantineWriter.class.getName());
		conf.put("malloc.file.max", String.valueOf(2 * 1024 * 1024));
		conf.put("malloc.ptrfile.increment", String.valueOf(2 * 1024));
		measurement.configure(conf, null, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		int LIMIT = 100;
		for (int i = 0; i < LIMIT; i++) {
			measurement.addDataPoint("value" + i, tags, ts, 1L, false);
		}
		measurement.close();

		measurement.configure(conf, null, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		List<Series> resultMap = new ArrayList<>();
		measurement.queryDataPoints("value.*", ts, ts + 1000, null, null, resultMap);
		assertEquals(LIMIT, resultMap.size());
		measurement.close();
	}

	@Test
	public void testDataPointsRecovery() throws Exception {
		long ts = System.currentTimeMillis();
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test").setTagValue("2").build());
		conf.put("disk.compression.class", ByzantineWriter.class.getName());
		conf.put("malloc.file.max", String.valueOf(1024 * 1024));
		try {
			measurement.configure(conf, null, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
			fail("Must throw invalid file max size exception");
		} catch (Exception e) {
		}
		conf.put("malloc.file.max", String.valueOf(2 * 1024 * 1024));
		measurement.configure(conf, null, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		int LIMIT = 100000;
		for (int i = 0; i < LIMIT; i++) {
			measurement.addDataPoint("value", tags, ts + i * 1000, 1L, false);
		}
		measurement.close();

		measurement.configure(conf, null, 4096, DBNAME, "m1", indexDir, dataDir, metadata, bgTaskPool);
		List<Series> resultMap = new ArrayList<>();
		measurement.queryDataPoints("value", ts, ts + 1000 * LIMIT, null, null, resultMap);
		Iterator<Series> iterator = resultMap.iterator();
		assertEquals(LIMIT, iterator.next().getDataPoints().size());

		resultMap.clear();
		TagFilter filter = MiscUtils.buildTagFilter("test=1&test=2");
		measurement.queryDataPoints("value", ts, ts + 1000 * LIMIT, filter, null, resultMap);
		iterator = resultMap.iterator();
		assertEquals(LIMIT, iterator.next().getDataPoints().size());
		
		measurement.close();
	}

	@Test
	public void testCompaction() throws IOException {
		final long ts = 1484788896586L;
		List<Tag> tags = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("1").build(),
				Tag.newBuilder().setTagKey("test2").setTagValue("2").build());
		conf.put("disk.compression.class", ByzantineWriter.class.getName());
		conf.put("malloc.file.max", String.valueOf(2 * 1024 * 1024));
		conf.put("buffer.size", String.valueOf(32768));
		conf.put("malloc.ptrfile.increment", String.valueOf(1024));
		conf.put("compaction.ratio", "1.2");
		conf.put("compaction.enabled", "true");
		measurement.configure(conf, null, 1024, DBNAME, "m2", indexDir, dataDir, metadata, bgTaskPool);
		int LIMIT = 7000;
		for (int i = 0; i < LIMIT; i++) {
			measurement.addDataPoint("value1", tags, ts + i, 1.2 * i, true, false);
		}
		assertEquals(1, measurement.getTimeSeries().size());
		TimeSeries series = measurement.getTimeSeries().iterator().next();
		System.out.println("Series:" + series);
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
		measurement.compact();
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
		measurement.close();
		// test buffer recovery after compaction, validate count
		measurement.configure(conf, null, 4096, DBNAME, "m2", indexDir, dataDir, metadata, bgTaskPool);
		series = measurement.getTimeSeries().iterator().next();
		queryDataPoints = series.queryDataPoints("", ts, ts + LIMIT + 1, null);
		assertEquals(LIMIT, queryDataPoints.size());
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = queryDataPoints.get(i);
			assertEquals(ts + i, dp.getTimestamp());
			assertEquals(i * 1.2, dp.getValue(), 0.01);
		}
		for (int i = 0; i < LIMIT; i++) {
			measurement.addDataPoint("value1", tags, LIMIT + ts + i, 1.2, false, false);
		}
		series.getBucketRawMap().entrySet().iterator().next().getValue().stream()
				.map(v -> "" + v.getCount() + ":" + v.isReadOnly() + ":" + (int) v.getRawBytes().get(1))
				.forEach(System.out::println);
		measurement.close();
		// test recovery again
		measurement.configure(conf, null, 4096, DBNAME, "m2", indexDir, dataDir, metadata, bgTaskPool);
		series = measurement.getTimeSeries().iterator().next();
		queryDataPoints = series.queryDataPoints("", ts - 1, ts + 2 + (LIMIT * 2), null);
		assertEquals(LIMIT * 2, queryDataPoints.size());
		for (int i = 0; i < LIMIT * 2; i++) {
			DataPoint dp = queryDataPoints.get(i);
			assertEquals("Error:" + i + " " + (dp.getTimestamp() - ts - i), ts + i, dp.getTimestamp());
		}
	}
}
