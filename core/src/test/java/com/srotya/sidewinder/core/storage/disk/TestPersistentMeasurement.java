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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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

import org.junit.Test;

import com.srotya.sidewinder.core.filters.AnyFilter;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.SeriesQueryOutput;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class TestPersistentMeasurement {

	private Map<String, String> conf = new HashMap<>();
	private DBMetadata metadata = new DBMetadata(28);
	private ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);

	@Test
	public void test() throws IOException {
		Measurement measurement = new PersistentMeasurement();
		measurement.configure(conf, "m1", "target/pmeasurement1.idx", "target/pmeasurement1", metadata, bgTaskPool);
		// TimeSeries series = measurement.getOrCreateTimeSeries("v1",
		// Arrays.asList("test1"), 4096, false, conf);
	}

	@Test
	public void testDataPointsRecovery() throws Exception {
		long ts = System.currentTimeMillis();
		MiscUtils.delete(new File("target/db132/"));
		List<String> tags = Arrays.asList("test1", "test2");
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> map = new HashMap<>();
		map.put("disk.compression.class", ByzantineWriter.class.getName());
		map.put("measurement.file.max", String.valueOf(1024 * 1024));
		try {
			m.configure(map, "m1", "target/db132/index", "target/db132/data", metadata, bgTaskPool);
			fail("Must throw invalid file max size exception");
		} catch (Exception e) {
		}
		map.put("measurement.file.max", String.valueOf(2 * 1024 * 1024));
		m.configure(map, "m1", "target/db132/index", "target/db132/data", metadata, bgTaskPool);
		int LIMIT = 1000000;
		for (int i = 0; i < LIMIT; i++) {
			TimeSeries t = m.getOrCreateTimeSeries("value", tags, 4096, false, map);
			t.addDataPoint(TimeUnit.MILLISECONDS, ts + i * 1000, 1L);
		}
		m.close();

		m = new PersistentMeasurement();
		m.configure(map, "m1", "target/db132/index", "target/db132/data", metadata, bgTaskPool);
		m.loadTimeseriesFromMeasurements();
		Set<SeriesQueryOutput> resultMap = new HashSet<>();
		m.queryDataPoints("value", ts, ts + 1000 * LIMIT, tags, new AnyFilter<>(), null, null, resultMap);
		Iterator<SeriesQueryOutput> iterator = resultMap.iterator();
		assertEquals(LIMIT, iterator.next().getDataPoints().size());
	}

	@Test
	public void testConstructRowKey() throws Exception {
		MiscUtils.delete(new File("target/db131/"));
		List<String> tags = Arrays.asList("test1", "test2");
		Measurement m = new PersistentMeasurement();
		m.configure(conf, "m1", "target/db131/index", "target/db131/data", metadata, bgTaskPool);
		TagIndex index = m.getTagIndex();
		String encodeTagsToString = m.encodeTagsToString(index, tags);
		String key = m.constructSeriesId("csd", tags, index);
		assertEquals("csd#" + encodeTagsToString, key);
	}

	@Test
	public void testMeasurementRecovery() throws IOException {
		MiscUtils.delete(new File("target/db141/"));
		Measurement m = new PersistentMeasurement();
		m.configure(conf, "m1", "target/db141/index", "target/db141/data", metadata, bgTaskPool);
		TimeSeries ts = m.getOrCreateTimeSeries("vf1", Arrays.asList("t1", "t2"), 4096, false, conf);
		long t = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			ts.addDataPoint(TimeUnit.MILLISECONDS, t + i * 1000, i);
		}
		List<DataPoint> dps = ts.queryDataPoints("vf1", Arrays.asList("t1", "t2"), t, t + 1000 * 100, null);
		assertEquals(100, dps.size());
		for (int i = 0; i < 100; i++) {
			DataPoint dp = dps.get(i);
			assertEquals(t + i * 1000, dp.getTimestamp());
			assertEquals(i, dp.getLongValue());
		}
		Set<SeriesQueryOutput> resultMap = new HashSet<>();
		m.queryDataPoints("vf1", t, t + 1000 * 100, Arrays.asList("t1", "t2"), new AnyFilter<>(), null, null,
				resultMap);
		assertEquals(1, resultMap.size());
		SeriesQueryOutput next = resultMap.iterator().next();
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
	}

	@Test
	public void testLinearizability() throws IOException, InterruptedException {
		for (int p = 0; p < 100; p++) {
			MiscUtils.delete(new File("target/db134/"));
			final long t1 = 1497720452566L;
			Measurement m = new PersistentMeasurement();
			m.configure(conf, "m1", "target/db134/index", "target/db134/data", metadata, bgTaskPool);
			ExecutorService es = Executors.newFixedThreadPool(2, new BackgrounThreadFactory("tlinear"));
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
							TimeSeries ts = m.getOrCreateTimeSeries("vf1", Arrays.asList("t1", "t2"), 4096, false,
									conf);
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
			TimeSeries ts = m.getOrCreateTimeSeries("vf1", Arrays.asList("t1", "t2"), 4096, false, conf);
			List<DataPoint> dps = ts.queryDataPoints("vf1", Arrays.asList("t1"), t1 - 120, t1 + 1000_000, null);
			assertEquals(200, dps.size());
			assertEquals(1, ts.getBucketCount());
		}
	}

	@Test
	public void testLinearizabilityWithRollOverBucket() throws IOException, InterruptedException {
		for (int p = 0; p < 100; p++) {
			final int LIMIT = 10000;
			MiscUtils.delete(new File("target/db135/"));
			final long t1 = 1497720452566L;
			Measurement m = new PersistentMeasurement();
			m.configure(conf, "m1", "target/db135/index", "target/db135/data", metadata, bgTaskPool);
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
							TimeSeries ts = m.getOrCreateTimeSeries("vf1", Arrays.asList("t1", "t2"), 4096, false,
									conf);
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
			TimeSeries ts = m.getOrCreateTimeSeries("vf1", Arrays.asList("t1", "t2"), 4096, false, conf);
			List<DataPoint> dps = ts.queryDataPoints("vf1", Arrays.asList("t1"), t1 - 100, t1 + 1000_0000, null);
			assertEquals(LIMIT * 2, dps.size(), 10);
		}
	}

	@Test
	public void testTagEncodeDecode() throws IOException {
		String indexDir = "target/test";
		MiscUtils.delete(new File(indexDir));
		new File(indexDir).mkdirs();
		DiskTagIndex table = new DiskTagIndex(indexDir, "test2");
		Measurement measurement = new PersistentMeasurement();
		String encodedStr = measurement.encodeTagsToString(table, Arrays.asList("host", "value", "test"));
		List<String> decodedStr = Measurement.decodeStringToTags(table, encodedStr);
		assertEquals(Arrays.asList("host", "value", "test"), decodedStr);
	}

}
