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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;

/**
 * @author ambud
 */
public class TestTimeSeries {

	private static final String compression = "byzantine";
	private static final String compaction = "byzantine";
	private Map<String, String> conf = new HashMap<>();

	@Test
	public void testTimeSeries() throws IOException {
		Measurement measurement = new MockMeasurement(100);
		DBMetadata metadata = new DBMetadata(24);
		TimeSeries series = new TimeSeries(measurement, compression, compaction, "2214abfa", 4096, metadata, true,
				conf);
		assertEquals("2214abfa", series.getSeriesId());
		assertEquals(4096, series.getTimeBucketSize());
		assertEquals((24 * 3600) / 4096, series.getRetentionBuckets());
	}

	@Test
	public void testThreadSafety() throws Exception {
		Measurement measurement = new MockMeasurement(1024);
		DBMetadata metadata = new DBMetadata(24);
		final TimeSeries series = new TimeSeries(measurement, compression, compaction, "2214abfa", 4096, metadata, true,
				conf);
		final int THREAD_COUNT = 1;
		final int POINT_COUNT = 4_000_000;
		ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
		long ts = System.currentTimeMillis();
		for (int i = 0; i < THREAD_COUNT; i++) {
			final int o = i;
			es.submit(() -> {
				long t = ts + o;
				try {
					for (int j = 0; j < POINT_COUNT; j++) {
						series.addDataPoint(TimeUnit.MILLISECONDS, t, j);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}
		es.shutdown();
		es.awaitTermination(100, TimeUnit.SECONDS);
		List<DataPoint> dps = series.queryDataPoints("", Arrays.asList(""), 0, Long.MAX_VALUE, null);
		assertEquals(THREAD_COUNT * POINT_COUNT, dps.size());
	}

	@Test
	public void testAddAndReadDataPoints() throws IOException {
		Measurement measurement = new MockMeasurement(100);
		DBMetadata metadata = new DBMetadata(24);
		TimeSeries series = new TimeSeries(measurement, compression, compaction, "43232", 4096, metadata, true, conf);
		long curr = System.currentTimeMillis();
		for (int i = 1; i <= 3; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, curr + i, 2.2 * i);
		}
		assertEquals(1, series.getBucketMap().size());
		Writer writer = series.getBucketMap().values().iterator().next();
		assertEquals(3, writer.getCount());

		Reader reader = TimeSeries.getReader(writer, null, null, true, "value", Arrays.asList("test"));
		for (int i = 0; i < 3; i++) {
			reader.readPair();
		}
		try {
			reader.readPair();
			fail("The read shouldn't succeed");
		} catch (IOException e) {
		}

		List<DataPoint> values = series.queryDataPoints("value", Arrays.asList("test"), curr, curr + 3, null);
		assertEquals(3, values.size());
		for (int i = 1; i <= 3; i++) {
			DataPoint dp = values.get(i - 1);
			assertEquals("Value mismatch:" + dp.getValue() + "\t" + (2.2 * i) + "\t" + i, dp.getValue(), 2.2 * i, 0.01);
			assertEquals("value", dp.getValueFieldName());
			assertEquals(Arrays.asList("test"), dp.getTags());
		}

		List<Reader> queryReaders = series.queryReader("value", Arrays.asList("test"), curr, curr + 3, null);
		assertEquals(1, queryReaders.size());
		reader = queryReaders.get(0);
		for (int i = 1; i <= 3; i++) {
			DataPoint dp = reader.readPair();
			assertEquals("Value mismatch:" + dp.getValue() + "\t" + (2.2 * i) + "\t" + i, dp.getValue(), 2.2 * i, 0.01);
			assertEquals("value", dp.getValueFieldName());
			assertEquals(Arrays.asList("test"), dp.getTags());
		}

		values = series.queryDataPoints("value", Arrays.asList("test"), curr - 1, curr - 1, null);
		assertEquals(0, values.size());
	}

	@Test
	public void testGarbageCollector() throws IOException {
		Measurement measurement = new MockMeasurement(100);
		DBMetadata metadata = new DBMetadata(24);
		TimeSeries series = new TimeSeries(measurement, compression, compaction, "43232", 4096, metadata, true, conf);
		long curr = 1484788896586L;
		for (int i = 0; i <= 24; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, curr + (4096_000 * i), 2.2 * i);
		}
		List<Reader> readers = series.queryReader("test", Arrays.asList("test"), curr, curr + (4096_000) * 23, null);
		// should return 3 partitions
		assertEquals(24, readers.size());
		series.collectGarbage();
		readers = series.queryReader("test", Arrays.asList("test"), curr, curr + (4096_000) * 26, null);
		assertEquals(21, readers.size());

		series = new TimeSeries(measurement, compression, compaction, "43232", 4096, metadata, true, conf);
		curr = 1484788896586L;
		for (int i = 0; i <= 24; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, curr + (4096_000 * i), 2.2 * i);
		}
		readers = series.queryReader("test", Arrays.asList("test"), curr, curr + (4096_000) * 28, null);
		// should return 25 partitions
		assertEquals(25, readers.size());
		List<Writer> collectGarbage = series.collectGarbage();
		assertEquals(4, collectGarbage.size());
		readers = series.queryReader("test", Arrays.asList("test"), curr, curr + (4096_000) * 28, null);
		assertEquals(21, readers.size());
	}

	@Test
	public void testTimeSeriesBucketRecoverDouble() throws IOException {
		Map<String, String> conf = new HashMap<>();
		MockMeasurement measurement = new MockMeasurement(100);
		DBMetadata metadata = new DBMetadata(28);

		TimeSeries ts = new TimeSeries(measurement, compression, compaction, "test12312", 4096 * 10, metadata, false,
				conf);
		long t = 1497720442566L;
		for (int i = 0; i < 1000; i++) {
			ts.addDataPoint(TimeUnit.MILLISECONDS, t + i, i * 0.1);
		}

		assertEquals("test12312", ts.getSeriesId());
		int size = ts.getBucketMap().values().size();
		assertTrue(!ts.isFp());

		ts = new TimeSeries(measurement, compression, compaction, "test12312", 4096 * 10, metadata, false, conf);
		ts.loadBucketMap(measurement.getBufTracker());
		assertEquals(size, measurement.getBufferRenewCounter());
		List<DataPoint> dps = ts.queryDataPoints("test", Arrays.asList("test32"), t, t + 1001, null);
		assertEquals(1000, dps.size());
		for (int j = 0; j < dps.size(); j++) {
			DataPoint dataPoint = dps.get(j);
			assertEquals(t + j, dataPoint.getTimestamp());
			assertEquals(j * 0.1, dataPoint.getValue(), 0);
		}
		assertEquals(2, ts.getRetentionBuckets());
	}

	@Test
	public void testTimeSeriesBucketRecover() throws IOException {
		Map<String, String> conf = new HashMap<>();
		MockMeasurement measurement = new MockMeasurement(1024);
		DBMetadata metadata = new DBMetadata(28);

		TimeSeries ts = new TimeSeries(measurement, compression, compaction, "test12312", 4096 * 10, metadata, false,
				conf);
		long t = 1497720442566L;
		for (int i = 0; i < 1000; i++) {
			ts.addDataPoint(TimeUnit.MILLISECONDS, t + i, i);
		}

		assertEquals("test12312", ts.getSeriesId());
		assertEquals(4, ts.getBucketMap().values().size());
		assertTrue(!ts.isFp());

		ts = new TimeSeries(measurement, compression, compaction, "test12312", 4096 * 10, metadata, false, conf);
		ts.loadBucketMap(measurement.getBufTracker());
		assertEquals(4, measurement.getBufferRenewCounter());
		List<DataPoint> dps = ts.queryDataPoints("test", Arrays.asList("test32"), t, t + 1001, null);
		assertEquals(1000, dps.size());
		for (int j = 0; j < dps.size(); j++) {
			DataPoint dataPoint = dps.get(j);
			assertEquals(t + j, dataPoint.getTimestamp());
			assertEquals(j, dataPoint.getLongValue());
		}
	}

	@Test
	public void testReadWriteSingle() throws IOException {
		Map<String, String> conf = new HashMap<>();
		MockMeasurement measurement = new MockMeasurement(100);
		DBMetadata metadata = new DBMetadata(28);

		TimeSeries ts = new TimeSeries(measurement, compression, compaction, "test12312", 4096 * 10, metadata, false,
				conf);
		long t = 1497720442566L;
		for (int i = 0; i < 10; i++) {
			ts.addDataPoint(TimeUnit.MILLISECONDS, t + i, i);
		}
		assertEquals(1, measurement.getBufferRenewCounter());
		List<DataPoint> dps = ts.queryDataPoints("test", Arrays.asList("test32"), t, t + 1001, null);
		assertEquals(10, dps.size());
		for (int j = 0; j < dps.size(); j++) {
			DataPoint dataPoint = dps.get(j);
			assertEquals(t + j, dataPoint.getTimestamp());
			assertEquals(j, dataPoint.getLongValue());
		}
	}

	@Test
	public void testReadWriteExpand() throws IOException {
		Map<String, String> conf = new HashMap<>();
		MockMeasurement measurement = new MockMeasurement(1024);
		DBMetadata metadata = new DBMetadata(28);

		TimeSeries ts = new TimeSeries(measurement, compression, compaction, "test12312", 4096 * 10, metadata, false,
				conf);
		long t = 1497720442566L;
		for (int i = 0; i < 1000; i++) {
			ts.addDataPoint(TimeUnit.MILLISECONDS, t + i, i);
		}
		assertEquals(4, measurement.getBufferRenewCounter());
		List<DataPoint> dps = ts.queryDataPoints("test", Arrays.asList("test32"), t, t + 1001, null);
		assertEquals(1000, dps.size());
		for (int j = 0; j < dps.size(); j++) {
			DataPoint dataPoint = dps.get(j);
			assertEquals(t + j, dataPoint.getTimestamp());
			assertEquals(j, dataPoint.getLongValue());
		}
	}

	@Test
	public void testCompaction() throws IOException {
		DBMetadata metadata = new DBMetadata(28);
		MockMeasurement measurement = new MockMeasurement(1024);
		HashMap<String, String> conf = new HashMap<>();
		conf.put("default.bucket.size", "409600");
		conf.put("compaction.enabled", "true");
		conf.put("use.query.pool", "false");
		conf.put("compaction.ratio", "1.1");

		final TimeSeries series = new TimeSeries(measurement, "byzantine", "byzantine", "asdasasd", 409600, metadata,
				true, conf);
		final long curr = 1497720652566L;

		String valueFieldName = "value";
		String tag = "host123123";
		List<String> tags = Arrays.asList(tag);
		for (int i = 1; i <= 10000; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, curr + i * 1000, i * 1.1);
		}

		long ts = System.nanoTime();
		List<DataPoint> dataPoints = series.queryDataPoints(valueFieldName, tags, curr - 1000, curr + 10000 * 1000 + 1,
				null);
		ts = System.nanoTime() - ts;
		System.out.println("Before compaction:" + ts / 1000 + "us");
		assertEquals(10000, dataPoints.size());
		for (int i = 1; i <= 10000; i++) {
			DataPoint dp = dataPoints.get(i - 1);
			assertEquals("Bad ts:" + i, curr + i * 1000, dp.getTimestamp());
			assertEquals(dp.getValue(), i * 1.1, 0.001);
		}
		SortedMap<String, List<Writer>> bucketRawMap = series.getBucketRawMap();
		assertEquals(1, bucketRawMap.size());
		int size = bucketRawMap.values().iterator().next().size();
		assertTrue(series.getCompactionSet().size() < size);
		assertTrue(size > 2);
		series.compact();
		ts = System.nanoTime();
		dataPoints = series.queryDataPoints(valueFieldName, tags, curr - 1000, curr + 10000 * 1000 + 1, null);
		ts = System.nanoTime() - ts;
		System.out.println("After compaction:" + ts / 1000 + "us");
		bucketRawMap = series.getBucketRawMap();
		assertEquals(2, bucketRawMap.values().iterator().next().size());
		int count = 0;
		for (List<Writer> list : bucketRawMap.values()) {
			for (Writer writer : list) {
				Reader reader = writer.getReader();
				count += reader.getPairCount();
			}
		}
		assertEquals(10000, count);
		assertEquals(10000, dataPoints.size());
		for (int i = 1; i <= 10000; i++) {
			DataPoint dp = dataPoints.get(i - 1);
			assertEquals("Bad ts:" + i, curr + i * 1000, dp.getTimestamp());
			assertEquals(dp.getValue(), i * 1.1, 0.001);
		}
	}

	@Test
	public void testCompactionGzip() throws IOException {
		DBMetadata metadata = new DBMetadata(28);
		MockMeasurement measurement = new MockMeasurement(1024);
		HashMap<String, String> conf = new HashMap<>();
		conf.put("default.bucket.size", "409600");
		conf.put("compaction.enabled", "true");
		conf.put("use.query.pool", "false");
		conf.put("compaction.ratio", "1.1");

		final TimeSeries series = new TimeSeries(measurement, "byzantine", "gzip", "asdasasd", 409600, metadata, true,
				conf);
		final long curr = 1497720652566L;

		String valueFieldName = "value";
		String tag = "host123123";
		List<String> tags = Arrays.asList(tag);
		for (int i = 1; i <= 10000; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, curr + i * 1000, i * 1.1);
		}

		SortedMap<String, List<Writer>> bucketRawMap = series.getBucketRawMap();
		assertEquals(1, bucketRawMap.size());
		int size = bucketRawMap.values().iterator().next().size();
		assertTrue(series.getCompactionSet().size() < size);
		assertTrue(size > 2);
		series.compact();
		List<DataPoint> dataPoints = series.queryDataPoints(valueFieldName, tags, curr - 1000, curr + 10000 * 1000 + 1,
				null);
		bucketRawMap = series.getBucketRawMap();
		assertEquals(2, bucketRawMap.values().iterator().next().size());
		int count = 0;
		for (List<Writer> list : bucketRawMap.values()) {
			for (Writer writer : list) {
				Reader reader = writer.getReader();
				count += reader.getPairCount();
			}
		}
		assertEquals(10000, count);
		assertEquals(10000, dataPoints.size());
		for (int i = 1; i <= 10000; i++) {
			DataPoint dp = dataPoints.get(i - 1);
			assertEquals("Bad ts:" + i, curr + i * 1000, dp.getTimestamp());
			assertEquals(dp.getValue(), i * 1.1, 0.001);
		}
	}

	@Test
	public void testCompactionThreadSafety() throws IOException, InterruptedException {
		DBMetadata metadata = new DBMetadata(28);
		MockMeasurement measurement = new MockMeasurement(1024);
		HashMap<String, String> conf = new HashMap<>();
		conf.put("default.bucket.size", "409600");
		conf.put("compaction.enabled", "true");
		conf.put("use.query.pool", "false");

		final TimeSeries series = new TimeSeries(measurement, "byzantine", "byzantine", "asdasasd", 409600, metadata,
				true, conf);
		final long curr = 1497720652566L;

		String valueFieldName = "value";
		String tag = "host123123";
		List<String> tags = Arrays.asList(tag);
		for (int i = 1; i <= 10000; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, curr + i * 1000, i * 1.1);
		}

		long ts = System.nanoTime();
		List<DataPoint> dataPoints = series.queryDataPoints(valueFieldName, tags, curr - 1000, curr + 10000 * 1000 + 1,
				null);
		ts = System.nanoTime() - ts;
		System.out.println("Before compaction:" + ts / 1000 + "us");
		assertEquals(10000, dataPoints.size());
		for (int i = 1; i <= 10000; i++) {
			DataPoint dp = dataPoints.get(i - 1);
			assertEquals("Bad ts:" + i, curr + i * 1000, dp.getTimestamp());
			assertEquals(dp.getValue(), i * 1.1, 0.001);
		}
		SortedMap<String, List<Writer>> bucketRawMap = series.getBucketRawMap();
		assertEquals(1, bucketRawMap.size());
		int size = bucketRawMap.values().iterator().next().size();
		assertTrue(series.getCompactionSet().size() < size);
		assertTrue(size > 2);
		List<Writer> compact = series.compact();
		System.out.println("Compacted series:" + compact.size() + "\toriginalsize:" + size + " newsize:"
				+ bucketRawMap.values().iterator().next().size());
		ts = System.nanoTime();
		dataPoints = series.queryDataPoints(valueFieldName, tags, curr - 1000, curr + 10000 * 1000 + 1, null);
		ts = System.nanoTime() - ts;
		System.out.println("After compaction:" + ts / 1000 + "us");
		final AtomicBoolean bool = new AtomicBoolean(false);
		Executors.newCachedThreadPool().execute(() -> {
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
				// TODO Auto-generated catch block
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
