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

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;

/**
 * @author ambud
 */
public class TestTimeSeries {

	@Test
	public void testTimeSeriesBucketRecoverDouble() throws IOException {
		Map<String, String> conf = new HashMap<>();
		MockMeasurement measurement = new MockMeasurement();
		DBMetadata metadata = new DBMetadata(28);
		ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);
		TimeSeries ts = new TimeSeries(measurement, ByzantineWriter.class.getName(), "test12312", 4096 * 10, metadata,
				false, conf, bgTaskPool);
		long t = 1497720442566L;
		for (int i = 0; i < 1000; i++) {
			ts.addDataPoint(TimeUnit.MILLISECONDS, t + i, i * 0.1);
		}

		assertEquals("test12312", ts.getSeriesId());
		assertEquals(4, ts.getBucketMap().values().size());
		assertTrue(!ts.isFp());

		ts = new TimeSeries(measurement, ByzantineWriter.class.getName(), "test12312", 4096 * 10, metadata, false, conf,
				bgTaskPool);
		assertEquals(4, measurement.getBufferRenewCounter());
		List<DataPoint> dps = ts.queryDataPoints("test", Arrays.asList("test32"), t, t + 1001, null);
		assertEquals(1000, dps.size());
		for (int j = 0; j < dps.size(); j++) {
			DataPoint dataPoint = dps.get(j);
			assertEquals(t + j, dataPoint.getTimestamp());
			assertEquals(j * 0.1, dataPoint.getValue(), 0);
		}
	}

	@Test
	public void testTimeSeriesBucketRecover() throws IOException {
		Map<String, String> conf = new HashMap<>();
		MockMeasurement measurement = new MockMeasurement();
		DBMetadata metadata = new DBMetadata(28);
		ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);
		TimeSeries ts = new TimeSeries(measurement, ByzantineWriter.class.getName(), "test12312", 4096 * 10, metadata,
				false, conf, bgTaskPool);
		long t = 1497720442566L;
		for (int i = 0; i < 1000; i++) {
			ts.addDataPoint(TimeUnit.MILLISECONDS, t + i, i);
		}

		assertEquals("test12312", ts.getSeriesId());
		assertEquals(4, ts.getBucketMap().values().size());
		assertTrue(!ts.isFp());

		ts = new TimeSeries(measurement, ByzantineWriter.class.getName(), "test12312", 4096 * 10, metadata, false, conf,
				bgTaskPool);
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
		MockMeasurement measurement = new MockMeasurement();
		DBMetadata metadata = new DBMetadata(28);
		ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);
		TimeSeries ts = new TimeSeries(measurement, ByzantineWriter.class.getName(), "test12312", 4096 * 10, metadata,
				false, conf, bgTaskPool);
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
		MockMeasurement measurement = new MockMeasurement();
		DBMetadata metadata = new DBMetadata(28);
		ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);
		TimeSeries ts = new TimeSeries(measurement, ByzantineWriter.class.getName(), "test12312", 4096 * 10, metadata,
				false, conf, bgTaskPool);
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

	private class MockMeasurement implements Measurement {

		private int bufferRenewCounter = 0;
		private List<ByteBuffer> list;

		public MockMeasurement() {
			list = new ArrayList<>();
		}

		@Override
		public void configure(Map<String, String> conf, String compressionClass, String baseIndexDirectory,
				String dataDirectory, String dbName, String measurementName, DBMetadata metadata,
				ScheduledExecutorService bgTaskPool) throws IOException {
		}

		@Override
		public Collection<TimeSeries> getTimeSeries() {
			return null;
		}

		@Override
		public Map<String, TimeSeries> getTimeSeriesMap() {
			return null;
		}

		@Override
		public TagIndex getTagIndex() {
			return null;
		}

		@Override
		public TimeSeries get(String entry) {
			return null;
		}

		@Override
		public void loadTimeseriesFromMeasurements() throws IOException {
		}

		@Override
		public TimeSeries getOrCreateTimeSeries(String rowKey, int timeBucketSize, boolean fp, Map<String, String> conf)
				throws IOException {
			return null;
		}

		@Override
		public void delete() throws IOException {
		}

		@Override
		public void garbageCollector() throws IOException {
		}

		@Override
		public ByteBuffer createNewBuffer() throws IOException {
			bufferRenewCounter++;
			ByteBuffer allocate = ByteBuffer.allocate(1024);
			list.add(allocate);
			return allocate;
		}

		public int getBufferRenewCounter() {
			return bufferRenewCounter;
		}

		@Override
		public List<ByteBuffer> getBufTracker() {
			return list;
		}

	}

}
