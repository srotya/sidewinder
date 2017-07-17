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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.TimeSeriesBucket;
import com.srotya.sidewinder.core.storage.compression.gorilla.GorillaWriter;

/**
 * Unit tests for {@link TimeSeries}
 * 
 * @author ambud
 */
public class TestTimeSeries {
	
	private String className = GorillaWriter.class.getName();

	@Test
	public void testTimeSeriesConstruct() {
		TimeSeries series = new TimeSeries(className, "2214abfa", new DBMetadata(24), 4096, true, new HashMap<>());
		assertEquals("2214abfa", series.getSeriesId());
		assertEquals(4096, series.getTimeBucketSize());
		assertEquals((24 * 3600) / 4096, series.getRetentionBuckets());
	}

	@Test
	public void testAddAndReadDataPoints() throws IOException {
		TimeSeries series = new TimeSeries(className, "43232", new DBMetadata(24), 4096, true, new HashMap<>());
		long curr = System.currentTimeMillis();
		for (int i = 1; i <= 3; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, curr + i, 2.2 * i);
		}
		assertEquals(1, series.getBucketMap().size());
		TimeSeriesBucket bucket = series.getBucketMap().values().iterator().next();
		assertEquals(3, bucket.getCount());

		Reader reader = bucket.getReader(null, null, true, "value", Arrays.asList("test"));
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
		TimeSeries series = new TimeSeries(className, "43232", new DBMetadata(24), 4096, true, new HashMap<>());
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

		series = new TimeSeries(className, "43232", new DBMetadata(28), 4096, true, new HashMap<>());
		curr = 1484788896586L;
		for (int i = 0; i <= 24; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, curr + (4096_000 * i), 2.2 * i);
		}
		readers = series.queryReader("test", Arrays.asList("test"), curr, curr + (4096_000) * 28, null);
		// should return 25 partitions
		assertEquals(25, readers.size());
		series.collectGarbage();
		readers = series.queryReader("test", Arrays.asList("test"), curr, curr + (4096_000) * 28, null);
		assertEquals(24, readers.size());
	}

}
