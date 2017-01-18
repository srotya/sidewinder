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
package com.srotya.sidewinder.core.storage.gorilla;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * Unit tests for {@link TimeSeries}
 * 
 * @author ambud
 */
public class TestTimeSeries {

	@Test
	public void testTimeSeriesConstruct() {
		TimeSeries series = new TimeSeries("2214abfa", 24, 4096, true);
		assertEquals("2214abfa", series.getSeriesId());
		assertEquals(4096, series.getTimeBucketSize());
		assertEquals((24 * 3600) / 4096, series.getRetentionBuckets());
	}

	@Test
	public void testAddDataPoint() throws IOException {
		TimeSeries series = new TimeSeries("43232", 24, 4096, true);
		long curr = System.currentTimeMillis();
		for (int i = 1; i <= 3; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, curr, 2.2 * i);
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
			assertEquals("Value mismatch:" + values.get(i - 1).getValue() + "\t" + (2.2 * i) + "\t" + i,
					values.get(i - 1).getValue(), 2.2 * i, 0.01);
		}
	}

}
