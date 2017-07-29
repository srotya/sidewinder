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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;

/**
 * @author ambud
 */
public class TestPersistentTimeSeries {

	@Test
	public void testConstructor() throws IOException {
		Map<String, String> conf = new HashMap<>();
		DBMetadata metadata = new DBMetadata(121);
		String measurementPath = "target/seriestest";
		String compressionFQCN = ByzantineWriter.class.getName();
		String seriesId = "series111";
		int timeBucketSize = 4096;
		PersistentTimeSeries timeseries = new PersistentTimeSeries(measurementPath, compressionFQCN, seriesId, metadata,
				timeBucketSize, false, conf, null);
		timeseries.addDataPoint(TimeUnit.MILLISECONDS, 1497720452566L, 1L);
		timeseries.addDataPoint(TimeUnit.MILLISECONDS, 1497720453566L, 1L);
		assertEquals(1, timeseries.getSeriesBuckets(TimeUnit.MILLISECONDS, 1497720452566L).size());
		timeseries = new PersistentTimeSeries(measurementPath, compressionFQCN, seriesId, metadata, timeBucketSize,
				false, conf, null);
	}

}
