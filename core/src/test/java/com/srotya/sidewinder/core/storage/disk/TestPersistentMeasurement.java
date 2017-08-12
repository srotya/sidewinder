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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
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
//		TimeSeries series = measurement.getOrCreateTimeSeries("v1", Arrays.asList("test1"), 4096, false, conf);
	}

	@Test
	public void testConstructRowKey() throws Exception {
		StorageEngine engine = new DiskStorageEngine();
		MiscUtils.delete(new File("target/db131/"));
		HashMap<String, String> map = new HashMap<>();
		map.put("data.dir", "target/db131/data");
		map.put("index.dir", "target/db131/index");
		engine.configure(map, Executors.newScheduledThreadPool(1, new BackgrounThreadFactory("bg")));
		List<String> tags = Arrays.asList("test1", "test2");
		Measurement m = new PersistentMeasurement();
		m.configure(conf, "m1", "target/db131/index", "target/db131/data", metadata, bgTaskPool);
		TagIndex index = m.getTagIndex();
		String encodeTagsToString = m.encodeTagsToString(index, tags);
		String key = m.constructSeriesId("csd", tags, index);
		assertEquals("csd#" + encodeTagsToString, key);
	}

	@Test
	public void testTagEncodeDecode() throws IOException {
		DiskTagIndex table = new DiskTagIndex("target/test", "test2");
		Measurement measurement = new PersistentMeasurement();
		String encodedStr = measurement.encodeTagsToString(table, Arrays.asList("host", "value", "test"));
		List<String> decodedStr = measurement.decodeStringToTags(table, encodedStr);
		assertEquals(Arrays.asList("host", "value", "test"), decodedStr);
	}

}
