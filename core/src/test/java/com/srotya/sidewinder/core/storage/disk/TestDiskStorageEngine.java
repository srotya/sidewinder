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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * Unit tests for {@link DiskStorageEngine}
 * 
 * @author ambud
 */
public class TestDiskStorageEngine {

	public static ScheduledExecutorService bgTasks;

	@BeforeClass
	public static void before() {
		bgTasks = Executors.newScheduledThreadPool(1, new BackgrounThreadFactory("te1"));
	}

	@AfterClass
	public static void after() {
		bgTasks.shutdown();
	}
	
	@Test
	public void testMultipleDrives() throws ItemNotFoundException, IOException {
		StorageEngine engine = new DiskStorageEngine();
		HashMap<String, String> map = new HashMap<>();
		MiscUtils.delete(new File("targer/db10221/"));
		map.put("index.dir", "target/db10221/index");
		map.put("data.dir", "target/db10221/data1, target/db10221/data2");
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("e").build());
		try {
			engine.configure(map, bgTasks);
		} catch (IOException e) {
			e.printStackTrace();
			fail("No IOException should be thrown");
		}
		long ts = System.currentTimeMillis();
		try {
			for (int i = 0; i < 10; i++) {
				engine.writeDataPointLocked(MiscUtils.buildDataPoint("test" + i, "ss", "value", tagd, ts, 2.2), false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
		assertEquals(5, new File("target/db10221/data1").listFiles().length);
		assertEquals(5, new File("target/db10221/data2").listFiles().length);
		engine.shutdown();
	}
	
	@Test
	public void testMultipleDrives2() throws ItemNotFoundException, IOException {
		StorageEngine engine = new DiskStorageEngine();
		HashMap<String, String> map = new HashMap<>();
		MiscUtils.delete(new File("targer/db10221/"));
		map.put("index.dir", "target/db10221/index");
		map.put("data.dir", "target/db10221/data1,target/db10221/data2");
		List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("e").build());
		try {
			engine.configure(map, bgTasks);
		} catch (IOException e) {
			e.printStackTrace();
			fail("No IOException should be thrown");
		}
		long ts = System.currentTimeMillis();
		try {
			for (int i = 0; i < 10; i++) {
				engine.writeDataPointLocked(MiscUtils.buildDataPoint("test" + i, "ss", "value", tagd, ts, 2.2), false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
		assertEquals(5, new File("target/db10221/data1").listFiles().length);
		assertEquals(5, new File("target/db10221/data2").listFiles().length);
		engine.shutdown();
	}
	
	@Test
	public void testQueryDataPointsRecovery() throws Exception {
		try {
			List<Tag> tagd = Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("e").build());
			DiskStorageEngine engine = new DiskStorageEngine();
			File file = new File("target/db201/");
			MiscUtils.delete(file);
			MiscUtils.ls(file);
			Map<String, String> map = new HashMap<>();
			map.put("index.dir", "target/db201/index");
			map.put("data.dir", "target/db201/data");
			engine.configure(map, bgTasks);
			long ts = System.currentTimeMillis();
			Map<String, Measurement> db = engine.getOrCreateDatabase("test3", 24);
			assertEquals(0, db.size());
			engine.writeDataPointLocked(MiscUtils.buildDataPoint("test3", "cpu", "value", tagd, ts, 1), false);
			engine.writeDataPointLocked(MiscUtils.buildDataPoint("test3", "cpu", "value", tagd, ts + (400 * 60000), 4),
					false);
			Measurement measurement = engine.getOrCreateMeasurement("test3", "cpu");
			assertEquals(1, measurement.getSeriesKeys().size());
			MiscUtils.ls(file);
			engine = new DiskStorageEngine();
			engine.configure(map, bgTasks);

			assertTrue(!engine.isMeasurementFieldFP("test3", "cpu", "value"));
			List<Series> queryDataPoints = engine.queryDataPoints("test3", "cpu", "value", ts, ts + (400 * 60000), null,
					null);
			try {
				engine.isMeasurementFieldFP("test3", "test", "test");
				fail("Measurement should not exist");
			} catch (Exception e) {
			}
			assertEquals(1, queryDataPoints.size());
			assertEquals(2, queryDataPoints.iterator().next().getDataPoints().size());
			assertEquals(ts, queryDataPoints.iterator().next().getDataPoints().get(0).getTimestamp());
			assertEquals(ts + (400 * 60000), queryDataPoints.iterator().next().getDataPoints().get(1).getTimestamp());
			
			TagFilter filter = MiscUtils.buildTagFilter("test=e");
			queryDataPoints = engine.queryDataPoints("test3", "cpu", "value", ts, ts + (400 * 60000), filter,
					null);
			assertEquals(1, queryDataPoints.size());
			assertEquals(2, queryDataPoints.iterator().next().getDataPoints().size());
			assertEquals(ts, queryDataPoints.iterator().next().getDataPoints().get(0).getTimestamp());
			assertEquals(ts + (400 * 60000), queryDataPoints.iterator().next().getDataPoints().get(1).getTimestamp());
			
			filter = MiscUtils.buildTagFilter("test=2");
			queryDataPoints = engine.queryDataPoints("test3", "cpu", "value", ts, ts + (400 * 60000), filter,
					null);
			assertEquals(0, queryDataPoints.size());
			
			try {
				engine.dropDatabase("test3");
			} catch (Exception e) {
				e.printStackTrace();
				fail("Database delete must succeed");
			}
			assertTrue(!new File("target/db201/data/test3").exists());
			assertEquals(0, engine.getOrCreateMeasurement("test3", "cpu").getSeriesKeys().size());
			engine.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

}
