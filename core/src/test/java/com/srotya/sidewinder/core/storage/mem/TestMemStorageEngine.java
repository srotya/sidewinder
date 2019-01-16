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
package com.srotya.sidewinder.core.storage.mem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * Unit tests for {@link MemStorageEngine}
 * 
 * @author ambud
 */
public class TestMemStorageEngine {

	public static ScheduledExecutorService bgTasks;

	@BeforeClass
	public static void before() {
		bgTasks = Executors.newScheduledThreadPool(1, new BackgrounThreadFactory("bgt"));
	}

	@AfterClass
	public static void after() {
		bgTasks.shutdown();
	}

	private Map<String, String> conf = new HashMap<>();

	@Test
	public void testConfigure() {
		StorageEngine engine = new MemStorageEngine();
		try {
			engine.writeDataPointWithLock(MiscUtils.buildDataPoint("test", "ss", Arrays.asList("value"),
					Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()),
					System.currentTimeMillis(), Arrays.asList(Double.doubleToLongBits(2.2)), Arrays.asList(true)), false);
		} catch (Exception e) {
		}

		try {
			engine.configure(new HashMap<>(), bgTasks);
		} catch (IOException e) {
			fail("No IOException should be thrown");
		}
		try {
			engine.writeDataPointWithLock(MiscUtils.buildDataPoint("test", "ss", Arrays.asList("value"),
					Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()),
					System.currentTimeMillis(), Arrays.asList(Double.doubleToLongBits(2.2)), Arrays.asList(true)), false);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Engine is initialized, no IO Exception should be thrown:" + e.getMessage());
		}
	}

	@Test
	public void testGetMeasurementsLike() throws Exception {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(conf, bgTasks);
		engine.writeDataPointWithLock(MiscUtils.buildDataPoint("test", "cpu", Arrays.asList("value"),
				Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()), System.currentTimeMillis(),
				Arrays.asList(2L), Arrays.asList(false)), false);
		engine.writeDataPointWithLock(MiscUtils.buildDataPoint("test", "mem", Arrays.asList("value"),
				Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()),
				System.currentTimeMillis() + 10, Arrays.asList(3L), Arrays.asList(false)), false);
		engine.writeDataPointWithLock(MiscUtils.buildDataPoint("test", "netm", Arrays.asList("value"),
				Arrays.asList(Tag.newBuilder().setTagKey("test").setTagValue("2").build()),
				System.currentTimeMillis() + 20, Arrays.asList(5L), Arrays.asList(false)), false);

		Set<String> result = engine.getMeasurementsLike("test", " ");
		assertEquals(3, result.size());

		result = engine.getMeasurementsLike("test", "c.*");
		assertEquals(1, result.size());

		result = engine.getMeasurementsLike("test", ".*m.*");
		assertEquals(2, result.size());
	}

	@Test
	public void testBaseTimeSeriesWrites() throws Exception {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgTasks);
		engine.startup();

		final long ts1 = System.currentTimeMillis();
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 500; k++) {
			final int p = k;
			es.submit(() -> {
				long ts = System.currentTimeMillis();
				for (int i = 0; i < 1000; i++) {
					try {
						engine.writeDataPointWithLock(MiscUtils.buildDataPoint("test", "helo" + p, Arrays.asList("value"),
								Arrays.asList(Tag.newBuilder().setTagKey("k").setTagValue("2").build()), ts + i * 60,
								Arrays.asList(ts + i), Arrays.asList(false)), false);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
		}
		es.shutdown();
		es.awaitTermination(10, TimeUnit.SECONDS);

		System.out.println("Write time:" + (System.currentTimeMillis() - ts1) + "\tms");
	}

}
