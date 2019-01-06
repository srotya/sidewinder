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
package com.srotya.sidewinder.core.qa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.SeriesOutput;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeField;
import com.srotya.sidewinder.core.storage.ValueField;
import com.srotya.sidewinder.core.storage.disk.DiskStorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

@RunWith(Parameterized.class)
public class QADataIntegrity {

	private static final String TARGET_QA_COMMON_TEST = "target/qa-common-test";
	public static ScheduledExecutorService bgTasks;
	private Map<String, String> conf;
	@Parameter
	public Class<StorageEngine> clazz;
	private StorageEngine engine;

	@BeforeClass
	public static void beforeClass() {
		bgTasks = Executors.newScheduledThreadPool(1, new BackgrounThreadFactory("te1"));
		TimeField.compactionRatio = 1.2;
		ValueField.compactionRatio = 1.2;
	}

	@SuppressWarnings("rawtypes")
	@Parameters(name = "{index}: Storage Engine Impl Class: {0}")
	public static Collection classes() {
		List<Object[]> implementations = new ArrayList<>();
		implementations.add(new Object[] { DiskStorageEngine.class });
		implementations.add(new Object[] { MemStorageEngine.class });
		return implementations;
	}

	@AfterClass
	public static void afterClass() {
		bgTasks.shutdown();
	}

	@Before
	public void before() throws InstantiationException, IllegalAccessException, IOException {
		conf = new HashMap<>();
		MiscUtils.delete(new File(TARGET_QA_COMMON_TEST));
		conf.put("data.dir", TARGET_QA_COMMON_TEST + "/data");
		conf.put("index.dir", TARGET_QA_COMMON_TEST + "/index");
		conf.put("gc.enabled", "false");
		engine = clazz.newInstance();
	}

	@After
	public void after() throws IOException {
		engine.shutdown();
		MiscUtils.delete(new File(TARGET_QA_COMMON_TEST));
	}

	@Test
	public void testCompactions() throws Exception {
		conf.put("compaction.delay", "1");
		conf.put("compaction.frequency", "2");
		conf.put("compaction.codec", "gorilla");
		engine.configure(conf, bgTasks);

		long ts = System.currentTimeMillis();
		String measurementName = "cpu";
		String dbName = "db1";
		for (int i = 0; i < 100000; i++) {
			for (int l = 0; l < 10; l++) {
				Point dp = Point.newBuilder().setDbName(dbName).setMeasurementName(measurementName)
						.addTags(Tag.newBuilder().setTagKey("host").setTagValue(String.valueOf(l)).build()).addFp(false)
						.addValueFieldName("user").addValue(Double.doubleToLongBits(21.2 * i))
						.setTimestamp(ts + i * 1000).build();
				engine.writeDataPointWithLock(dp, false);
			}
		}

		Thread.sleep(4000);

		// check fields
		Set<String> fields = engine.getFieldsForMeasurement(dbName, measurementName);
		assertEquals(2, fields.size());
		assertTrue(fields.contains("user"));

		// check tag keys
		Set<String> tagKeys = engine.getTagKeysForMeasurement(dbName, measurementName);
		assertEquals(1, tagKeys.size());
		assertEquals("host", tagKeys.iterator().next());

		// check tag filters
		TagFilter filter = MiscUtils.buildSimpleFilter("host~.*");
		Set<String> rowKeys = engine.getTagFilteredRowKeys(dbName, measurementName, filter);
		assertEquals(10, rowKeys.size());

		// check dataset
		List<SeriesOutput> all = engine.queryDataPoints(dbName, measurementName, "user", ts - 1000 * 10,
				ts + 1000 * 100, filter);
		assertEquals(10, all.size());

		// Measurement m = engine.getMeasurementMap().get(dbName).get(measurementName);
	}

	@Test
	public void testParallelInsert() throws Exception {
		engine.configure(conf, bgTasks);
		ExecutorService es = Executors.newCachedThreadPool();

		long ts = System.currentTimeMillis();

		final AtomicBoolean b = new AtomicBoolean(false);
		String measurementName = "cpu";
		String dbName = "db1";
		for (int k = 0; k < 10; k++) {
			final int p = k;
			es.submit(() -> {
				while (!b.get()) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				try {

					for (int i = 0; i < 10000; i++) {
						long tso = System.currentTimeMillis() + p * 10;
						for (int l = 0; l < 10; l++) {
							Point dp = Point.newBuilder().setDbName(dbName).setMeasurementName(measurementName)
									.addTags(Tag.newBuilder().setTagKey("host").setTagValue(String.valueOf(l)).build())
									.addFp(false).addValueFieldName("user").addValue(2112).setTimestamp(tso + i * 1000)
									.build();
							engine.writeDataPointWithLock(dp, false);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}

		b.set(true);
		es.shutdown();
		es.awaitTermination(100, TimeUnit.SECONDS);

		// check fields
		Set<String> fields = engine.getFieldsForMeasurement(dbName, measurementName);
		assertEquals(2, fields.size());
		assertTrue(fields.contains("user"));

		// check tag keys
		Set<String> tagKeys = engine.getTagKeysForMeasurement(dbName, measurementName);
		assertEquals(1, tagKeys.size());
		assertEquals("host", tagKeys.iterator().next());

		// check tag filters
		TagFilter filter = MiscUtils.buildSimpleFilter("host~.*");
		Set<String> rowKeys = engine.getTagFilteredRowKeys(dbName, measurementName, filter);
		assertEquals(10, rowKeys.size());

		// check dataset
		List<SeriesOutput> all = engine.queryDataPoints(dbName, measurementName, "user", ts - 1000 * 10,
				ts + 1000 * 100, filter);
		assertEquals(10, all.size());
		for (SeriesOutput s : all) {
			assertEquals(s.getTags() + " bad datapoints", 1000, s.getDataPoints().size());
		}

		for (int i = 0; i < 10; i++) {
			filter = MiscUtils.buildSimpleFilter("host=" + i);
			List<SeriesOutput> points = engine.queryDataPoints(dbName, measurementName, "user", ts - 1000 * 10,
					ts + 1000 * 100, filter);
			assertEquals(1, points.size());
			SeriesOutput next = points.iterator().next();
			assertEquals(next.getTags() + " bad datapoints", 1000, next.getDataPoints().size());
			for (DataPoint dp : next.getDataPoints()) {
				assertEquals(2112, dp.getLongValue());
			}
		}
	}

}
