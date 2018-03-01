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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter.FilterType;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class TestMappedTagIndex {

	private static StorageEngine engine;
	private static ScheduledExecutorService bgTasks;

	@BeforeClass
	public static void before() throws IOException {
		engine = new MemStorageEngine();
		bgTasks = Executors.newScheduledThreadPool(1);
		engine.configure(new HashMap<>(), Executors.newScheduledThreadPool(1));
	}

	@Test
	public void testDiskTagIndexBasic() throws IOException {
		MiscUtils.delete(new File("target/i5"));
		String indexDir = "target/i5";
		new File(indexDir).mkdirs();
		MappedSetTagIndex index = new MappedSetTagIndex(indexDir, "i2");
		for (int i = 0; i < 1000; i++) {
			String idx = index.mapTagKey("tag");
			index.index(idx, index.mapTagValue(String.valueOf(i + 1)), "test212");
		}
		for (int i = 0; i < 1000; i++) {
			String entry = index.mapTagKey("tag");
			assertEquals("tag", index.getTagKeyMapping(entry));
			assertEquals(String.valueOf(i + 1), index.getTagValueMapping(index.mapTagValue(String.valueOf((i + 1)))));
			// assertEquals("test212", index.searchRowKeysForTag(entry).iterator().next());
		}
	}

	@Test
	public void testDiskTagIndexFilterEvaluation() throws IOException, InterruptedException {
		MiscUtils.delete(new File("target/i7"));
		String indexDir = "target/i7";
		new File(indexDir).mkdirs();
		MappedSetTagIndex index = new MappedSetTagIndex(indexDir, "s7");
		for (int i = 0; i < 10_000; i++) {
			index.index("key", String.valueOf(i), String.valueOf(i));
		}

		TagFilter filter = new SimpleTagFilter(FilterType.GREATER_THAN, "key", "9");
		Set<String> keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(1110, keys.size());

		filter = new SimpleTagFilter(FilterType.GREATER_THAN_EQUALS, "key", "9");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(1111, keys.size());

		filter = new SimpleTagFilter(FilterType.LESS_THAN, "key", "10");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(2, keys.size());

		filter = new SimpleTagFilter(FilterType.LESS_THAN_EQUALS, "key", "1000");
		keys = index.searchRowKeysForTagFilter(filter);
		// keys.stream().forEach(System.out::println);
		assertEquals(5, keys.size());
	}

	@Test
	public void testDiskTagIndexFilterEvaluationNormalized() throws IOException, InterruptedException {
		MiscUtils.delete(new File("target/i8"));
		String indexDir = "target/i8";
		new File(indexDir).mkdirs();
		MappedSetTagIndex index = new MappedSetTagIndex(indexDir, "s8");
		for (int i = 0; i < 10_000; i++) {
			String value = String.format("%04d", i);
			index.index("key", value, String.valueOf(i));
		}

		TagFilter filter = new SimpleTagFilter(FilterType.EQUALS, "key", "9990");
		Set<String> keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(1, keys.size());

		filter = new SimpleTagFilter(FilterType.GREATER_THAN, "key", "9990");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(9, keys.size());

		filter = new SimpleTagFilter(FilterType.GREATER_THAN_EQUALS, "key", "9990");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(10, keys.size());

		filter = new SimpleTagFilter(FilterType.LESS_THAN, "key", "0010");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(10, keys.size());

		filter = new SimpleTagFilter(FilterType.LESS_THAN_EQUALS, "key", "0010");
		keys = index.searchRowKeysForTagFilter(filter);
		// keys.stream().forEach(System.out::println);
		assertEquals(11, keys.size());
	}

	// @Test
	public void testTagIndexPerformance() throws IOException, InterruptedException {
		MiscUtils.delete(new File("target/perf/index-dir"));
		MiscUtils.delete(new File("target/perf/data-dir"));
		DiskStorageEngine engine = new DiskStorageEngine();
		HashMap<String, String> conf = new HashMap<>();
		conf.put("index.dir", "target/perf/index-dir");
		conf.put("data.dir", "target/perf/data-dir");
		engine.configure(conf, Executors.newScheduledThreadPool(1, new BackgrounThreadFactory("bgt")));
		final long ms = System.currentTimeMillis();
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 6; k++) {
			es.submit(() -> {
				for (int i = 0; i < 200_000_000; i++) {
					try {
						engine.getOrCreateTimeSeries("db1", "m1", "v10",
								Arrays.asList(String.valueOf(i % 10_000), "test=" + String.valueOf(i % 5),
										"test2=" + String.valueOf(i % 5), "goliath=" + String.valueOf(i % 10_000),
										"goliath2=" + String.valueOf(i % 1_500)),
								4096, true);
					} catch (IOException e) {
						e.printStackTrace();
					}
					if (i % 1_000_000 == 0) {
						System.out.println(i + "\t" + (System.currentTimeMillis() - ms) / 1000);
					}
				}
			});
		}
		es.shutdown();
		es.awaitTermination(1000, TimeUnit.SECONDS);
		System.err.println("Index time:" + (System.currentTimeMillis() - ms));
		Map<String, Map<String, Measurement>> index = engine.getMeasurementMap();
		assertEquals(1, index.size());
		Entry<String, Map<String, Measurement>> next = index.entrySet().iterator().next();
		assertEquals("db1", next.getKey());
		Entry<String, Measurement> itr = next.getValue().entrySet().iterator().next();
		assertEquals("m1", itr.getKey());
		MappedSetTagIndex value = (MappedSetTagIndex) itr.getValue().getTagIndex();
		assertEquals(20000 + 10 + 1500, value.getTags().size());
	}

	@Test
	public void testTagIndexThreaded() throws InterruptedException, IOException {
		String indexDir = "target/i4";
		new File(indexDir).mkdirs();
		MetricsRegistryService.getInstance(engine, bgTasks).getInstance("requests");
		final MappedSetTagIndex index = new MappedSetTagIndex(indexDir, "m2");
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 10; k++) {
			es.submit(() -> {
				try {
					for (int i = 0; i < 1000; i++) {
						String idx = index.mapTagKey("tag");
						index.index(idx, index.mapTagValue(String.valueOf(i + 1)), "test212");
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		}
		es.shutdown();
		es.awaitTermination(10, TimeUnit.SECONDS);
		// for (int i = 0; i < 1000; i++) {
		// String entry = index.mapTagKey("tag" + (i + 1));
		// assertEquals("tag" + (i + 1), index.getTagKeyMapping(entry));
		// assertEquals("test212", index.searchRowKeysForTag(entry).iterator().next());
		// }
		//
		// MappedSetTagIndex index2 = new MappedSetTagIndex(indexDir, "m2");
		// for (int i = 0; i < 1000; i++) {
		// String entry = index2.mapTagKey("tag" + (i + 1));
		// assertEquals("tag" + (i + 1), index2.getTagKeyMapping(entry));
		// assertEquals("test212", index2.searchRowKeysForTag(entry).iterator().next());
		// }
	}

}
