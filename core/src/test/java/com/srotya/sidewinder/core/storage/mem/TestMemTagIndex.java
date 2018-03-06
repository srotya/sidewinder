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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.filters.ComplexTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter.FilterType;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.filters.ComplexTagFilter.ComplexFilterType;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class TestMemTagIndex {

	private static StorageEngine engine;
	private static ScheduledExecutorService bgTasks;

	@BeforeClass
	public static void before() throws IOException {
		engine = new MemStorageEngine();
		bgTasks = Executors.newScheduledThreadPool(1);
		engine.configure(new HashMap<>(), bgTasks);
	}

	@Test
	public void testTagIndexBasic() {
		MemTagIndex index = new MemTagIndex(
				MetricsRegistryService.getInstance(engine, bgTasks).getInstance("requests"));
		for (int i = 0; i < 1000; i++) {
			index.index("tag", String.valueOf(i + 1), "test212");
		}

	}

	// @Test
	public void testTagIndexPerformance() throws IOException, InterruptedException {
		MemStorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), null);
		long ms = System.currentTimeMillis();
		ExecutorService es = Executors.newFixedThreadPool(7);
		for (int k = 0; k < 7; k++) {
			es.submit(() -> {
				for (int i = 0; i < 30_000_000; i++) {
					try {
						engine.getOrCreateTimeSeries("db1", "m1", "v10",
								Arrays.asList(String.valueOf(i % 1_000_000), "test=" + String.valueOf(i % 5),
										"goliath=" + String.valueOf(i % 1000), "goliath2=" + String.valueOf(i % 150)),
								4096, true);
					} catch (IOException e) {
						e.printStackTrace();
					}
					if (i % 100000 == 0)
						System.out.println(i);
				}
			});
		}
		es.shutdownNow();
		es.awaitTermination(1000, TimeUnit.SECONDS);
		ms = System.currentTimeMillis() - ms;
		System.err.println("Index time:" + ms);
	}

	@Test
	public void testTagIndexFilterEvaluation() throws IOException, InterruptedException {
		MemTagIndex index = new MemTagIndex(null);
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
		
		filter = new ComplexTagFilter(ComplexFilterType.AND,
				Arrays.asList(new SimpleTagFilter(FilterType.EQUALS, "key", "9990"),
						new SimpleTagFilter(FilterType.EQUALS, "key", "9991")));
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());
		
		filter = new ComplexTagFilter(ComplexFilterType.AND,
				Arrays.asList(new SimpleTagFilter(FilterType.EQUALS, "key1", "9990"),
						new SimpleTagFilter(FilterType.EQUALS, "key", "9991")));
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());
	}

	@Test
	public void testDiskTagIndexFilterEvaluationNormalized() throws IOException, InterruptedException {
		MemTagIndex index = new MemTagIndex(null);
		for (int i = 0; i < 10_000; i++) {
			String format = String.format("%04d", i);
			index.index("key", format, format);
		}

		TagFilter filter = new SimpleTagFilter(FilterType.GREATER_THAN, "key", "9990");
		Set<String> keys = index.searchRowKeysForTagFilter(filter);
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
		
		filter = new ComplexTagFilter(ComplexFilterType.AND,
				Arrays.asList(new SimpleTagFilter(FilterType.EQUALS, "key", "9990"),
						new SimpleTagFilter(FilterType.EQUALS, "key", "9991")));
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());
		
		filter = new ComplexTagFilter(ComplexFilterType.AND,
				Arrays.asList(new SimpleTagFilter(FilterType.EQUALS, "key1", "9990"),
						new SimpleTagFilter(FilterType.EQUALS, "key", "9991")));
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());
		
		filter = new ComplexTagFilter(ComplexFilterType.AND,
				Arrays.asList(new SimpleTagFilter(FilterType.EQUALS, "key", "9990"),
						new SimpleTagFilter(FilterType.EQUALS, "key1", "9991")));
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());
		
		filter = new ComplexTagFilter(ComplexFilterType.OR,
				Arrays.asList(new SimpleTagFilter(FilterType.EQUALS, "key1", "9990"),
						new SimpleTagFilter(FilterType.EQUALS, "key", "9991")));
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(1, keys.size());
	}

}
