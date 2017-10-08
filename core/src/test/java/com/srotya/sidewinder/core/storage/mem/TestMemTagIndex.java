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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class TestMemTagIndex {

	private static StorageEngine engine;

	@BeforeClass
	public static void before() throws IOException {
		engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), Executors.newScheduledThreadPool(1));
	}

	@Test
	public void testTagIndexBasic() {
		MemTagIndex index = new MemTagIndex(MetricsRegistryService.getInstance(engine).getInstance("requests"));
		for (int i = 0; i < 1000; i++) {
			String idx = index.createEntry("tag" + (i + 1));
			index.index(idx, "test212");
		}

		for (int i = 0; i < 1000; i++) {
			String entry = index.createEntry("tag" + (i + 1));

			assertEquals("tag" + (i + 1), index.getEntry(entry));
			assertEquals("test212", index.searchRowKeysForTag(entry).iterator().next());
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
	public void testTagIndexThreaded() throws InterruptedException {
		MemTagIndex index = new MemTagIndex(MetricsRegistryService.getInstance(engine).getInstance("requests"));
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 10; k++) {
			es.submit(() -> {
				for (int i = 0; i < 1000; i++) {
					String idx = index.createEntry("tag" + (i + 1));
					index.index(idx, "test212");
				}
			});
		}
		es.shutdown();
		es.awaitTermination(10, TimeUnit.SECONDS);
		for (int i = 0; i < 1000; i++) {
			String entry = index.createEntry("tag" + (i + 1));
			assertEquals("tag" + (i + 1), index.getEntry(entry));
			assertEquals("test212", index.searchRowKeysForTag(entry).iterator().next());
		}
	}

}
