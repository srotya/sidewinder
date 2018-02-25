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
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class TestMappedBitmapTagIndex {

	private static StorageEngine engine;
	private static ScheduledExecutorService bgTasks;

	@BeforeClass
	public static void before() throws IOException {
		engine = new MemStorageEngine();
		bgTasks = Executors.newScheduledThreadPool(1);
		engine.configure(new HashMap<>(), Executors.newScheduledThreadPool(1));
	}

	@Test
	public void testDiskTagIndexBasic() throws IOException, InterruptedException {
		MiscUtils.delete(new File("target/s6"));
		String indexDir = "target/s6";
		new File(indexDir).mkdirs();
		MappedBitmapTagIndex index = new MappedBitmapTagIndex(indexDir, "s2", null);
		long ts = System.currentTimeMillis();
		for (int i = 10_000; i < 1_000_000; i++) {
			index.index("key" + (i % 10000), (i % 1_000_000));
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println("Time:" + ts);
	}

	// @Test
	public void testTagIndexThreaded() throws InterruptedException, IOException {
		String indexDir = "target/s4";
		new File(indexDir).mkdirs();
		MetricsRegistryService.getInstance(engine, bgTasks).getInstance("requests");
		final MappedSetTagIndex index = new MappedSetTagIndex(indexDir, "m2");
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 10; k++) {
			es.submit(() -> {
				try {
					for (int i = 0; i < 1000; i++) {
						String idx = index.mapTag("tag" + (i + 1));
						index.index(idx, "test212");
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		}
		es.shutdown();
		es.awaitTermination(10, TimeUnit.SECONDS);
		for (int i = 0; i < 1000; i++) {
			String entry = index.mapTag("tag" + (i + 1));
			assertEquals("tag" + (i + 1), index.getTagMapping(entry));
			assertEquals("test212", index.searchRowKeysForTag(entry).iterator().next());
		}

		// recover tag index
		MappedSetTagIndex index2 = new MappedSetTagIndex(indexDir, "m2");
		for (int i = 0; i < 1000; i++) {
			String entry = index2.mapTag("tag" + (i + 1));
			assertEquals("tag" + (i + 1), index2.getTagMapping(entry));
			assertEquals("test212", index2.searchRowKeysForTag(entry).iterator().next());
		}
	}

}
