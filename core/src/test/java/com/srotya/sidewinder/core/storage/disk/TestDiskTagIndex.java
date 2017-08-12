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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.mem.MemTagIndex;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class TestDiskTagIndex {

	@Test
	public void testDiskTagIndexBasic() {
		MemTagIndex index = new MemTagIndex();
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

//	@Test
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
		DiskTagIndex value = (DiskTagIndex) itr.getValue().getTagIndex();
		assertEquals(20000 + 10 + 1500, value.getTags().size());
	}

	@Test
	public void testTagIndexThreaded() throws InterruptedException, IOException {
		final DiskTagIndex index = new DiskTagIndex("target/index", "m2");
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 10; k++) {
			es.submit(() -> {
				try {
					for (int i = 0; i < 1000; i++) {
						String idx = index.createEntry("tag" + (i + 1));
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
			String entry = index.createEntry("tag" + (i + 1));
			assertEquals("tag" + (i + 1), index.getEntry(entry));
			assertEquals("test212", index.searchRowKeysForTag(entry).iterator().next());
		}

		DiskTagIndex index2 = new DiskTagIndex("target/index", "m2");
		for (int i = 0; i < 1000; i++) {
			String entry = index2.createEntry("tag" + (i + 1));
			assertEquals("tag" + (i + 1), index2.getEntry(entry));
			assertEquals("test212", index2.searchRowKeysForTag(entry).iterator().next());
		}
	}

}
