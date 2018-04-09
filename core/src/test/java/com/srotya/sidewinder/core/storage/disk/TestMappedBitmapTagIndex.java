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
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter.FilterType;
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.SeriesFieldMap;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class TestMappedBitmapTagIndex {

	private static StorageEngine engine;

	@BeforeClass
	public static void before() throws IOException {
		engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), Executors.newScheduledThreadPool(1));
	}

	@Test
	public void testIndexRecovery() throws IOException, InterruptedException {
		MiscUtils.delete(new File("target/s9"));
		String indexDir = "target/s9";
		new File(indexDir).mkdirs();
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> conf = new HashMap<>();
		m.configure(conf, engine, 4096, "d", "m", "target/s9/i/bitmap", "target/s9/d/bitmap", new DBMetadata(), null);
		MappedBitmapTagIndex index = new MappedBitmapTagIndex();
		index.configure(conf, "target/s9/i/bitmap", m);
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 10_000; i++) {
			index.index("key", String.valueOf(i), i);
			ByteString valueOf = new ByteString(String.valueOf(i));
			m.getSeriesListAsList().add(new SeriesFieldMap(valueOf, i));
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println("Time:" + ts);

		for (int i = 0; i < 10_000; i++) {
			assertEquals(new HashSet<>(Arrays.asList(new ByteString(String.valueOf(i)))),
					index.searchRowKeysForTagFilter(new SimpleTagFilter(FilterType.EQUALS, "key", String.valueOf(i))));
		}

		for (int k = 0; k < 10; k++) {
			index = new MappedBitmapTagIndex();
			index.configure(conf, "target/s9/i/bitmap", m);
			for (int i = 0; i < 10_000; i++) {
				assertEquals(new HashSet<>(Arrays.asList(new ByteString(String.valueOf(i)))), index
						.searchRowKeysForTagFilter(new SimpleTagFilter(FilterType.EQUALS, "key", String.valueOf(i))));
			}
		}
	}

	@Test
	public void testMultiIndexRecovery() throws IOException, InterruptedException {
		MiscUtils.delete(new File("target/s9"));
		String indexDir = "target/s9";
		new File(indexDir).mkdirs();
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> conf = new HashMap<>();
		m.configure(conf, engine, 4096, "d", "m", "target/s9/i/bitmap", "target/s9/d/bitmap", new DBMetadata(), null);
		MappedBitmapTagIndex index = new MappedBitmapTagIndex();
		index.configure(conf, "target/s9/i/bitmap", m);
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 10_000; i++) {
			index.index("key", String.valueOf(i), i);
			index.index("key", String.valueOf(i - 1), i);
			ByteString valueOf = new ByteString(String.valueOf(i));
			m.getSeriesListAsList().add(new SeriesFieldMap(valueOf, i));
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println("Time:" + ts);

		for (int i = 0; i < 9999; i++) {
			assertEquals(
					new HashSet<>(
							Arrays.asList(new ByteString(String.valueOf(i)), new ByteString(String.valueOf(i + 1)))),
					index.searchRowKeysForTagFilter(new SimpleTagFilter(FilterType.EQUALS, "key", String.valueOf(i))));
		}

		for (int k = 0; k < 10; k++) {
			index = new MappedBitmapTagIndex();
			index.configure(conf, "target/s9/i/bitmap", m);
			for (int i = 1; i < 9999; i++) {
				assertEquals(
						new HashSet<>(Arrays.asList(new ByteString(String.valueOf(i)),
								new ByteString(String.valueOf(i + 1)))),
						index.searchRowKeysForTagFilter(
								new SimpleTagFilter(FilterType.EQUALS, "key", String.valueOf(i))));
			}
		}
	}

}