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
import java.util.Set;
import java.util.concurrent.Executors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter.FilterType;
import com.srotya.sidewinder.core.filters.TagFilter;
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
	public void testDiskTagIndexBasic() throws IOException, InterruptedException {
		MiscUtils.delete(new File("target/s6"));
		String indexDir = "target/s6";
		new File(indexDir).mkdirs();
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> conf = new HashMap<>();
		m.configure(conf, engine, "d", "m", "target/s6/i/bitmap", "target/s6/d/bitmap", new DBMetadata(), null);
		MappedBitmapTagIndex index = new MappedBitmapTagIndex(indexDir, "s6", m);
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 10_000; i++) {
			index.index("key", String.valueOf(i), i);
			String valueOf = String.valueOf(i);
			m.getSeriesListAsList().add(new SeriesFieldMap(valueOf));
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println("Time:" + ts);

		for (int i = 0; i < 10_000; i++) {
			assertEquals(new HashSet<>(Arrays.asList(String.valueOf(i))),
					index.searchRowKeysForTagFilter(new SimpleTagFilter(FilterType.EQUALS, "key", String.valueOf(i))));
		}
	}

	@Test
	public void testDiskTagIndexFilterEvaluation() throws IOException, InterruptedException {
		MiscUtils.delete(new File("target/s7"));
		String indexDir = "target/s7";
		new File(indexDir).mkdirs();
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> conf = new HashMap<>();
		m.configure(conf, engine, "d", "m", "target/s7/i/bitmap", "target/s7/d/bitmap", new DBMetadata(), null);
		MappedBitmapTagIndex index = new MappedBitmapTagIndex(indexDir, "s7", m);
		for (int i = 0; i < 10_000; i++) {
			index.index("key", String.valueOf(i), i);
			String valueOf = String.valueOf(i);
			m.getSeriesListAsList().add(new SeriesFieldMap(valueOf));
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
		MiscUtils.delete(new File("target/s8"));
		String indexDir = "target/s8";
		new File(indexDir).mkdirs();
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> conf = new HashMap<>();
		m.configure(conf, engine, "d", "m", "target/s8/i/bitmap", "target/s8/d/bitmap", new DBMetadata(), null);
		MappedBitmapTagIndex index = new MappedBitmapTagIndex(indexDir, "s8", m);
		for (int i = 0; i < 10_000; i++) {
			String format = String.format("%04d", i);
			index.index("key", format, i);
			m.getSeriesListAsList().add(new SeriesFieldMap(format));
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
	}

	@Test
	public void testIndexRecovery() throws IOException, InterruptedException {
		MiscUtils.delete(new File("target/s9"));
		String indexDir = "target/s9";
		new File(indexDir).mkdirs();
		PersistentMeasurement m = new PersistentMeasurement();
		Map<String, String> conf = new HashMap<>();
		m.configure(conf, engine, "d", "m", "target/s9/i/bitmap", "target/s9/d/bitmap", new DBMetadata(), null);
		MappedBitmapTagIndex index = new MappedBitmapTagIndex(indexDir, "s9", m);
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 10_000; i++) {
			index.index("key", String.valueOf(i), i);
			String valueOf = String.valueOf(i);
			m.getSeriesListAsList().add(new SeriesFieldMap(valueOf));
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println("Time:" + ts);

		for (int i = 0; i < 10_000; i++) {
			assertEquals(new HashSet<>(Arrays.asList(String.valueOf(i))),
					index.searchRowKeysForTagFilter(new SimpleTagFilter(FilterType.EQUALS, "key", String.valueOf(i))));
		}

		for (int k = 0; k < 10; k++) {
			index = new MappedBitmapTagIndex(indexDir, "s9", m);
			for (int i = 0; i < 10_000; i++) {
				assertEquals(new HashSet<>(Arrays.asList(String.valueOf(i))), index
						.searchRowKeysForTagFilter(new SimpleTagFilter(FilterType.EQUALS, "key", String.valueOf(i))));
			}
		}
	}

}