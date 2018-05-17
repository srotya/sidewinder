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
package com.srotya.sidewinder.core.storage;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.srotya.sidewinder.core.filters.ComplexTagFilter;
import com.srotya.sidewinder.core.filters.ComplexTagFilter.ComplexFilterType;
import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter.FilterType;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.storage.disk.MappedBitmapTagIndex;
import com.srotya.sidewinder.core.storage.disk.MappedSetTagIndex;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemTagIndex;
import com.srotya.sidewinder.core.storage.mem.MemoryMeasurement;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
@RunWith(Parameterized.class)
public class TestTagIndices {

	private static StorageEngine engine;
	@Parameter
	public Class<TagIndex> clazz;
	private MemoryMeasurement m;
	private TagIndex index;

	@BeforeClass
	public static void beforeClass() throws IOException {
		engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), Executors.newScheduledThreadPool(1));
	}

	@SuppressWarnings("rawtypes")
	@Parameters(name = "{index}: Tag Index Impl Class: {0}")
	public static Collection classes() {
		List<Object[]> implementations = new ArrayList<>();
		implementations.add(new Object[] { MappedBitmapTagIndex.class });
		implementations.add(new Object[] { MappedSetTagIndex.class });
		implementations.add(new Object[] { MemTagIndex.class });
		return implementations;
	}

	@Before
	public void before() throws IOException, InstantiationException, IllegalAccessException {
		m = new MemoryMeasurement();
		DBMetadata md = new DBMetadata(10, 4096, 1024 * 10);
		m.configure(new HashMap<>(), engine, 1024, "", "", "", "", md, null);
		index = clazz.newInstance();
		new File("target/index-common").mkdirs();
		index.configure(new HashMap<>(), "target/index-common", m);
	}

	@After
	public void after() throws IOException {
		MiscUtils.delete(new File("target/index-common"));
	}

	@Test
	public void testDiskTagIndexBasic() throws IOException, InterruptedException {
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 10_000; i++) {
			index.index("key", String.valueOf(i), i);
			ByteString valueOf = new ByteString(String.valueOf(i));
			m.getSeriesList().add(new Series(valueOf, i));
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println("Time:" + ts);

		for (int i = 0; i < 10_000; i++) {
			assertEquals(new HashSet<>(Arrays.asList(new ByteString(String.valueOf(i)))),
					index.searchRowKeysForTagFilter(new SimpleTagFilter(FilterType.EQUALS, "key", String.valueOf(i))));
		}
	}

	@Test
	public void testDiskTagIndexFilterEvaluation() throws IOException, InterruptedException {
		for (int i = 0; i < 10_000; i++) {
			index.index("key", String.valueOf(i), i);
			ByteString valueOf = new ByteString(String.valueOf(i));
			m.getSeriesList().add(new Series(valueOf, i));
		}

		TagFilter filter = new SimpleTagFilter(FilterType.GREATER_THAN, "key", "9");
		Set<ByteString> keys = index.searchRowKeysForTagFilter(filter);
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
		for (int i = 0; i < 10_000; i++) {
			ByteString format = new ByteString(String.format("%04d", i));
			index.index("key", format.toString(), i);
			m.getSeriesList().add(new Series(format, i));
		}

		TagFilter filter = new SimpleTagFilter(FilterType.GREATER_THAN, "key", "9990");
		Set<ByteString> keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(9, keys.size());

		filter = new SimpleTagFilter(FilterType.GREATER_THAN_EQUALS, "key", "9990");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(10, keys.size());

		filter = new SimpleTagFilter(FilterType.LESS_THAN, "key", "0010");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(10, keys.size());

		filter = new SimpleTagFilter(FilterType.LESS_THAN_EQUALS, "key", "0010");
		keys = index.searchRowKeysForTagFilter(filter);
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

		filter = new ComplexTagFilter(ComplexFilterType.OR,
				Arrays.asList(new SimpleTagFilter(FilterType.EQUALS, "key", "9990"),
						new SimpleTagFilter(FilterType.EQUALS, "key", "9991")));
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(2, keys.size());
	}

	@Test
	public void testIndexMultiTags() throws IOException, InterruptedException {
		for (int i = 0; i < 10; i++) {
			ByteString format = new ByteString(String.format("%04d", i));
			index.index("key", format.toString(), i / 2);
			m.getSeriesList().add(new Series(format, i));
		}

		TagFilter filter = new ComplexTagFilter(ComplexFilterType.AND,
				Arrays.asList(new SimpleTagFilter(FilterType.EQUALS, "key", "0000"),
						new SimpleTagFilter(FilterType.EQUALS, "key", "0000")));
		Set<ByteString> keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(1, keys.size());

		filter = new ComplexTagFilter(ComplexFilterType.AND,
				Arrays.asList(new SimpleTagFilter(FilterType.EQUALS, "key", "0000"),
						new SimpleTagFilter(FilterType.EQUALS, "key", "0002")));
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());

		filter = new ComplexTagFilter(ComplexFilterType.AND,
				Arrays.asList(new SimpleTagFilter(FilterType.LESS_THAN, "key", "0004"),
						new SimpleTagFilter(FilterType.LESS_THAN_EQUALS, "key", "0005")));
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(2, keys.size());
	}

	@Test
	public void testDiskTagIndexFilterNegativeTests() throws IOException, InterruptedException {
		for (int i = 0; i < 10_000; i++) {
			ByteString format = new ByteString(String.format("%05d", i));
			index.index("key", format.toString(), i);
			m.getSeriesList().add(new Series(format, i));
		}

		// reindex everything
		for (int i = 0; i < 10_000; i++) {
			ByteString format = new ByteString(String.format("%05d", i));
			index.index("key", format.toString(), i);
			m.getSeriesList().add(new Series(format, i));
		}

		TagFilter filter = new SimpleTagFilter(FilterType.LESS_THAN, "key", "00000");
		Set<ByteString> keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());

		filter = new SimpleTagFilter(FilterType.LESS_THAN_EQUALS, "key", "-1");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());

		filter = new SimpleTagFilter(FilterType.EQUALS, "key", "10000");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());

		filter = new SimpleTagFilter(FilterType.GREATER_THAN, "key", "09999");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());

		filter = new SimpleTagFilter(FilterType.GREATER_THAN, "key", "09999");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());

		filter = new SimpleTagFilter(FilterType.GREATER_THAN, "key", "10000");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());

		filter = new SimpleTagFilter(FilterType.GREATER_THAN_EQUALS, "key", "10000");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(0, keys.size());

		filter = new SimpleTagFilter(FilterType.LIKE, "key", "02.*");
		keys = index.searchRowKeysForTagFilter(filter);
		assertEquals(1000, keys.size());
	}

}