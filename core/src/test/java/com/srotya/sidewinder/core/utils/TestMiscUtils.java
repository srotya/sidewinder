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
package com.srotya.sidewinder.core.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.srotya.sidewinder.core.filters.Filter;

/**
 * @author ambud
 */
public class TestMiscUtils {

	@Test
	public void testBuildTagFilter() {
		List<String> tags = new ArrayList<>();
		Filter<List<String>> filter = MiscUtils.buildTagFilter("host1&test|tree", tags);
		assertEquals(3, tags.size());
		assertTrue(filter.isRetain(Arrays.asList("host1", "test")));
		assertTrue(filter.isRetain(Arrays.asList("tree")));
		assertTrue(!filter.isRetain(Arrays.asList("test")));
		tags.clear();

		filter = MiscUtils.buildTagFilter("user1&test1", tags);
		assertEquals(2, tags.size());
		assertTrue(!filter.isRetain(Arrays.asList("test1")));
		assertTrue(!filter.isRetain(Arrays.asList("user1")));
		assertTrue(filter.isRetain(Arrays.asList("user1", "test1")));

		filter = MiscUtils.buildTagFilter("user1|test1", tags);
		assertTrue(filter.isRetain(Arrays.asList("test1")));
		assertTrue(filter.isRetain(Arrays.asList("user1")));
		assertTrue(filter.isRetain(Arrays.asList("user1", "test1")));
	}
	
	@Test
	public void testCreateAggregateFunctionValid() throws InstantiationException, IllegalAccessException, Exception {
		String[] parts = new String[] { "", "derivative,10,smean" };
		MiscUtils.createAggregateFunction(parts);
	}

	@Test
	public void testCreateAggregateFunctionInvalid() throws InstantiationException, IllegalAccessException, Exception {
		try {
			String[] parts = new String[] { "", "derivative,10,mean" };
			MiscUtils.createAggregateFunction(parts);
			fail("must throw an exception");
		} catch (IllegalArgumentException e) {
		}

		try {
			String[] parts = new String[] { "", "derivative,10,sum" };
			MiscUtils.createAggregateFunction(parts);
			fail("must throw an exception");
		} catch (IllegalArgumentException e) {
		}

		try {
			String[] parts = new String[] { "", "derivative,test,ssum" };
			MiscUtils.createAggregateFunction(parts);
			fail("must throw an exception");
		} catch (Exception e) {
		}

		try {
			String[] parts = new String[] { "", "ssum,test,ssum" };
			MiscUtils.createAggregateFunction(parts);
		} catch (Exception e) {
			fail("must NOT throw an exception");
		}
	}
	
}
