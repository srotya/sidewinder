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
package com.srotya.sidewinder.core.filters;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.srotya.sidewinder.core.filters.tags.ContainsFilter;

/**
 * Unit tests for {@link AndFilter}, {@link OrFilter}, {@link ContainsFilter},
 * {@link NotFilter} and {@link AnyFilter}
 * 
 * @author ambud
 */
public class TestFilters {

	@Test
	public void testContainsFilter() {
		Filter<List<String>> filter = new ContainsFilter<String, List<String>>("cap");
		assertTrue(!filter.retain(Arrays.asList("test", "10", "hello")));
		assertTrue(filter.retain(Arrays.asList("test", "10", "cap")));
	}

	@Test
	public void testAndFilter() {
		Filter<List<String>> filter = new AndFilter<>(Arrays.asList(new ContainsFilter<String, List<String>>("cap"),
				new ContainsFilter<String, List<String>>("10")));
		assertTrue(!filter.retain(Arrays.asList("test", "10", "hello")));
		assertTrue(filter.retain(Arrays.asList("test", "10", "cap")));
		assertTrue(!filter.retain(Arrays.asList("test", "12", "ca2p")));
	}

	@Test
	public void testOrFilter() {
		Filter<List<String>> filter = new OrFilter<>(Arrays.asList(new ContainsFilter<String, List<String>>("cap"),
				new ContainsFilter<String, List<String>>("10")));
		assertTrue(filter.retain(Arrays.asList("test", "10", "hello")));
		assertTrue(filter.retain(Arrays.asList("test", "10", "cap")));
		assertTrue(!filter.retain(Arrays.asList("test", "12", "ca2p")));
	}

	@Test
	public void testAnyFilter() {
		Filter<List<String>> filter = new AnyFilter<List<String>>();
		assertTrue(filter.retain(Arrays.asList("test", "10", "hello")));
		assertTrue(filter.retain(Arrays.asList("test", "10", "cap")));
		assertTrue(filter.retain(Arrays.asList("test", "12", "ca2p")));
	}

	@Test
	public void testNotFilter() {
		Filter<List<String>> filter = new NotFilter<>(new ContainsFilter<String, List<String>>("10"));
		assertTrue(!filter.retain(Arrays.asList("test", "10", "hello")));
		assertTrue(!filter.retain(Arrays.asList("test", "10", "cap")));
		assertTrue(filter.retain(Arrays.asList("test", "12", "ca2p")));
	}

	@Test
	public void testComplexFilter() {
		Filter<List<String>> filter = new OrFilter<>(Arrays.asList(new ContainsFilter<String, List<String>>("cap"),
				new ContainsFilter<String, List<String>>("10"),
				new AndFilter<>(Arrays.asList(new ContainsFilter<String, List<String>>("cap"),
						new ContainsFilter<String, List<String>>("12")))));
		assertTrue(filter.retain(Arrays.asList("test", "10", "hello")));
		assertTrue(filter.retain(Arrays.asList("test", "10", "cap")));
		assertTrue(!filter.retain(Arrays.asList("test", "12", "ca2p")));
	}

}
