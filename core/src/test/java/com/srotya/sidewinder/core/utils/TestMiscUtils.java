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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.BadRequestException;

import org.junit.Test;

import com.srotya.sidewinder.core.api.grafana.TargetSeries;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public class TestMiscUtils {

	@Test
	public void testTagToString() {
		String tagString = MiscUtils.tagToString(Arrays.asList("test", "test2"));
		assertEquals("/test/test2", tagString);
	}

	@Test
	public void testBuildDataPoint() {
		DataPoint dp = MiscUtils.buildDataPoint(10L, 10.1);
		assertEquals(10L, dp.getTimestamp(), 0);
		assertEquals(10.1, dp.getValue(), 0);
		assertTrue(dp.getDbName() == null);

		dp = MiscUtils.buildDataPoint(10L, 10L);
		assertEquals(10L, dp.getTimestamp(), 0);
		assertEquals(10, dp.getLongValue(), 0);

		dp = MiscUtils.buildDataPoint("test", "test2", "test3", Arrays.asList("test6"), 10L, 10.1);
		assertEquals(10L, dp.getTimestamp(), 0);
		assertEquals(10.1, dp.getValue(), 0);
		
		dp = MiscUtils.buildDataPoint("test", "test2", "test3", Arrays.asList("test6"), 10L, 10L);
		assertEquals(10L, dp.getTimestamp(), 0);
		assertEquals(10, dp.getLongValue(), 0);
	}

	@Test
	public void testExtractTargetFromQuery() {
		TargetSeries series = MiscUtils.extractTargetFromQuery("cpu.value.tes=2|tes=3");
		assertEquals("cpu", series.getMeasurementName());
		assertEquals("value", series.getFieldName());
		assertEquals(Arrays.asList("tes=2", "tes=3"), series.getTagList());
		assertTrue(series.getTagFilter().isRetain(Arrays.asList("tes=2", "tes=3")));
		assertTrue(series.getTagFilter().isRetain(Arrays.asList("tes=2")));
		assertTrue(series.getTagFilter().isRetain(Arrays.asList("tes=3")));
		assertTrue(!series.getTagFilter().isRetain(Arrays.asList("tes=4")));

		try {
			series = MiscUtils.extractTargetFromQuery("cpuvalue|tes=3");
			fail("Invalid request must throw an exception");
		} catch (BadRequestException e) {
		}
	}

	@Test
	public void testSplitNormalizeString() {
		String[] splits = MiscUtils.splitAndNormalizeString("/data/drive1, /data/drive2,/data/drive3");
		for (String split : splits) {
			assertTrue(!split.contains(" "));
		}
	}

	@Test
	public void testReadAllLines() throws IOException {
		PrintWriter pr = new PrintWriter(new File("target/filereadtest.txt"));
		pr.append("this is a test\nthis is a test");
		pr.close();
		List<String> lines = MiscUtils.readAllLines(new File("target/filereadtest.txt"));
		for (String line : lines) {
			assertEquals("this is a test", line);
		}
	}

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
