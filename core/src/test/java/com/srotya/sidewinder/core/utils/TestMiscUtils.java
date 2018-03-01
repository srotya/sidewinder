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
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.BadRequestException;

import org.junit.Test;

import com.srotya.sidewinder.core.api.grafana.TargetSeries;
import com.srotya.sidewinder.core.filters.ComplexTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter.FilterType;
import com.srotya.sidewinder.core.filters.ComplexTagFilter.ComplexFilterType;
import com.srotya.sidewinder.core.filters.TagFilter;
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

		dp = MiscUtils.buildDataPoint(10L, 10L);
		assertEquals(10L, dp.getTimestamp(), 0);
		assertEquals(10, dp.getLongValue(), 0);

	}

	@Test
	public void testExtractTargetFromQuery() {
		TargetSeries series = MiscUtils.extractTargetFromQuery("cpu.value.tes=2|tes=3");
		assertEquals("cpu", series.getMeasurementName());
		assertEquals("value", series.getFieldName());
		// assertEquals(Arrays.asList("tes=2", "tes=3"), series.getTagList());

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
	public void testBuildTagFilter() throws InvalidFilterException {
		TagFilter filter = MiscUtils.buildTagFilter("host=1&test=2|tree=1");
		assertEquals(ComplexTagFilter.class, filter.getClass());
		ComplexTagFilter complex = ((ComplexTagFilter) filter);
		assertEquals(ComplexFilterType.OR, complex.getType());
		assertEquals(2, complex.getFilters().size());
		assertEquals(ComplexTagFilter.class, complex.getFilters().get(0).getClass());
		assertEquals(SimpleTagFilter.class, complex.getFilters().get(1).getClass());

		filter = MiscUtils.buildTagFilter("user>=1&test<1");
		complex = ((ComplexTagFilter) filter);
		assertEquals(ComplexFilterType.AND, complex.getType());
		assertEquals(2, complex.getFilters().size());
		SimpleTagFilter sfilter = ((SimpleTagFilter) complex.getFilters().get(0));
		assertEquals("user", sfilter.getTagKey());
		assertEquals("1", sfilter.getComparedValue());
		assertEquals(FilterType.GREATER_THAN_EQUALS, sfilter.getFilterType());
		sfilter = ((SimpleTagFilter) complex.getFilters().get(1));
		assertEquals(FilterType.LESS_THAN, sfilter.getFilterType());
		assertEquals("test", sfilter.getTagKey());
		assertEquals("1", sfilter.getComparedValue());

		filter = MiscUtils.buildTagFilter("user<=1|test>1");
		complex = ((ComplexTagFilter) filter);
		assertEquals(ComplexFilterType.OR, complex.getType());
		assertEquals(2, complex.getFilters().size());
		sfilter = ((SimpleTagFilter) complex.getFilters().get(0));
		assertEquals("user", sfilter.getTagKey());
		assertEquals("1", sfilter.getComparedValue());
		assertEquals(FilterType.LESS_THAN_EQUALS, sfilter.getFilterType());
		sfilter = ((SimpleTagFilter) complex.getFilters().get(1));
		assertEquals(FilterType.GREATER_THAN, sfilter.getFilterType());
		assertEquals("test", sfilter.getTagKey());
		assertEquals("1", sfilter.getComparedValue());
	}

	@Test
	public void testCreateAggregateFunctionValid() throws InstantiationException, IllegalAccessException, Exception {
		String[] parts = new String[] { "derivative,10,smean" };
		MiscUtils.createFunctionChain(parts, 0);
	}

	@Test
	public void testCreateAggregateFunctionInvalid() throws InstantiationException, IllegalAccessException, Exception {
		try {
			String[] parts = new String[] { "", "derivative,10,mean" };
			MiscUtils.createFunctionChain(parts, 1);
			fail("must throw an exception");
		} catch (IllegalArgumentException e) {
		}

		try {
			String[] parts = new String[] { "", "derivative,10,sum" };
			MiscUtils.createFunctionChain(parts, 1);
			fail("must throw an exception");
		} catch (IllegalArgumentException e) {
		}

		try {
			String[] parts = new String[] { "", "derivative,test,ssum" };
			MiscUtils.createFunctionChain(parts, 1);
			fail("must throw an exception");
		} catch (Exception e) {
		}

		try {
			String[] parts = new String[] { "", "ssum,test,ssum" };
			MiscUtils.createFunctionChain(parts, 1);
		} catch (Exception e) {
			e.printStackTrace();
			fail("must NOT throw an exception");
		}
	}

}
