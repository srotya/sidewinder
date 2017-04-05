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
package com.srotya.sidewinder.core.ingress.http;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * Unit tests for {@link HTTPDataPointDecoder}
 * 
 * @author ambud
 */
public class TestHttpDataPointDecoder {

	@Test
	public void testIdenticalWriteSinglePoint() {
		String testPoints = "cpu,host=server01,region=uswest value=1 1434055562000000000";
		List<DataPoint> dps = HTTPDataPointDecoder.dataPointsFromString("test", testPoints);
		DataPoint dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertTrue(dp.getTags().contains("host=server01"));
		assertTrue(dp.getTags().contains("region=uswest"));
		assertEquals(1, dp.getLongValue());
		assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());
	}

	@Test
	public void testIdenticalWriteMultipoints() {
		String testPoints = "cpu,host=server01,region=uswest value=1 1434055562000000000\ncpu,host=server01,region=uswest value=1 1434055562000000000\ncpu,host=server01,region=uswest value=1 1434055562000000000";
		List<DataPoint> dps = HTTPDataPointDecoder.dataPointsFromString("test", testPoints);
		for (DataPoint dp : dps) {
			assertEquals("cpu", dp.getMeasurementName());
			assertTrue(dp.getTags().contains("host=server01"));
			assertTrue(dp.getTags().contains("region=uswest"));
			assertEquals(1, dp.getLongValue());
			assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());
		}
	}

	@Test
	public void testDoublePointMeasurementValue() {
		String testPoints = "cpu value=1,value1=2 1434055562000000000";
		List<DataPoint> dps = HTTPDataPointDecoder.dataPointsFromString("test", testPoints);

		assertEquals(2, dps.size());

		DataPoint dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTags().size());
		assertEquals(1, dp.getLongValue());
		assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());

		dp = dps.get(1);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTags().size());
		assertEquals(2, dp.getLongValue());
		assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());
	}

	@Test
	public void testSinglePointMeasurementValue() {
		String testPoints = "cpu value=1 1434055562000000000";
		List<DataPoint> dps = HTTPDataPointDecoder.dataPointsFromString("test", testPoints);
		assertEquals(1, dps.size());
		DataPoint dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTags().size());
		assertEquals(1, dp.getLongValue());
		assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());
	}

	@Test
	public void testSinglePointMeasurementValueWithoutTimestamp() {
		String testPoints = "cpu value=1";
		List<DataPoint> dps = HTTPDataPointDecoder.dataPointsFromString("test", testPoints);
		assertEquals(1, dps.size());
		DataPoint dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTags().size());
		assertEquals(1, dp.getLongValue());
		long ts = System.currentTimeMillis();
		assertTrue(dp.getTimestamp() > 0);
		assertEquals(1, (dp.getTimestamp() / ts), 1);
	}

	@Test
	public void testSinglePointWithIMeasurementValueWithoutTimestamp() {
		String testPoints = "cpu value=1i\ncpu\ncpu mem test hello";
		List<DataPoint> dps = HTTPDataPointDecoder.dataPointsFromString("test", testPoints);
		assertEquals(1, dps.size());
		DataPoint dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTags().size());
		assertEquals(1, dp.getLongValue());
		long ts = System.currentTimeMillis();
		assertEquals(1, (dp.getTimestamp() / ts), 1);
	}

	@Test
	public void testFloatMeasurementValueWithoutTimestamp() {
		String testPoints = "cpu value=1.2";
		List<DataPoint> dps = HTTPDataPointDecoder.dataPointsFromString("test", testPoints);
		assertEquals(1, dps.size());
		DataPoint dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTags().size());
		assertEquals(1.2, dp.getValue(), 0.01);
		long ts = System.currentTimeMillis();
		assertEquals(1, (dp.getTimestamp() / ts), 1);
	}

}
