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

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.utils.InfluxDecoder;

/**
 * Unit tests for {@link InfluxDecoder}
 * 
 * @author ambud
 */
public class TestInfluxDecoder {

	@Test
	public void testIdenticalWriteSinglePoint() {
		String testPoints = "cpu,host=server01,region=uswest value=1i 1434055562000000000";
		List<Point> dps = InfluxDecoder.pointsFromString("test", testPoints);
		Point dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertTrue(dp.getTagsList().contains("host=server01"));
		assertTrue(dp.getTagsList().contains("region=uswest"));
		assertEquals(1, dp.getValue());
		assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());
	}

	@Test
	public void testIdenticalWriteMultipoints() {
		String testPoints = "cpu,host=server01,region=uswest value=1i 1434055562000000000\ncpu,host=server01,region=uswest value=1i 1434055562000000000\ncpu,host=server01,region=uswest value=1i 1434055562000000000";
		List<Point> dps = InfluxDecoder.pointsFromString("test", testPoints);
		for (Point dp : dps) {
			assertEquals("cpu", dp.getMeasurementName());
			assertTrue(dp.getTagsList().contains("host=server01"));
			assertTrue(dp.getTagsList().contains("region=uswest"));
			assertEquals(1, dp.getValue());
			assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());
		}
	}

	@Test
	public void testDoublePointMeasurementValue() {
		String testPoints = "cpu value=1i,value1=2i 1434055562000000000";
		List<Point> dps = InfluxDecoder.pointsFromString("test", testPoints);

		assertEquals(2, dps.size());

		Point dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTagsList().size());
		assertEquals(1, dp.getValue());
		assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());

		dp = dps.get(1);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTagsList().size());
		assertEquals(2, dp.getValue());
		assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());
	}

	@Test
	public void testSinglePointMeasurementValue() {
		String testPoints = "cpu value=1i 1434055562000000000";
		List<Point> dps = InfluxDecoder.pointsFromString("test", testPoints);
		assertEquals(1, dps.size());
		Point dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTagsList().size());
		assertEquals(1, dp.getValue());
		assertEquals(1434055562000000000L / (1000 * 1000), dp.getTimestamp());
	}

	@Test
	public void testSinglePointMeasurementValueWithoutTimestamp() {
		String testPoints = "cpu value=1i";
		List<Point> dps = InfluxDecoder.pointsFromString("test", testPoints);
		assertEquals(1, dps.size());
		Point dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTagsList().size());
		assertEquals(1, dp.getValue());
		long ts = System.currentTimeMillis();
		assertTrue(dp.getTimestamp() > 0);
		assertEquals(1, (dp.getTimestamp() / ts), 1);
	}

	@Test
	public void testSinglePointWithIMeasurementValueWithoutTimestamp() {
		String testPoints = "cpu value=1i\ncpu\ncpu mem test hello";
		List<Point> dps = InfluxDecoder.pointsFromString("test", testPoints);
		assertEquals(1, dps.size());
		Point dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTagsList().size());
		assertEquals(1, dp.getValue());
		long ts = System.currentTimeMillis();
		assertEquals(1, (dp.getTimestamp() / ts), 1);
	}

	@Test
	public void testFloatMeasurementValueWithoutTimestamp() {
		String testPoints = "cpu value=1.2";
		List<Point> dps = InfluxDecoder.pointsFromString("test", testPoints);
		assertEquals(1, dps.size());
		Point dp = dps.get(0);
		assertEquals("cpu", dp.getMeasurementName());
		assertEquals(0, dp.getTagsList().size());
		assertEquals(1.2, Double.longBitsToDouble(dp.getValue()), 0.01);
		long ts = System.currentTimeMillis();
		assertEquals(1, (dp.getTimestamp() / ts), 1);
	}

	@Test
	public void testPerf() {
		String testPoints = "cpu,host=server01,region=uswest value=1i 1434055562000000000\ncpu,host=server01,region=uswest value=1i 1434055562000000000\ncpu,host=server01,region=uswest value=1i 1434055562000000000\ncpu,host=server01,region=uswest value=1i 1434055562000000000\ncpu,host=server01,region=uswest value=1i 1434055562000000000";
		for (int i = 0; i < 100000; i++) {
			InfluxDecoder.pointsFromString("test", testPoints);
		}
	}
}
