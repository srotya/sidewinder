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
package com.srotya.sidewinder.core.aggregators;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.srotya.sidewinder.core.aggregators.windowed.DerivativeFunction;
import com.srotya.sidewinder.core.aggregators.windowed.IntegralFunction;
import com.srotya.sidewinder.core.aggregators.windowed.ReducingWindowedAggregator;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedFirst;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedLast;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedMax;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedMean;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedMin;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class TestWindowedFunctions {
	
	@Test
	public void testIntegralFunction() throws Exception {
		double[] values = { 1.1, 2.2, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = 1486617103629L;
		for (int i = 0; i < values.length; i++) {
			double d = values[i];
			ts = ts + (30_000);
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new IntegralFunction();
		rwa.init(new Object[] { 70, "smean" });
		List<DataPoint> result = rwa.aggregateDataPoints(dps);
		assertEquals(2, result.size());
		assertEquals(1.1, result.get(0).getValue(), 0);
		assertEquals(9.9, result.get(1).getValue(), 0);
	}
	
	@Test
	public void testWindowedMax() throws Exception {
		double[] values = { 1.1, 2.2, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = 1486617103629L;
		for (int i = 0; i < values.length; i++) {
			double d = values[i];
			ts = ts + (30_000);
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new WindowedMax();
		rwa.init(new Object[] { 70, "smean" });
		List<DataPoint> result = rwa.aggregateDataPoints(dps);
		assertEquals(2, result.size());
		assertEquals(1.1, result.get(0).getValue(), 0);
		assertEquals(4.4, result.get(1).getValue(), 0);
	}
	
	@Test
	public void testWindowedMin() throws Exception {
		double[] values = { 1.1, 2.2, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = 1486617103629L;
		for (int i = 0; i < values.length; i++) {
			double d = values[i];
			ts = ts + (30_000);
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new WindowedMin();
		rwa.init(new Object[] { 70, "smean" });
		List<DataPoint> result = rwa.aggregateDataPoints(dps);
		assertEquals(2, result.size());
		assertEquals(1.1, result.get(0).getValue(), 0);
		assertEquals(2.2, result.get(1).getValue(), 0);
	}
	
	@Test
	public void testWindowedFirst() throws Exception {
		double[] values = { 1.1, 3.3, 2.2, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = 1486617103629L;
		for (int i = 0; i < values.length; i++) {
			double d = values[i];
			ts = ts + (30_000);
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new WindowedFirst();
		rwa.init(new Object[] { 70, "smean" });
		List<DataPoint> result = rwa.aggregateDataPoints(dps);
		assertEquals(2, result.size());
		assertEquals(1.1, result.get(0).getValue(), 0);
		assertEquals(3.3, result.get(1).getValue(), 0);
	}
	
	@Test
	public void testWindowedLast() throws Exception {
		double[] values = { 1.1, 2.2, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = 1486617103629L;
		for (int i = 0; i < values.length; i++) {
			double d = values[i];
			ts = ts + (30_000);
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new WindowedLast();
		rwa.init(new Object[] { 70, "smean" });
		List<DataPoint> result = rwa.aggregateDataPoints(dps);
		assertEquals(2, result.size());
		assertEquals(1.1, result.get(0).getValue(), 0);
		assertEquals(4.4, result.get(1).getValue(), 0);
	}
	
	@Test
	public void testWindowedMean() throws Exception {
		double[] values = { 1.1, 2.2, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = 1486617103629L;
		for (int i = 0; i < values.length; i++) {
			double d = values[i];
			ts = ts + (30_000);
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new WindowedMean();
		rwa.init(new Object[] { 70, "smean" });
		List<DataPoint> result = rwa.aggregateDataPoints(dps);
		assertEquals(2, result.size());
		assertEquals(1.1, result.get(0).getValue(), 0);
		assertEquals(3.3, result.get(1).getValue(), 0.001);
	}

	@Test
	public void testDerivativeAggregator() throws Exception {
		double[] values = { 1.1, 2.2, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = 1486617103629L;
		for (int i = 0; i < values.length; i++) {
			double d = values[i];
			ts = ts + (30_000);
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new DerivativeFunction();
		rwa.init(new Object[] { 70, "smean" });
		List<DataPoint> result = rwa.aggregateDataPoints(dps);
		// 1.65 and 3.85
		assertEquals(1, result.size());
		assertEquals(0.00003666666667 * 1000, result.get(0).getValue(), 0.01);
		System.out.println(result.get(0).getValue() * 1000 + "\t" + ts);
	}

	@Test
	public void testDerivativeAggregator2() throws Exception {
		long[] values = { 1, 4, 1, 4, 1 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = 1486617103629L;
		for (int i = 0; i < values.length; i++) {
			long d = values[i];
			ts = ts + (10_000);
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new DerivativeFunction();
		rwa.init(new Object[] { 20, "smean" });
		List<DataPoint> result = rwa.aggregateDataPoints(dps);
		assertEquals(1, result.size());
		assertEquals(false, result.get(0).isFp());
		assertEquals(0, result.get(0).getValue() * 1000, 0.01);
		System.out.println(result.get(0).getValue() * 1000 + "\t" + ts);
	}

}
