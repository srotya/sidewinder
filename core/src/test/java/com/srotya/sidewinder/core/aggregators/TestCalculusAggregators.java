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
import com.srotya.sidewinder.core.aggregators.windowed.ReducingWindowedAggregator;
import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public class TestCalculusAggregators {

	@Test
	public void testDerivativeAggregator() throws Exception {
		double[] values = { 1.1, 2.2, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = 1486617103629L;
		for (int i = 0; i < values.length; i++) {
			double d = values[i];
			ts = ts + (30_000);
			dps.add(new DataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new DerivativeFunction();
		rwa.init(new Object[] { 70, "smean" });
		List<DataPoint> result = rwa.aggregate(dps);
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
			dps.add(new DataPoint(ts, d));
		}
		ReducingWindowedAggregator rwa = new DerivativeFunction();
		rwa.init(new Object[] { 20, "smean" });
		List<DataPoint> result = rwa.aggregate(dps);
		assertEquals(1, result.size());
		assertEquals(false, result.get(0).isFp());
		assertEquals(0, result.get(0).getValue() * 1000, 0.01);
		System.out.println(result.get(0).getValue() * 1000 + "\t" + ts);
	}

}
