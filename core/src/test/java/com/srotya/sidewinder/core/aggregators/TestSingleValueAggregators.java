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

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public class TestSingleValueAggregators {
	
	@Test
	public void testSumAggregator() {
		double[] values = { 2.2, 1.1, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (double d : values) {
			dps.add(new DataPoint(ts, d));
		}
		SingleValueAggregator sva = new SumValueAggregator();
		List<DataPoint> result = sva.aggregate(dps);
		assertEquals(1, result.size());
		assertEquals(11, result.get(0).getValue(), 0.01);
		
		dps.clear();
		long[] vals = { 1, 2, 3, 4, 5 };
		for (long l : vals) {
			dps.add(new DataPoint(ts, l));
		}
		result = sva.aggregate(dps);
		assertEquals(1, result.size());
		assertEquals(15, result.get(0).getLongValue(), 0.01);
	}

	@Test
	public void testMeanAggregator() {
		double[] values = { 2.2, 1.1, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (double d : values) {
			dps.add(new DataPoint(ts, d));
		}
		SingleValueAggregator sva = new MeanValueAggregator();
		List<DataPoint> result = sva.aggregate(dps);
		assertEquals(1, result.size());
		assertEquals(2.75, result.get(0).getValue(), 0.01);

		dps.clear();
		long[] vals = { 1, 2, 3, 4, 5 };
		for (long l : vals) {
			dps.add(new DataPoint(ts, l));
		}
		result = sva.aggregate(dps);
		assertEquals(1, result.size());
		assertEquals(3, result.get(0).getLongValue(), 0.01);
	}

}
