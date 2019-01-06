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
package com.srotya.sidewinder.core.functions.list;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.srotya.sidewinder.core.functions.list.ReduceFunction;
import com.srotya.sidewinder.core.functions.list.BasicSingleFunctions.*;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.SeriesOutput;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class TestSingleValueAggregators {

	@Test
	public void testFirstFunctionLong() {
		FirstFunction f = new FirstFunction();
		long[] values = { 2, 1, 3, 4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (long d : values) {
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		SeriesOutput series = new SeriesOutput();
		series.setDataPoints(dps);
		SeriesOutput result = f.apply(series);
		assertEquals(2, result.getDataPoints().get(0).getLongValue());

	}

	@Test
	public void testLastFunctionLong() {
		LastFunction f = new LastFunction();
		long[] values = { 2, 1, 3, 4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (long d : values) {
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		SeriesOutput series = new SeriesOutput();
		series.setDataPoints(dps);
		SeriesOutput result = f.apply(series);
		assertEquals(4, result.getDataPoints().get(0).getLongValue());
	}

	@Test
	public void testMinFunctionLong() {
		MinFunction f = new MinFunction();
		long[] values = { 2, 1, 3, 4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (long d : values) {
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		SeriesOutput series = new SeriesOutput();
		series.setDataPoints(dps);
		SeriesOutput result = f.apply(series);
		assertEquals(1, result.getDataPoints().get(0).getLongValue());
	}

	@Test
	public void testFirstFunction() {
		FirstFunction f = new FirstFunction();
		double[] values = { 2.2, 1.1, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (double d : values) {
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		SeriesOutput series = new SeriesOutput();
		series.setDataPoints(dps);
		SeriesOutput result = f.apply(series);
		assertEquals(2.2, result.getDataPoints().get(0).getValue(), 0);

		// List<long[]> data = new ArrayList<>();
		// for(double d:values) {
		// data.add(new long[] {ts, Double.doubleToLongBits(d)});
		// }
		// List<long[]> aggregatePoints = f.aggregatePoints(data, true);
		// assertEquals(1, aggregatePoints.size());
		// assertEquals(2.2, Double.longBitsToDouble(aggregatePoints.get(0)[1]), 0);
	}

	@Test
	public void testLastFunction() {
		LastFunction f = new LastFunction();
		double[] values = { 2.2, 1.1, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (double d : values) {
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		SeriesOutput series = new SeriesOutput();
		series.setDataPoints(dps);
		SeriesOutput result = f.apply(series);
		assertEquals(4.4, result.getDataPoints().get(0).getValue(), 0);
	}

	@Test
	public void testMaxFunction() {
		MaxFunction f = new MaxFunction();
		double[] values = { 2.2, 1.1, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (double d : values) {
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		SeriesOutput series = new SeriesOutput();
		series.setDataPoints(dps);
		series.setFp(true);
		SeriesOutput result = f.apply(series);
		assertEquals(4.4, result.getDataPoints().get(0).getValue(), 0);
	}

	@Test
	public void testMinFunction() {
		MinFunction f = new MinFunction();
		double[] values = { 2.2, 1.1, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (double d : values) {
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		SeriesOutput series = new SeriesOutput();
		series.setDataPoints(dps);
		SeriesOutput result = f.apply(series);

		assertEquals(1.1, result.getDataPoints().get(0).getValue(), 0);
	}

	@Test
	public void testSumAggregator() {
		double[] values = { 2.2, 1.1, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (double d : values) {
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReduceFunction sva = new SumFunction();
		SeriesOutput series = new SeriesOutput();
		series.setFp(true);
		series.setDataPoints(dps);
		SeriesOutput result = sva.apply(series);
		assertEquals(11, result.getDataPoints().get(0).getValue(), 0.01);

		dps.clear();
		long[] vals = { 1, 2, 3, 4, 5 };
		for (long l : vals) {
			dps.add(MiscUtils.buildDataPoint(ts, l));
		}
		series.setFp(false);
		result = sva.apply(series);

		assertEquals(15, result.getDataPoints().get(0).getLongValue(), 0.01);
	}

	@Test
	public void testMeanAggregator() {
		double[] values = { 2.2, 1.1, 3.3, 4.4 };
		List<DataPoint> dps = new ArrayList<>();
		long ts = System.currentTimeMillis();
		for (double d : values) {
			dps.add(MiscUtils.buildDataPoint(ts, d));
		}
		ReduceFunction sva = new MeanFunction();
		SeriesOutput series = new SeriesOutput();
		series.setFp(true);
		series.setDataPoints(dps);
		SeriesOutput result = sva.apply(series);
		assertEquals(2.75, result.getDataPoints().get(0).getValue(), 0.01);

		dps.clear();
		long[] vals = { 1, 2, 3, 4, 5 };
		for (long l : vals) {
			dps.add(MiscUtils.buildDataPoint(ts, l));
		}
		series.setFp(false);
		for (int p = 0; p < 1000; p++) {
			result = sva.apply(series);
		}
		assertEquals(3, result.getDataPoints().get(0).getLongValue(), 0.01);
	}

}
