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
package com.srotya.sidewinder.core.aggregators.windowed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.srotya.sidewinder.core.aggregators.AggregationFunction;
import com.srotya.sidewinder.core.aggregators.FunctionTable;
import com.srotya.sidewinder.core.aggregators.SingleResultFunction;
import com.srotya.sidewinder.core.aggregators.WindowedFunction;
import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public abstract class ReducingWindowedAggregator extends WindowedFunction {

	private SingleResultFunction aggregator;

	public ReducingWindowedAggregator() {
	}

	@Override
	public void init(Object[] args) throws Exception {
		super.init(args);
		String aggregatorName = "smean";
		if (args.length > 1) {
			aggregatorName = args[1].toString();
		}
		Class<? extends AggregationFunction> lookupFunction = FunctionTable.get().lookupFunction(aggregatorName);
		if (lookupFunction == null) {
			throw new IllegalArgumentException("Invalid aggregation function:" + aggregatorName);
		}
		AggregationFunction tmp = lookupFunction.newInstance();
		if (!(tmp instanceof SingleResultFunction)) {
			throw new IllegalArgumentException("Supplied reduction function doesn't produce single result");
		}
		this.aggregator = (SingleResultFunction) tmp;
	}
	
	@Override
	public List<long[]> aggregatePoints(List<long[]> datapoints, boolean isFp) {
		SortedMap<Long, List<long[]>> map = new TreeMap<>();
		for (long[] dataPoint : datapoints) {
			try {
				long bucket = (dataPoint[0] / getTimeWindow()) * getTimeWindow();
				List<long[]> list = map.get(bucket);
				if (list == null) {
					list = new ArrayList<>();
					map.put(bucket, list);
				}
				list.add(dataPoint);
			} catch (Exception e) {
				System.err.println("Exception :" + getTimeWindow());
			}
		}
		List<long[]> reducedDataPoints = new ArrayList<>();
		for (Entry<Long, List<long[]>> entry : map.entrySet()) {
			List<long[]> aggregate = aggregator.aggregatePoints(entry.getValue(), isFp);
			if (!aggregate.isEmpty()) {
				long[] dp = aggregate.get(0);
				dp[0] = entry.getKey();
				reducedDataPoints.add(dp);
			}
		}
		if (reducedDataPoints.size() > 0) {
			return aggregatePointsAfterReduction(reducedDataPoints, isFp);
		} else {
			return reducedDataPoints;
		}
	}

	protected abstract List<long[]> aggregatePointsAfterReduction(List<long[]> datapoints, boolean isFp);

	@Override
	public final List<DataPoint> aggregateDataPoints(List<DataPoint> datapoints) {
		SortedMap<Long, List<DataPoint>> map = new TreeMap<>();
		for (DataPoint dataPoint : datapoints) {
			try {
				long bucket = (dataPoint.getTimestamp() / getTimeWindow()) * getTimeWindow();
				List<DataPoint> list = map.get(bucket);
				if (list == null) {
					list = new ArrayList<>();
					map.put(bucket, list);
				}
				list.add(dataPoint);
			} catch (Exception e) {
				System.err.println("Exception :" + getTimeWindow());
			}
		}
		List<DataPoint> reducedDataPoints = new ArrayList<>();
		for (Entry<Long, List<DataPoint>> entry : map.entrySet()) {
			List<DataPoint> aggregate = aggregator.aggregateDataPoints(entry.getValue());
			if (!aggregate.isEmpty()) {
				DataPoint dp = aggregate.get(0);
				dp.setTimestamp(entry.getKey());
				reducedDataPoints.add(dp);
			}
		}
		if (reducedDataPoints.size() > 0) {
			return aggregateAfterReduction(reducedDataPoints);
		} else {
			return reducedDataPoints;
		}
	}

	public abstract List<DataPoint> aggregateAfterReduction(List<DataPoint> datapoints);

	@Override
	public int getNumberOfArgs() {
		return super.getNumberOfArgs() + 1;
	}
}
