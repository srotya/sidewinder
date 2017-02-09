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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public abstract class ReducingWindowedAggregator extends WindowedAggregator {

	private SingleValueAggregator aggregator;

	public ReducingWindowedAggregator(int timeWindow, SingleValueAggregator aggregator) {
		super(timeWindow);
		this.aggregator = aggregator;
	}

	@Override
	public final List<DataPoint> aggregate(List<DataPoint> datapoints) {
		SortedMap<Long, List<DataPoint>> map = new TreeMap<>();
		for (DataPoint dataPoint : datapoints) {
			long bucket = (dataPoint.getTimestamp() / getTimeWindow()) * getTimeWindow();
			List<DataPoint> list = map.get(bucket);
			if (list == null) {
				list = new ArrayList<>();
				map.put(bucket, list);
			}
			list.add(dataPoint);
		}
		List<DataPoint> reducedDataPoints = new ArrayList<>();
		for (Entry<Long, List<DataPoint>> entry : map.entrySet()) {
			List<DataPoint> aggregate = aggregator.aggregate(entry.getValue());
			if (!aggregate.isEmpty()) {
				DataPoint dp = aggregate.get(0);
				dp.setTimestamp(entry.getKey());
				reducedDataPoints.add(dp);
			}
		}
		return aggregateAfterReduction(reducedDataPoints);
	}

	public abstract List<DataPoint> aggregateAfterReduction(List<DataPoint> datapoints);
}
