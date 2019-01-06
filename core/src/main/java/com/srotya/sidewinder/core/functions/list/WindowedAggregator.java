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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public abstract class WindowedAggregator extends WindowedFunction {

	private ReduceFunction aggregator;

	public WindowedAggregator() {
	}

	@Override
	public void init(Object[] args) throws Exception {
		super.init(args);
		String aggregatorName = "smean";
		if (args.length > 1) {
			aggregatorName = args[1].toString();
		}
		@SuppressWarnings("unchecked")
		Class<ReduceFunction> lookupFunction = (Class<ReduceFunction>) FunctionTable.get()
				.lookupFunction(aggregatorName);
		if (lookupFunction == null) {
			throw new IllegalArgumentException("Invalid aggregation function:" + aggregatorName);
		}
		try {
			ReduceFunction tmp = lookupFunction.newInstance();
			this.aggregator = (ReduceFunction) tmp;
		} catch (ClassCastException e) {
			throw new IllegalArgumentException("Invalid aggregation function:" + aggregatorName);
		}
	}
	
	@Override
	public final List<DataPoint> apply(SortedMap<Long, List<DataPoint>> map, boolean isFp) {
		List<DataPoint> reducedDataPoints = new ArrayList<>();
		for (Entry<Long, List<DataPoint>> entry : map.entrySet()) {
			DataPoint aggregate = new DataPoint();
			aggregator.aggregateToSingle(entry.getValue(), aggregate, isFp);
			aggregate.setTimestamp(entry.getKey());
			reducedDataPoints.add(aggregate);
		}
		if (reducedDataPoints.size() > 0) {
			return aggregateAfterReduction(reducedDataPoints, isFp);
		} else {
			return reducedDataPoints;
		}
	}

	public List<DataPoint> aggregateAfterReduction(List<DataPoint> dataPoints, boolean isFp) {
		return dataPoints;
	}

	@Override
	public int getNumberOfArgs() {
		return super.getNumberOfArgs() + 1;
	}
}
