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

import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public class DerivativeAggregator extends ReducingWindowedAggregator {

	public DerivativeAggregator(int timeWindow, SingleValueAggregator aggregator) {
		super(timeWindow, aggregator);
	}

	@Override
	public List<DataPoint> aggregateAfterReduction(List<DataPoint> datapoints) {
		int i = 0;
		while (i < datapoints.size() - 1) {
			if (!datapoints.get(0).isFp()) {
				long val = (datapoints.get(i + 1).getLongValue() - datapoints.get(i).getLongValue()) / getTimeWindow();
				datapoints.get(i).setLongValue(val);
			} else {
				double val = (datapoints.get(i + 1).getValue() - datapoints.get(i).getValue()) / getTimeWindow();
				datapoints.get(i).setValue(val);
			}
			datapoints.remove(i + 1);
			i++;
		}
		return datapoints;
	}

}
