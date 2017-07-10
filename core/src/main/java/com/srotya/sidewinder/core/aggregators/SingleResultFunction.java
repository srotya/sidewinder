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

import java.util.Arrays;
import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public abstract class SingleResultFunction implements AggregationFunction {
	
	@Override
	public void init(Object[] args) {
	}

	@Override
	public List<DataPoint> aggregate(List<DataPoint> dataPoints) {
		if (dataPoints.size() > 0) {
			DataPoint dp = new DataPoint();
			dp.setTimestamp(0);
			dp.setValue(0);
			dp.setTags(dataPoints.get(0).getTags());
			dp.setFp(dataPoints.get(0).isFp());
			dp.setMeasurementName(dataPoints.get(0).getMeasurementName());
			dp.setValueFieldName(dataPoints.get(0).getValueFieldName());
			dp.setDbName(dataPoints.get(0).getDbName());
			aggregateToSingle(dataPoints, dp);
			return Arrays.asList(dp);
		} else {
			return Arrays.asList();
		}
	}

	protected abstract void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output);
	
	@Override
	public int getNumberOfArgs() {
		return 0;
	}

}
