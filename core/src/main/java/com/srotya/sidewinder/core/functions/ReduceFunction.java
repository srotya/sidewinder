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
package com.srotya.sidewinder.core.functions;

import java.util.Arrays;
import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.SeriesOutput;

public abstract class ReduceFunction extends SingleSeriesFunction {

	@Override
	public void init(Object[] args) throws Exception {
	}

	@Override
	public SeriesOutput apply(SeriesOutput dataPoints) {
		SeriesOutput output = new SeriesOutput(dataPoints.getMeasurementName(), dataPoints.getValueFieldName(),
				dataPoints.getTags());
		output.setFp(dataPoints.isFp());
		DataPoint single = new DataPoint();
		single.setTimestamp(dataPoints.getDataPoints().get(0).getTimestamp());
		aggregateToSingle(dataPoints.getDataPoints(), single, dataPoints.isFp());
		output.setDataPoints(Arrays.asList(single));
		return output;
	}

	public abstract void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp);

}
