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
package com.srotya.sidewinder.core.aggregators.single;

import java.util.List;

import com.srotya.sidewinder.core.aggregators.FunctionName;
import com.srotya.sidewinder.core.aggregators.SingleResultFunction;
import com.srotya.sidewinder.core.analytics.MathUtils;
import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
@FunctionName(alias = "sstddev")
public class StdDeviationFunction extends SingleResultFunction {

	@Override
	protected void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output) {
		output.setTimestamp(dataPoints.get(0).getTimestamp());
		output.setFp(dataPoints.get(0).isFp());
		output.setTags(dataPoints.get(0).getTags());
		if (!dataPoints.get(0).isFp()) {
			long[] ary = new long[dataPoints.size()];
			for (int i = 0; i < dataPoints.size(); i++) {
				DataPoint dataPoint = dataPoints.get(i);
				ary[i] = dataPoint.getLongValue();
			}
			long avg = MathUtils.mean(ary);
			long standardDeviation = MathUtils.standardDeviation(ary, avg);
			output.setLongValue(standardDeviation);
		} else {
			double[] ary = new double[dataPoints.size()];
			for (int i = 0; i < dataPoints.size(); i++) {
				DataPoint dataPoint = dataPoints.get(i);
				ary[i] = dataPoint.getLongValue();
			}
			double avg = MathUtils.mean(ary);
			double standardDeviation = MathUtils.standardDeviation(ary, avg);
			output.setValue(standardDeviation);
		}
	}

}
