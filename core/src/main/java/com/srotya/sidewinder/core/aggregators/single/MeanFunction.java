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
import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
@FunctionName(alias = "smean")
public class MeanFunction extends SumFunction {

	@Override
	protected void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output) {
		super.aggregateToSingle(dataPoints, output);
		if (output.isFp()) {
			output.setValue(output.getValue() / dataPoints.size());
		} else {
			output.setLongValue(output.getLongValue() / dataPoints.size());
		}
	}

	@Override
	public void aggregateToSinglePoint(List<long[]> dataPoints, long[] output, boolean isFp) {
		super.aggregateToSinglePoint(dataPoints, output, isFp);
		if (isFp) {
			output[1] = Double.doubleToLongBits(Double.longBitsToDouble(output[1]) / dataPoints.size());
		} else {
			output[1] = (output[1] / dataPoints.size());
		}
	}

}
