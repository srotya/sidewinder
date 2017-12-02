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
package com.srotya.sidewinder.core.functions.single;

import java.util.Iterator;
import java.util.List;

import com.srotya.sidewinder.core.functions.FunctionName;
import com.srotya.sidewinder.core.functions.ReduceFunction;
import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
@FunctionName(alias = "ssum")
public class SumFunction extends ReduceFunction {

	@Override
	public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
		output.setTimestamp(dataPoints.get(0).getTimestamp());
		if (!isFp) {
			long sum = 0;
			for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
				DataPoint dataPoint = iterator.next();
				sum += dataPoint.getLongValue();
			}
			output.setLongValue(sum);
		} else {
			double sum = 0;
			for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
				DataPoint dataPoint = iterator.next();
				sum += dataPoint.getValue();
			}
			output.setValue(sum);
		}
	}

}
