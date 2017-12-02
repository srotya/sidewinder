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
@FunctionName(alias = "smin")
public class MinFunction extends ReduceFunction {

	@Override
	public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
		if(dataPoints.isEmpty()) {
			return;
		}
		if (isFp) {
			double min = dataPoints.get(0).getValue();
			for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
				DataPoint dataPoint = iterator.next();
				if (dataPoint.getValue() < min) {
					min = dataPoint.getValue();
				}
			}
			output.setValue(min);
		} else {
			long min = dataPoints.get(0).getLongValue();
			for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
				DataPoint dataPoint = iterator.next();
				if (dataPoint.getLongValue() < min) {
					min = dataPoint.getLongValue();
				}
			}
			output.setLongValue(min);
		}
	}

}
