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

import java.util.List;

import com.srotya.sidewinder.core.functions.FunctionName;
import com.srotya.sidewinder.core.functions.ReduceFunction;
import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
@FunctionName(alias = "sfirst")
public class FirstFunction extends ReduceFunction {

	@Override
	public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
		if (isFp) {
			output.setValue(dataPoints.get(0).getValue());
		} else {
			output.setLongValue(dataPoints.get(0).getLongValue());
		}
	}

}
