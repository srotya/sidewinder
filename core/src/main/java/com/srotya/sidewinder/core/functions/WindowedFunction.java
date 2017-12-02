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
package com.srotya.sidewinder.core.functions;

import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Series;

/**
 * @author ambud
 */
public abstract class WindowedFunction extends SingleSeriesFunction {

	private int timeWindow;

	public WindowedFunction() {
	}

	public void init(Object[] args) throws Exception {
		timeWindow = ((Integer) args[0]);
		if (timeWindow <= 0) {
			timeWindow = 1;
		}
		timeWindow = timeWindow * 1000;
	}
	
	@Override
	public Series apply(Series t) {
		Series output = new Series(t.getMeasurementName(), t.getValueFieldName(), t.getTags());
		output.setFp(t.isFp());
		output.setDataPoints(apply(t.getDataPoints(), t.isFp()));
		return output;
	}
	
	public abstract List<DataPoint> apply(List<DataPoint> datapoints, boolean isFp);

	/**
	 * @return time window for this aggregation
	 */
	public int getTimeWindow() {
		return timeWindow;
	}
	
	@Override
	public int getNumberOfArgs() {
		return 1;
	}
}
