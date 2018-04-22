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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.SeriesOutput;

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
	public SeriesOutput apply(SeriesOutput t) {
		SeriesOutput output = new SeriesOutput(t.getMeasurementName(), t.getValueFieldName(), t.getTags());
		output.setFp(t.isFp());
		SortedMap<Long, List<DataPoint>> map = new TreeMap<>();
		for (DataPoint dataPoint : t.getDataPoints()) {
			try {
				long bucket = (dataPoint.getTimestamp() / getTimeWindow()) * getTimeWindow();
				List<DataPoint> list = map.get(bucket);
				if (list == null) {
					list = new ArrayList<>();
					map.put(bucket, list);
				}
				list.add(dataPoint);
			} catch (Exception e) {
				System.err.println("Exception :" + getTimeWindow());
			}
		}
		output.setDataPoints(apply(map, t.isFp()));
		return output;
	}
	
	public abstract List<DataPoint> apply(SortedMap<Long, List<DataPoint>> map, boolean isFp);

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
