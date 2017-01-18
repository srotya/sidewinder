/**
 * Copyright 2016 Ambud Sharma
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
package com.srotya.sidewinder.core.analytics;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public class CorrelationEngine {

	/**
	 * @param series
	 */
	public void correlate(List<DataPoint> base, Map<String, List<DataPoint>> series) {
		checkAndCorrectLength(base, series);
	}

	public static void checkAndCorrectLength(List<DataPoint> base, Map<String, List<DataPoint>> series) {
		for (Entry<String, List<DataPoint>> entry : series.entrySet()) {
			if (entry.getValue().size() != base.size()) {
				// fix this series
			}
		}
	}

	public static void alignTimeSeries(List<DataPoint> base, List<DataPoint> candidate) {
		int i = 0;
		int j = 0;
		while (i < base.size()) {
			if ((base.get(i).getTimestamp() - candidate.get(j).getTimestamp()) > 1000) {
				candidate.add(j, new DataPoint(base.get(i).getTimestamp(), 0));
				j++;
			} else {
				i++;
			}
		}
	}

}
