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

public abstract class TransformFunction implements Function {

	@Override
	public List<Series> apply(List<Series> series) {
		for (Series s : series) {
			List<DataPoint> dps = s.getDataPoints();
			for (DataPoint dp : dps) {
				if (s.isFp()) {
					dp.setValue(transform(dp.getValue()));
				} else {
					dp.setLongValue(transform(dp.getLongValue()));
				}
			}
		}
		return series;
	}

	public abstract long transform(long value);

	public abstract double transform(double value);

	@Override
	public void init(Object[] args) throws Exception {
	}

	@Override
	public int getNumberOfArgs() {
		return 0;
	}

}
