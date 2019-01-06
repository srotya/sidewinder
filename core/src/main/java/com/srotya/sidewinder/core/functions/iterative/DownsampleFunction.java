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
package com.srotya.sidewinder.core.functions.iterative;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleBinaryOperator;
import java.util.function.LongBinaryOperator;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.DataPointIterator;
import com.srotya.sidewinder.core.utils.TimeUtils;

public class DownsampleFunction implements Iterator<DataPoint> {

	private DataPointIterator iterator;
	private int timeWindow;
	private LongBinaryOperator longOperator;
	private DoubleBinaryOperator doubleOperator;

	public DownsampleFunction(DataPointIterator iterator, int timeWindow, TimeUnit unit, LongBinaryOperator operator) {
		this.iterator = iterator;
		this.longOperator = operator;
		this.timeWindow = TimeUtils.timeToMilliSeconds(unit, timeWindow);
	}

	public DownsampleFunction(DataPointIterator iterator, int timeWindow, TimeUnit unit, DoubleBinaryOperator operator,
			Void dummy) {
		this.iterator = iterator;
		this.doubleOperator = operator;
		this.timeWindow = TimeUtils.timeToMilliSeconds(unit, timeWindow);
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public DataPoint next() {
		DataPoint dp = iterator.next();
		long ts = dp.getTimestamp();
		long longValue = dp.getLongValue();
		double doubleValue = Double.longBitsToDouble(dp.getLongValue());
		long maxTs = ts + timeWindow;
		int c = 0;
		while (iterator.hasNext() && (dp = iterator.next()).getTimestamp() < maxTs) {
			if (longOperator != null) {
				longValue = longOperator.applyAsLong(longValue, dp.getLongValue());
			} else {
				doubleValue = doubleOperator.applyAsDouble(doubleValue, Double.longBitsToDouble(dp.getLongValue()));
			}
			c++;
		}
		if (c > 0) {
			iterator.prev();
		}
		return new DataPoint(ts, longValue);
	}

}