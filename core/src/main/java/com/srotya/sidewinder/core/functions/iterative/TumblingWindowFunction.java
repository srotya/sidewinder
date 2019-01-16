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

import com.srotya.sidewinder.core.functions.list.FunctionName;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.DataPointIterator;

@FunctionName(alias = { "window", "downsample" }, description = "Runs tumbling window reduce", type = "window")
public class TumblingWindowFunction extends FunctionIterator {

	private int timeWindowInSeconds;
	private Class<? extends ReduceFunction> lookupFunction;

	public TumblingWindowFunction(DataPointIterator iterator, boolean isFp) {
		super(iterator, isFp);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(Object[] args) throws Exception {
		timeWindowInSeconds = ((Integer) args[0]);
		if (timeWindowInSeconds <= 0) {
			timeWindowInSeconds = 1;
		}
		timeWindowInSeconds = timeWindowInSeconds * 1000;
		String aggregatorName = "smean";
		if (args.length >= 2) {
			aggregatorName = (String) args[1];
		}
		lookupFunction = (Class<? extends ReduceFunction>) FunctionIteratorTable.get().lookupFunction(aggregatorName);
		if (lookupFunction == null) {
			throw new IllegalArgumentException("Invalid aggregation function:" + aggregatorName);
		}
		try {
			lookupFunction.getConstructor(DataPointIterator.class, Boolean.TYPE);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid aggregation function:" + aggregatorName, e);
		}
	}

	@Override
	public int getNumberOfArgs() {
		return 2;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public DataPoint next() {
		try {
			ReduceFunction rf = lookupFunction.getConstructor(DataPointIterator.class, Boolean.TYPE)
					.newInstance(new TumblingWindowLimitIterator(iterator, timeWindowInSeconds), isFp);
			return rf.next();
		} catch (Exception e) {
			throw new RuntimeException(e);
			// MUST not error since we already checked during function init
		}
	}

	public static class TumblingWindowLimitIterator extends DataPointIterator {

		private DataPointIterator iterator;
		private int timeWindowInSeconds;
		private DataPoint dp;
		private long tsWindow = -1;

		public TumblingWindowLimitIterator(DataPointIterator iterator, int timeWindowInSeconds) {
			this.timeWindowInSeconds = timeWindowInSeconds;
			this.iterator = iterator;
			this.dp = iterator.next();
		}

		@Override
		public boolean hasNext() {
			if (iterator.hasNext()) {
				long ts = getWindowTs((dp = iterator.next()));
				if (tsWindow == -1 || tsWindow == ts) {
					tsWindow = ts;
					return true;
				} else {
					iterator.prev();
					return false;
				}
			} else {
				return false;
			}
		}

		@Override
		public DataPoint next() {
			return dp;
		}

		private long getWindowTs(DataPoint dp) {
			return (dp.getTimestamp() / timeWindowInSeconds) * timeWindowInSeconds;
		}
	}

}