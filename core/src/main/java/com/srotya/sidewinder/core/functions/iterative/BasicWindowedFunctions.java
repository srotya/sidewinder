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
import com.srotya.sidewinder.core.storage.DataPointIterator;

public class BasicWindowedFunctions {

	@FunctionName(alias = "rms", description = "Returns the RMS value for each window of time", type = "windowed aggregate")
	public static class RMSFunction extends TumblingWindowFunction {

		public RMSFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public void init(Object[] args) throws Exception {
			if (args.length > 1) {
				args[1] = "srms";
			} else {
				args = new Object[] { args[0], "srms" };
			}
			super.init(args);
		}
	}

	@FunctionName(alias = "integral", description = "Returns the integral (sum) value for each window of time", type = "windowed aggregate")
	public static class IntegralFunction extends TumblingWindowFunction {

		public IntegralFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public void init(Object[] args) throws Exception {
			if (args.length > 1) {
				args[1] = "ssum";
			} else {
				args = new Object[] { args[0], "ssum" };
			}
			super.init(args);
		}

	}

	@FunctionName(alias = "first", description = "Returns the first value for each window of time", type = "windowed aggregate")
	public static class WindowedFirst extends TumblingWindowFunction {

		public WindowedFirst(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public void init(Object[] args) throws Exception {
			if (args.length > 1) {
				args[1] = "sfirst";
			} else {
				args = new Object[] { args[0], "sfirst" };
			}
			super.init(args);
		}

	}

	@FunctionName(alias = "last", description = "Returns the last value for each window of time", type = "windowed aggregate")
	public static class WindowedLast extends TumblingWindowFunction {

		public WindowedLast(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public void init(Object[] args) throws Exception {
			if (args.length > 1) {
				args[1] = "slast";
			} else {
				args = new Object[] { args[0], "slast" };
			}
			super.init(args);
		}

	}

	@FunctionName(alias = "max", description = "Returns the max value for each window of time", type = "windowed aggregate")
	public static class WindowedMax extends TumblingWindowFunction {

		public WindowedMax(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public void init(Object[] args) throws Exception {
			if (args.length > 1) {
				args[1] = "smax";
			} else {
				args = new Object[] { args[0], "smax" };
			}
			super.init(args);
		}

	}

	@FunctionName(alias = { "mean", "average",
			"avg" }, description = "Returns the average value for each window of time", type = "windowed aggregate")
	public static class WindowedMean extends TumblingWindowFunction {

		public WindowedMean(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public void init(Object[] args) throws Exception {
			if (args.length > 1) {
				args[1] = "smean";
			} else {
				args = new Object[] { args[0], "smean" };
			}
			super.init(args);
		}

	}

	@FunctionName(alias = "min", description = "Returns the minimum value for each window of time", type = "windowed aggregate")
	public static class WindowedMin extends TumblingWindowFunction {

		public WindowedMin(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public void init(Object[] args) throws Exception {
			if (args.length > 1) {
				args[1] = "smin";
			} else {
				args = new Object[] { args[0], "smin" };
			}
			super.init(args);
		}

	}

	@FunctionName(alias = "stddev", description = "Returns the standard deviation value for each window of time", type = "windowed aggregate")
	public static class WindowedStdDev extends TumblingWindowFunction {

		public WindowedStdDev(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public void init(Object[] args) throws Exception {
			if (args.length > 1) {
				args[1] = "sstddev";
			} else {
				args = new Object[] { args[0], "sstddev" };
			}
			super.init(args);
		}

	}

}