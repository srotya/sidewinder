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
package com.srotya.sidewinder.core.functions.windowed;

import java.util.List;

import com.srotya.sidewinder.core.functions.FunctionName;
import com.srotya.sidewinder.core.storage.DataPoint;

public class BasicWindowedFunctions {

	@FunctionName(alias = { "derivative", "dvdt" }, description = "Takes the derivative value for each window of values", type="windowed aggregate")
	public static class DerivativeFunction extends ReducingWindowedAggregator {

		@Override
		public List<DataPoint> aggregateAfterReduction(List<DataPoint> datapoints, boolean isFp) {
			DataPoint origin = datapoints.get(datapoints.size() - 1);
			int i = 0;
			while (i < datapoints.size() - 1) {
				if (!isFp) {
					long val = (datapoints.get(i + 1).getLongValue() - datapoints.get(i).getLongValue())
							/ (getTimeWindow() / 1000);
					datapoints.get(i).setLongValue(val);
				} else {
					double val = (datapoints.get(i + 1).getValue() - datapoints.get(i).getValue())
							/ (getTimeWindow() / 1000);
					datapoints.get(i).setLongValue(Double.doubleToLongBits(val));
				}
				datapoints.remove(i + 1);
				i++;
			}
			if (datapoints.get(datapoints.size() - 1).getLongValue() == origin.getLongValue()) {
				datapoints.remove(datapoints.size() - 1);
			}
			return datapoints;
		}

	}

	@FunctionName(alias = "diff", description="Takes the difference between the next and current value", type="windowed aggregate")
	public static class DiffFunction extends ReducingWindowedAggregator {

		@Override
		public List<DataPoint> aggregateAfterReduction(List<DataPoint> datapoints, boolean isFp) {
			DataPoint origin = datapoints.get(datapoints.size() - 1);
			int i = 0;
			while (i < datapoints.size() - 1) {
				if (!isFp) {
					long val = (datapoints.get(i + 1).getLongValue() - datapoints.get(i).getLongValue());
					datapoints.get(i).setLongValue(val);
				} else {
					double val = (datapoints.get(i + 1).getValue() - datapoints.get(i).getValue());
					datapoints.get(i).setLongValue(Double.doubleToLongBits(val));
				}
				datapoints.remove(i + 1);
				i++;
			}
			if (datapoints.get(datapoints.size() - 1).getLongValue() == origin.getLongValue()) {
				datapoints.remove(datapoints.size() - 1);
			}
			return datapoints;
		}

	}

	@FunctionName(alias = "rms", description="Returns the RMS value for each window of time", type="windowed aggregate")
	public static class RMSFunction extends ReducingWindowedAggregator {

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

	@FunctionName(alias = "integral", description="Returns the integral (sum) value for each window of time", type="windowed aggregate")
	public static class IntegralFunction extends ReducingWindowedAggregator {

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

	@FunctionName(alias = "first", description="Returns the first value for each window of time", type="windowed aggregate")
	public static class WindowedFirst extends ReducingWindowedAggregator {

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

	@FunctionName(alias = "last", description="Returns the last value for each window of time", type="windowed aggregate")
	public static class WindowedLast extends ReducingWindowedAggregator {

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

	@FunctionName(alias = "max", description="Returns the max value for each window of time", type="windowed aggregate")
	public static class WindowedMax extends ReducingWindowedAggregator {

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

	@FunctionName(alias = { "mean", "average" }, description="Returns the average value for each window of time", type="windowed aggregate")
	public static class WindowedMean extends ReducingWindowedAggregator {

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

	@FunctionName(alias = "min", description="Returns the minimum value for each window of time", type="windowed aggregate")
	public static class WindowedMin extends ReducingWindowedAggregator {

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

	@FunctionName(alias = "stddev", description="Returns the standard deviation value for each window of time", type="windowed aggregate")
	public static class WindowedStdDev extends ReducingWindowedAggregator {

		@Override
		public void init(Object[] args) throws Exception {
			if (args.length > 1) {
				args[1] = "stddev";
			} else {
				args = new Object[] { args[0], "stddev" };
			}
			super.init(args);
		}

		@Override
		public List<DataPoint> aggregateAfterReduction(List<DataPoint> datapoints, boolean isFp) {
			return datapoints;
		}

	}

}
