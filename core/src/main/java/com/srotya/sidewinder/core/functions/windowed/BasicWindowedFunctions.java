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

	@FunctionName(alias = { "derivative", "dvdt" })
	public static class DerivativeFunction extends ReducingWindowedAggregator {

		@Override
		public List<DataPoint> aggregateAfterReduction(List<DataPoint> datapoints, boolean isFp) {
			DataPoint origin = datapoints.get(datapoints.size() - 1);
			int i = 0;
			while (i < datapoints.size() - 1) {
				if (!isFp) {
					long val = (datapoints.get(i + 1).getLongValue() - datapoints.get(i).getLongValue())
							/ (getTimeWindow() / 1000);
					datapoints.get(i).setLongValue(Math.abs(val));
				} else {
					double val = (datapoints.get(i + 1).getValue() - datapoints.get(i).getValue())
							/ (getTimeWindow() / 1000);
					datapoints.get(i).setLongValue(Math.abs(Double.doubleToLongBits(val)));
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

	@FunctionName(alias = "integral")
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
	
	@FunctionName(alias = "first")
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
	
	@FunctionName(alias = "last")
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
	
	@FunctionName(alias = "max")
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
	
	@FunctionName(alias = { "mean", "average" })
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
	
	@FunctionName(alias = "min")
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
	
	@FunctionName(alias = "stddev")
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
