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

public class BasicSingleFunctions {

	@FunctionName(alias = "sfirst", description = "Returns the first value in the series", type = "single")
	public static class FirstFunction extends ReduceFunction {

		public FirstFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		protected double reduce(double prevReturn, double current) {
			return prevReturn;
		}

		@Override
		protected long reduce(long prevReturn, long current) {
			return prevReturn;
		}

	}

	@FunctionName(alias = "slast", description = "Returns the last value in the series", type = "single")
	public static class LastFunction extends ReduceFunction {

		public LastFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		protected double reduce(double prevReturn, double current) {
			return current;
		}

		@Override
		protected long reduce(long prevReturn, long current) {
			return current;
		}

	}

	@FunctionName(alias = "smax", description = "Returns the largest value in the series", type = "single")
	public static class MaxFunction extends ReduceFunction {

		public MaxFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		protected double reduce(double prevReturn, double current) {
			if (prevReturn < current) {
				return current;
			} else {
				return prevReturn;
			}
		}

		@Override
		protected long reduce(long prevReturn, long current) {
			if (prevReturn < current) {
				return current;
			} else {
				return prevReturn;
			}
		}

	}

	@FunctionName(alias = "smin", description = "Returns the smallest value in the series", type = "single")
	public static class MinFunction extends ReduceFunction {

		public MinFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		protected double reduce(double prevReturn, double current) {
			if (prevReturn > current) {
				return current;
			} else {
				return prevReturn;
			}
		}

		@Override
		protected long reduce(long prevReturn, long current) {
			if (prevReturn > current) {
				return current;
			} else {
				return prevReturn;
			}
		}

	}

	@FunctionName(alias = "ssum", description = "Returns the sum of all value in the series", type = "single")
	public static class SumFunction extends ReduceFunction {

		public SumFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		protected double reduce(double prevReturn, double current) {
			return prevReturn + current;
		}

		@Override
		protected long reduce(long prevReturn, long current) {
			return prevReturn + current;
		}

	}

	@FunctionName(alias = "srms", description = "Returns the Root Mean Squared value of the series", type = "single")
	public static class SRMSFunction extends ReduceFunction {

		public SRMSFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		protected double reduce(double prevReturn, double current) {
			return Math.sqrt(prevReturn * prevReturn + current * current);
		}

		@Override
		protected long reduce(long prevReturn, long current) {
			return (long) Math.sqrt(prevReturn * prevReturn + current * current);
		}

	}

	@FunctionName(alias = "smean", description = "Returns the average value of the series", type = "single")
	public static class MeanFunction extends ReduceFunction {

		private int n = 1;

		public MeanFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		protected double reduce(double prevReturn, double current) {
			double l = ((prevReturn * n) + current) / (n + 1);
			n++;
			return l;
		}

		@Override
		protected long reduce(long prevReturn, long current) {
			return (long) reduce((double) prevReturn, (double) current);
		}

	}

}