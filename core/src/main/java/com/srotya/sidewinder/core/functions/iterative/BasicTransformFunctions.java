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

public class BasicTransformFunctions {

	@FunctionName(alias = "tabs", description = "Returns the absolute value (non-negative) for each value in the series", type = "transform")
	public class AbsoluteFunction extends TransformFunction {

		public AbsoluteFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public double transform(double value) {
			return Math.abs(value);
		}

		@Override
		public long transform(long value) {
			return Math.abs(value);
		}

	}

	@FunctionName(alias = "tsquare", description = "Returns the square(^2) of each value in the series", type = "transform")
	public static class SquareFunction extends TransformFunction {

		public SquareFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.pow(value, 2);
		}

		@Override
		public double transform(double value) {
			return Math.pow(value, 2);
		}

	}

	@FunctionName(alias = "tcube", description = "Returns the cube(^3) of each value in the series", type = "transform")
	public static class CubeFunction extends TransformFunction {

		public CubeFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.pow(value, 3);
		}

		@Override
		public double transform(double value) {
			return Math.pow(value, 3);
		}

	}

	@FunctionName(alias = "tcbrt", description = "Returns the cube root of each value in the series", type = "transform")
	public static class CbrtFunction extends TransformFunction {

		public CbrtFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.cbrt(value);
		}

		@Override
		public double transform(double value) {
			return Math.cbrt(value);
		}

	}

	@FunctionName(alias = "tceil", description = "Returns the ceil of each value in the series", type = "transform")
	public static class CeilFunction extends TransformFunction {

		public CeilFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.ceil(value);
		}

		@Override
		public double transform(double value) {
			return Math.ceil(value);
		}

	}

	@FunctionName(alias = "tfloor", description = "Returns the floor of each value in the series", type = "transform")
	public static class FloorFunction extends TransformFunction {

		public FloorFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.floor(value);
		}

		@Override
		public double transform(double value) {
			return Math.floor(value);
		}

	}

	@FunctionName(alias = "tsqrt", description = "Returns the square root of each value in the series", type = "transform")
	public static class SqrtFunction extends TransformFunction {

		public SqrtFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.sqrt(value);
		}

		@Override
		public double transform(double value) {
			return Math.sqrt(value);
		}

	}

	@FunctionName(alias = "tlog", description = "Returns the log (base e) of each value in the series", type = "transform")
	public static class LogFunction extends TransformFunction {

		public LogFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.log(value);
		}

		@Override
		public double transform(double value) {
			return Math.log(value);
		}

	}

	@FunctionName(alias = "tlog10", description = "Returns the log (base 10) of each value in the series", type = "transform")
	public static class Log10Function extends TransformFunction {

		public Log10Function(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.log10(value);
		}

		@Override
		public double transform(double value) {
			return Math.log10(value);
		}

	}

	@FunctionName(alias = "tsin", description = "Returns the sine of each value in the series", type = "transform")
	public static class SineFunction extends TransformFunction {

		public SineFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.sin(value);
		}

		@Override
		public double transform(double value) {
			return Math.sin(value);
		}

	}

	@FunctionName(alias = "tcos", description = "Returns the cosime of each value in the series", type = "transform")
	public static class CosineFunction extends TransformFunction {

		public CosineFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.cos(value);
		}

		@Override
		public double transform(double value) {
			return Math.cos(value);
		}

	}

	@FunctionName(alias = "ttan", description = "Returns the tangent of each value in the series", type = "transform")
	public static class TangentFunction extends TransformFunction {

		public TangentFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return (long) Math.tan(value);
		}

		@Override
		public double transform(double value) {
			return Math.tan(value);
		}

	}

	@FunctionName(alias = "tneg", description = "Returns the negative (sign inverted) of each value in the series", type = "transform")
	public static class NegateFunction extends TransformFunction {

		public NegateFunction(DataPointIterator iterator, boolean isFp) {
			super(iterator, isFp);
		}

		@Override
		public long transform(long value) {
			return -value;
		}

		@Override
		public double transform(double value) {
			return -value;
		}

	}

}
