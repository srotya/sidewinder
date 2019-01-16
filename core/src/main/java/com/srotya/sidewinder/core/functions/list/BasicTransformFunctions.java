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
package com.srotya.sidewinder.core.functions.list;

public class BasicTransformFunctions {

	@FunctionName(alias = "abs", description = "Returns the absolute value (non-negative) for each value in the series", type = "transform")
	public class AbsoluteFunction extends TransformFunction {

		@Override
		public double transform(double value) {
			return Math.abs(value);
		}

		@Override
		public long transform(long value) {
			return Math.abs(value);
		}

	}

	@FunctionName(alias = "square", description = "Returns the square(^2) of each value in the series", type = "transform")
	public static class SquareFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.pow(value, 2);
		}

		@Override
		public double transform(double value) {
			return Math.pow(value, 2);
		}

	}

	@FunctionName(alias = "cube", description = "Returns the cube(^3) of each value in the series", type = "transform")
	public static class CubeFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.pow(value, 3);
		}

		@Override
		public double transform(double value) {
			return Math.pow(value, 3);
		}

	}

	@FunctionName(alias = "cbrt", description = "Returns the cube root of each value in the series", type = "transform")
	public static class CbrtFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.cbrt(value);
		}

		@Override
		public double transform(double value) {
			return Math.cbrt(value);
		}

	}

	@FunctionName(alias = "ceil", description = "Returns the ceil of each value in the series", type = "transform")
	public static class CeilFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.ceil(value);
		}

		@Override
		public double transform(double value) {
			return Math.ceil(value);
		}

	}

	@FunctionName(alias = "floor", description = "Returns the floor of each value in the series", type = "transform")
	public static class FloorFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.floor(value);
		}

		@Override
		public double transform(double value) {
			return Math.floor(value);
		}

	}

	@FunctionName(alias = "sqrt", description = "Returns the square root of each value in the series", type = "transform")
	public static class SqrtFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.sqrt(value);
		}

		@Override
		public double transform(double value) {
			return Math.sqrt(value);
		}

	}

	@FunctionName(alias = "log", description = "Returns the log (base e) of each value in the series", type = "transform")
	public static class LogFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.log(value);
		}

		@Override
		public double transform(double value) {
			return Math.log(value);
		}

	}

	@FunctionName(alias = "log10", description = "Returns the log (base 10) of each value in the series", type = "transform")
	public static class Log10Function extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.log10(value);
		}

		@Override
		public double transform(double value) {
			return Math.log10(value);
		}

	}

	@FunctionName(alias = "sin", description = "Returns the sine of each value in the series", type = "transform")
	public static class SineFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.sin(value);
		}

		@Override
		public double transform(double value) {
			return Math.sin(value);
		}

	}

	@FunctionName(alias = "cos", description = "Returns the cosime of each value in the series", type = "transform")
	public static class CosineFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.cos(value);
		}

		@Override
		public double transform(double value) {
			return Math.cos(value);
		}

	}

	@FunctionName(alias = "tan", description = "Returns the tangent of each value in the series", type = "transform")
	public static class TangentFunction extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.tan(value);
		}

		@Override
		public double transform(double value) {
			return Math.tan(value);
		}

	}

	@FunctionName(alias = "neg", description = "Returns the negative (sign inverted) of each value in the series", type = "transform")
	public static class NegateFunction extends TransformFunction {

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
