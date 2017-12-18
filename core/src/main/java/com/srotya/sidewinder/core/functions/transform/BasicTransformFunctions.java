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
package com.srotya.sidewinder.core.functions.transform;

import com.srotya.sidewinder.core.functions.FunctionName;

public class BasicTransformFunctions {

	@FunctionName(alias = "cbrt")
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

	@FunctionName(alias = "ceil")
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

	@FunctionName(alias = "floor")
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

	@FunctionName(alias = "sqrt")
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

	@FunctionName(alias = "log")
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

	@FunctionName(alias = "log10")
	public static class LogBase10Function extends TransformFunction {

		@Override
		public long transform(long value) {
			return (long) Math.log10(value);
		}

		@Override
		public double transform(double value) {
			return Math.log10(value);
		}

	}

	@FunctionName(alias = "sin")
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

	@FunctionName(alias = "cos")
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

	@FunctionName(alias = "tan")
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

}
