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

public class BasicConstantFunctions {

	@FunctionName(alias = "add", description = "Adds a constant to each value in the series (transform function)")
	public static class AddFunction extends ConstantFunction {

		@Override
		public long transform(long value) {
			return value + (long) constant;
		}

		@Override
		public double transform(double value) {
			return value + constant;
		}

	}

	@FunctionName(alias = "sub", description = "Subtracts a constant from each value in the series (transform function)")
	public static class SubtractFunction extends ConstantFunction {

		@Override
		public long transform(long value) {
			return value - (long) constant;
		}

		@Override
		public double transform(double value) {
			return value - constant;
		}

	}

	@FunctionName(alias = "mult", description = "Multiplies a constant to each value in the series (transform function)")
	public static class MultiplyFunction extends ConstantFunction {

		@Override
		public long transform(long value) {
			return value * (long) constant;
		}

		@Override
		public double transform(double value) {
			return value * constant;
		}

	}

	@FunctionName(alias = "divide", description = "Divides each value in the series with a constant (transform function)")
	public static class DivideFunction extends ConstantFunction {

		@Override
		public long transform(long value) {
			return value / (long) constant;
		}

		@Override
		public double transform(double value) {
			return value / constant;
		}

	}

}
