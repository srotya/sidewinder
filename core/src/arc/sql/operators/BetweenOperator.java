/**
 * Copyright 2016 Ambud Sharma
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
package com.srotya.sidewinder.core.operators;

public class BetweenOperator implements Operator<Number> {

	private Number upperBound;
	private Number lowerBound;
	private boolean isFloat;
	private boolean isInclusive;

	public BetweenOperator(boolean isFloat, boolean isInclusive, Number lowerBound, Number upperBound) {
		this.isFloat = isFloat;
		this.isInclusive = isInclusive;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}

	@Override
	public boolean operate(Number value) {
		if (isInclusive) {
			if (isFloat) {
				return lowerBound.doubleValue() <= value.doubleValue()
						&& upperBound.doubleValue() >= value.doubleValue();
			} else {
				return lowerBound.longValue() <= value.longValue() && upperBound.longValue() >= value.longValue();
			}
		} else {
			if (isFloat) {
				return lowerBound.doubleValue() < value.doubleValue() && upperBound.doubleValue() > value.doubleValue();
			} else {
				return lowerBound.longValue() <= value.longValue() && upperBound.longValue() >= value.longValue();
			}
		}
	}

}
