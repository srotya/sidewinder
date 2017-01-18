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
package com.srotya.sidewinder.core.sql.operators;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public class BetweenOperator implements Operator {

	private Number upperBound;
	private Number lowerBound;
	private boolean isFloat;
	private boolean isInclusive;
	private String column;

	public BetweenOperator(String column, boolean isFloat, boolean isInclusive, Number lowerBound, Number upperBound) {
		this.column = column;
		this.isFloat = isFloat;
		this.isInclusive = isInclusive;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}

	@Override
	public boolean operate(DataPoint val) {
		Number value = null;
		if (column.equals("timestamp")) {
			value = (Number) val.getTimestamp();
		} else {
			if (isFloat) {
				value = (Number) val.getValue();
			} else {
				value = (Number) val.getLongValue();
			}
		}
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

	/**
	 * @return the upperBound
	 */
	public Number getUpperBound() {
		return upperBound;
	}

	/**
	 * @param upperBound
	 *            the upperBound to set
	 */
	public void setUpperBound(Number upperBound) {
		this.upperBound = upperBound;
	}

	/**
	 * @return the lowerBound
	 */
	public Number getLowerBound() {
		return lowerBound;
	}

	/**
	 * @param lowerBound
	 *            the lowerBound to set
	 */
	public void setLowerBound(Number lowerBound) {
		this.lowerBound = lowerBound;
	}

	/**
	 * @return the isFloat
	 */
	public boolean isFloat() {
		return isFloat;
	}

	/**
	 * @param isFloat
	 *            the isFloat to set
	 */
	public void setFloat(boolean isFloat) {
		this.isFloat = isFloat;
	}

	/**
	 * @return the isInclusive
	 */
	public boolean isInclusive() {
		return isInclusive;
	}

	/**
	 * @param isInclusive
	 *            the isInclusive to set
	 */
	public void setInclusive(boolean isInclusive) {
		this.isInclusive = isInclusive;
	}

}
