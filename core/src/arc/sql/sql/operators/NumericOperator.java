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
public abstract class NumericOperator extends SimpleOperator {

	private boolean isFloat;

	public NumericOperator(String column, boolean isFloat, Number literal) {
		super(column, literal);
		this.isFloat = isFloat;
	}

	@Override
	public boolean operate(DataPoint value) {
		if(getLiteral().equals("timestamp")) {
			return compareTrue((Number) getLiteral(), (Number) value.getTimestamp());
		}else {
			return compareTrue((Number) getLiteral(), (Number) value.getLongValue());
		}
	}

	public abstract boolean compareTrue(Number literal, Number value);

	public boolean isFloat() {
		return isFloat;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "NumericOperator [column=" + getColumn() + "\tliteral=" + getLiteral() + "]";
	}

}
