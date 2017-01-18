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

public abstract class NumericOperator extends SimpleOperator<Number>{

	private boolean isFloat;

	public NumericOperator(boolean isFloat, Number literal) {
		super(literal);
		this.isFloat = isFloat;
	}

	@Override
	public boolean operate(Number value) {
		return compareTrue(getLiteral(), value) ;
	}

	public abstract boolean compareTrue(Number literal, Number value);
	
	public boolean isFloat() {
		return isFloat;
	}

}
