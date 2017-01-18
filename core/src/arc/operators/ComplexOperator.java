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

import java.util.List;

public abstract class ComplexOperator<E> implements Operator<E> {

	private List<Operator<E>> operators;

	public ComplexOperator(List<Operator<E>> operators) {
		this.operators = operators;
	}

	@Override
	public boolean operate(E value) {
		boolean result = operators.get(0).operate(value);
		for (int i = 1; i < operators.size(); i++) {
			boolean temp = result;
			result = operator(result, operators.get(i), value);
			if (shortCircuit(temp, result)) {
				break;
			}
		}
		return result;
	}

	public abstract boolean shortCircuit(boolean prev, boolean current);

	public abstract boolean operator(boolean prev, Operator<E> next, E value);

}
