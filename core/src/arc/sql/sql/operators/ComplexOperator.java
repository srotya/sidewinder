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

import java.util.ArrayList;
import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * @author ambud
 */
public abstract class ComplexOperator implements Operator {

	private List<Operator> operators;

	public ComplexOperator(List<Operator> operators) {
		if (operators == null) {
			this.operators = new ArrayList<>();
		} else {
			this.operators = operators;
		}
	}

	@Override
	public boolean operate(DataPoint value) {
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

	public abstract boolean operator(boolean prev, Operator next, DataPoint value);

	public void addOperator(Operator operator) {
		operators.add(operator);
	}
	
	public void addOperators(List<Operator> operators) {
		operators.addAll(operators);
	}

	public List<Operator> getOperators() {
		return operators;
	}

}
