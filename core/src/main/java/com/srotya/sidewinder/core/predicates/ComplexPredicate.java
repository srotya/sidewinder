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
package com.srotya.sidewinder.core.predicates;

import java.util.ArrayList;
import java.util.List;

public abstract class ComplexPredicate implements Predicate {

	private List<Predicate> predicates;

	public ComplexPredicate(List<Predicate> predicates) {
		if(predicates==null) {
			this.predicates = new ArrayList<>();
		}else {
			this.predicates = predicates;
		}
	}
	
	@Override
	public boolean apply(long value) {
		boolean result = predicates.get(0).apply(value);
		for (int i = 1; i < predicates.size(); i++) {
			boolean temp = result;
			result = predicate(result, predicates.get(i), value);
			if (shortCircuit(temp, result)) {
				break;
			}
		}
		return result;
	}
	
	public abstract boolean shortCircuit(boolean prev, boolean current);

	public abstract boolean predicate(boolean prev, Predicate next, long value);

	public void addPredicate(Predicate predicate) {
		predicates.add(predicate);
	}
	
	public void addPredicates(List<Predicate> operators) {
		predicates.addAll(operators);
	}

	public List<Predicate> getPredicates() {
		return predicates;
	}

}
