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
package com.srotya.sidewinder.core.predicates;

import java.util.List;

/**
 * @author ambud
 */
public class OrPredicate extends ComplexPredicate {

	public OrPredicate(List<Predicate> predicates) {
		super(predicates);
	}

	@Override
	public boolean shortCircuit(boolean prev, boolean current) {
		return prev || current;
	}

	@Override
	public boolean predicate(boolean prev, Predicate next, long value) {
		return prev || next.apply(value);
	}

	@Override
	public String toString() {
		return "OrOperator " + getPredicates() + "";
	}
}
