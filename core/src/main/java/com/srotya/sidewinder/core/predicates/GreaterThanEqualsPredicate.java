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

/**
 * {@link Predicate} implementing Greater than equals operator, where the
 * supplied (tested) value is greater than equal to the predicate's configured
 * value.
 * 
 * @author ambud
 */
public class GreaterThanEqualsPredicate implements Predicate {

	private long rhs;

	public GreaterThanEqualsPredicate(long rhs) {
		this.rhs = rhs;
	}

	@Override
	public boolean test(long value) {
		return value >= rhs;
	}

}
