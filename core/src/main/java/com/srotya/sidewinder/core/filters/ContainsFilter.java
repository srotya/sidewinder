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
package com.srotya.sidewinder.core.filters;

import java.util.List;

/**
 * @author ambud
 *
 * @param <E>
 */
public class ContainsFilter<K, E extends List<K>> implements Filter<E> {

	private K literal;

	public ContainsFilter(K literal) {
		this.literal = literal;
	}

	@Override
	public boolean isRetain(E value) {
		return value.contains(literal);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ContainsFilter [literal=" + literal + "]";
	}

}
