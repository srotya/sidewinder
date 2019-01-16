/**
 * Copyright Ambud Sharma
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
package com.srotya.sidewinder.core.functions.iterative;

import java.lang.reflect.Constructor;

import com.srotya.sidewinder.core.storage.DataPointIterator;

public abstract class FunctionIterator extends DataPointIterator {

	protected DataPointIterator iterator;
	protected boolean isFp;
	
	public static FunctionIterator getInstance(Class<? extends FunctionIterator> type, DataPointIterator itr, boolean isFp) throws Exception {
		Constructor<? extends FunctionIterator> c = type.getConstructor(DataPointIterator.class, Boolean.TYPE);
		return c.newInstance(itr, isFp);
	}
	
	public static FunctionIterator getDummyInstance(Class<? extends FunctionIterator> type) throws Exception {
		return getInstance(type, new DataPointIterator(null, null), false);
	}

	public FunctionIterator(DataPointIterator iterator, boolean isFp) {
		this.iterator = iterator;
		this.isFp = isFp;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	public abstract void init(Object[] args) throws Exception;

	public abstract int getNumberOfArgs();

	/**
	 * @return the iterator
	 */
	public DataPointIterator getIterator() {
		return iterator;
	}

	/**
	 * @param iterator the iterator to set
	 */
	public void setIterator(DataPointIterator iterator) {
		this.iterator = iterator;
	}

	/**
	 * @return the isFp
	 */
	public boolean isFp() {
		return isFp;
	}

	/**
	 * @param isFp the isFp to set
	 */
	public void setFp(boolean isFp) {
		this.isFp = isFp;
	}

}