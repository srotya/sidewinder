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
import java.util.List;

import com.srotya.sidewinder.core.storage.DataPointIterator;

public class FunctionIteratorFactory {

	private List<FunctionTemplate> templateChain;

	public FunctionIteratorFactory(List<FunctionTemplate> templateChain)
			throws NoSuchMethodException, SecurityException {
		this.templateChain = templateChain;
	}

	public DataPointIterator build(DataPointIterator iterator, boolean isFp) throws Exception {
		DataPointIterator functionIterator = iterator;
		// chain all the function outputs
		for (FunctionTemplate template : templateChain) {
			FunctionIterator temp = template.getConstructor().newInstance(functionIterator, isFp);
			temp.init(template.getArgs());
			functionIterator = temp;
		}
		return functionIterator;
	}

	public static class FunctionTemplate {

		private Object[] args;
		private Constructor<? extends FunctionIterator> constructor;

		public FunctionTemplate(Class<? extends FunctionIterator> functionIteratorClass, Object[] args)
				throws NoSuchMethodException, SecurityException {
			this.args = args;
			this.constructor = functionIteratorClass.getConstructor(DataPointIterator.class, Boolean.TYPE);
		}

		/**
		 * @return the args
		 */
		public Object[] getArgs() {
			return args;
		}

		/**
		 * @return the constructor
		 */
		public Constructor<? extends FunctionIterator> getConstructor() {
			return constructor;
		}
	}
}
