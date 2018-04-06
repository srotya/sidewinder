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
package com.srotya.sidewinder.core.functions.transform;

public abstract class ConstantFunction extends TransformFunction {

	protected double constant;

	@Override
	public void init(Object[] args) throws Exception {
		if (args[0] instanceof Integer) {
			constant = (int) args[0];
		} else {
			constant = (double) args[0];
		}
	}

	@Override
	public int getNumberOfArgs() {
		return 1;
	}

}