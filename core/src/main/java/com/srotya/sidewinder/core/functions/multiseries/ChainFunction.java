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
package com.srotya.sidewinder.core.functions.multiseries;

import java.util.ArrayList;
import java.util.List;

import com.srotya.sidewinder.core.functions.Function;
import com.srotya.sidewinder.core.storage.Series;

/**
 * @author ambud
 */
public class ChainFunction implements Function {

	private List<Function> chain;

	@Override
	public List<Series> apply(List<Series> t) {
		List<Series> output = t;
		for(Function f:chain) {
			output = f.apply(output);
		}
		return output;
	}

	@Override
	public void init(Object[] args) throws Exception {
		chain = new ArrayList<>();
		for (int i = 0; i < args.length; i++) {
			Object object = args[i];
			chain.add((Function) object);
		}
	}

	@Override
	public int getNumberOfArgs() {
		return -1;
	}

}
