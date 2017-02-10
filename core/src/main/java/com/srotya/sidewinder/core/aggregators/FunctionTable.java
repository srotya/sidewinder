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
package com.srotya.sidewinder.core.aggregators;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author ambud
 */
public class FunctionTable {

	private static final FunctionTable table = new FunctionTable();
	private Map<String, Class<? extends AggregationFunction>> functionMap;

	private FunctionTable() {
		functionMap = new HashMap<>();
	}
	
	static {
		FunctionTable.get().register("sum", SumFunction.class);
		FunctionTable.get().register("mean", MeanFunction.class);
		FunctionTable.get().register("avg", MeanFunction.class);
		FunctionTable.get().register("dvdt", DerivativeFunction.class);
		FunctionTable.get().register("none", null);
	}

	public static FunctionTable get() {
		return table;
	}

	public void register(String name, Class<? extends AggregationFunction> functionClass) {
		functionMap.put(name.toLowerCase(), functionClass);
	}

	public Class<? extends AggregationFunction> lookupFunction(String name) {
		return functionMap.get(name.toLowerCase());
	}
	
	public Set<String> listFunctions() {
		return functionMap.keySet();
	}
}
