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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.reflections.Reflections;

import com.srotya.sidewinder.core.functions.list.FunctionName;

/**
 * @author ambud
 */
@SuppressWarnings("unchecked")
public class FunctionIteratorTable {

	private static final Logger logger = Logger.getLogger(FunctionIteratorTable.class.getName());
	public static final String NONE = "none";
	public static final FunctionIteratorTable table = new FunctionIteratorTable();
	public Map<String, Class<? extends FunctionIterator>> functionMap;

	private FunctionIteratorTable() {
		functionMap = new HashMap<>();
	}

	static {
		String packageName = FunctionIteratorTable.class.getPackage().getName();
		findAndRegisterFunctionsWithPackageName(packageName);
		FunctionIteratorTable.get().register(NONE, null);
	}

	public static void findAndRegisterFunctionsWithPackageName(String packageName) {
		Reflections reflections = new Reflections(packageName.trim());
		Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(FunctionName.class);
		for (Class<?> annotatedClass : annotatedClasses) {
			FunctionName function = annotatedClass.getAnnotation(FunctionName.class);
			String[] aliases = function.alias();
			String description = function.description();
			if (aliases == null || aliases.length == 0) {
				logger.warning("Ignoring aggregation function:" + annotatedClass.getName());
				continue;
			}
			for (String alias : aliases) {
				alias = alias.trim();
				if (!alias.isEmpty()) {
					FunctionIteratorTable.get().register(alias, (Class<? extends FunctionIterator>) annotatedClass);
					logger.fine("|" + alias + "|" + annotatedClass.getSimpleName() + "|" + description + "|"
							+ function.type() + "|");
				}
			}
		}
	}

	public static FunctionIteratorTable get() {
		return table;
	}

	public void register(String name, Class<? extends FunctionIterator> functionClass) {
		functionMap.put(name.toLowerCase(), functionClass);
	}

	public Class<? extends FunctionIterator> lookupFunction(String name) {
		return functionMap.get(name.toLowerCase());
	}

	public Set<String> listFunctions() {
		return functionMap.keySet();
	}
}
