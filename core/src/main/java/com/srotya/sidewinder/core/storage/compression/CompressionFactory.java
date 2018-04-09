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
package com.srotya.sidewinder.core.storage.compression;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.reflections.Reflections;

/**
 * @author ambud
 */
public class CompressionFactory {

	private static final Logger logger = Logger.getLogger(CompressionFactory.class.getName());
	private static Map<Integer, Class<Writer>> codecMap = new HashMap<>();
	private static Map<Class<Writer>, Integer> idMap = new HashMap<>();
	private static Map<String, Class<Writer>> codecNameMap = new HashMap<>();

	static {
		String compressionPackages = System.getProperty("compression.lib", "com.srotya.sidewinder.core.storage");
		findAndRegisterCompressorsWithPackageName(compressionPackages);
	}

	@SuppressWarnings("unchecked")
	public static void findAndRegisterCompressorsWithPackageName(String packageName) {
		Reflections reflections = new Reflections(packageName.trim());
		Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(Codec.class);
		logger.info("Found " + annotatedClasses.size() + " compression classes");
		for (Class<?> annotatedClass : annotatedClasses) {
			Codec compressor = annotatedClass.getAnnotation(Codec.class);
			int id = compressor.id();
			String alias = compressor.name();
			codecNameMap.put(alias, (Class<Writer>) annotatedClass);
			codecMap.put(id, (Class<Writer>) annotatedClass);
			idMap.put((Class<Writer>) annotatedClass, id);
			logger.fine("Registering compression class with alias:" + alias);
		}
	}

	public static Class<Writer> getClassById(int id) {
		return codecMap.get(id);
	}

	public static Class<Writer> getClassByName(String name) {
		return codecNameMap.get(name);
	}

	public static int getIdByClass(Class<Writer> classObj) {
		return idMap.get(classObj);
	}
}
