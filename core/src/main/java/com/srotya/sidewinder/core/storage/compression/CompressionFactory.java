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
	private static Map<Integer, Class<ValueWriter>> valueCodecMap = new HashMap<>();
	private static Map<Class<ValueWriter>, Integer> valueIdMap = new HashMap<>();
	private static Map<String, Class<ValueWriter>> valueCodecNameMap = new HashMap<>();

	private static Map<Integer, Class<TimeWriter>> timeCodecMap = new HashMap<>();
	private static Map<Class<TimeWriter>, Integer> timeIdMap = new HashMap<>();
	private static Map<String, Class<TimeWriter>> timeCodecNameMap = new HashMap<>();

	static {
		String compressionPackages = System.getProperty("compression.lib", "com.srotya.sidewinder.core.storage");
		findAndRegisterTimeCompressorsWithPackageName(compressionPackages);
		findAndRegisterValueCompressorsWithPackageName(compressionPackages);
	}
	
	@SuppressWarnings("unchecked")
	public static void findAndRegisterTimeCompressorsWithPackageName(String packageName) {
		Reflections reflections = new Reflections(packageName.trim());
		Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(TimeCodec.class);
		logger.info("Found " + annotatedClasses.size() + " compression classes for time fields");
		for (Class<?> annotatedClass : annotatedClasses) {
			TimeCodec compressor = annotatedClass.getAnnotation(TimeCodec.class);
			int id = compressor.id();
			String alias = compressor.name();
			timeCodecNameMap.put(alias, (Class<TimeWriter>) annotatedClass);
			timeCodecMap.put(id, (Class<TimeWriter>) annotatedClass);
			timeIdMap.put((Class<TimeWriter>) annotatedClass, id);
			logger.fine("Registering compression class with alias:" + alias);
		}
	}

	@SuppressWarnings("unchecked")
	public static void findAndRegisterValueCompressorsWithPackageName(String packageName) {
		Reflections reflections = new Reflections(packageName.trim());
		Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(ValueCodec.class);
		logger.info("Found " + annotatedClasses.size() + " compression classes for value fields");
		for (Class<?> annotatedClass : annotatedClasses) {
			ValueCodec compressor = annotatedClass.getAnnotation(ValueCodec.class);
			int id = compressor.id();
			String alias = compressor.name();
			valueCodecNameMap.put(alias, (Class<ValueWriter>) annotatedClass);
			valueCodecMap.put(id, (Class<ValueWriter>) annotatedClass);
			valueIdMap.put((Class<ValueWriter>) annotatedClass, id);
			logger.fine("Registering compression class with alias:" + alias);
		}
	}
	
	public static Class<TimeWriter> getTimeClassById(int id) {
		return timeCodecMap.get(id);
	}

	public static Class<TimeWriter> getTimeClassByName(String name) {
		return timeCodecNameMap.get(name);
	}

	public static int getIdByTimeClass(Class<TimeWriter> classObj) {
		return timeIdMap.get(classObj);
	}

	public static Class<ValueWriter> getValueClassById(int id) {
		return valueCodecMap.get(id);
	}

	public static Class<ValueWriter> getValueClassByName(String name) {
		return valueCodecNameMap.get(name);
	}

	public static int getIdByValueClass(Class<ValueWriter> classObj) {
		return valueIdMap.get(classObj);
	}
}
