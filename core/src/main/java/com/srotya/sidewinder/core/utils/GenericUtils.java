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
package com.srotya.sidewinder.core.utils;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

/**
 * @author ambud
 */
public class GenericUtils {
	
	private GenericUtils() {
	}

	public static List<String> jsonArrayToStringArray(JsonArray array) {
		List<String> ary = new ArrayList<>();
		for (JsonElement jsonElement : array) {
			ary.add(jsonElement.getAsString());
		}
		return ary;
	}

}
