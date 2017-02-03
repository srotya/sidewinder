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
package com.srotya.sidewinder.core.api.grafana;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.filters.Filter;

/**
 * @author ambud
 */
public class TestGrafanaUtils {

	@Test
	public void testFilterExtractor() {
		List<String> filterElements = new ArrayList<>();
		JsonObject element = new JsonObject();
		element.addProperty("target", "ticker");
		JsonArray array = new JsonArray();
		JsonObject obj = new JsonObject();
		obj.addProperty("value", "test");
		array.add(obj);

		obj = new JsonObject();
		obj.addProperty("type", "condition");
		obj.addProperty("value", "AND");
		array.add(obj);

		obj = new JsonObject();
		obj.addProperty("value", "test2");
		array.add(obj);

		element.add("filters", array);
		
		System.out.println(new Gson().toJson(element));

		Filter<List<String>> filter = GrafanaUtils.extractGrafanaFilter(element, filterElements);
		System.out.println(filter);
		
		assertTrue(filter.isRetain(Arrays.asList("test","test2")));
		assertTrue(!filter.isRetain(Arrays.asList("test")));
	}
	
	@Test
	public void testMultiFilterExtractor() {
		List<String> filterElements = new ArrayList<>();
		JsonObject element = new JsonObject();
		element.addProperty("target", "ticker");
		JsonArray array = new JsonArray();
		JsonObject obj = new JsonObject();
		obj.addProperty("value", "test");
		array.add(obj);

		obj = new JsonObject();
		obj.addProperty("type", "condition");
		obj.addProperty("value", "AND");
		array.add(obj);

		obj = new JsonObject();
		obj.addProperty("value", "test2");
		array.add(obj);
		
		obj = new JsonObject();
		obj.addProperty("type", "condition");
		obj.addProperty("value", "AND");
		array.add(obj);
		
		obj = new JsonObject();
		obj.addProperty("value", "test3");
		array.add(obj);

		element.add("filters", array);
		
		System.out.println(new Gson().toJson(element));

		Filter<List<String>> filter = GrafanaUtils.extractGrafanaFilter(element, filterElements);
		System.out.println(filter);
		
		assertTrue(filter.isRetain(Arrays.asList("test","test2","test3")));
		assertTrue(!filter.isRetain(Arrays.asList("test")));
	}

}
