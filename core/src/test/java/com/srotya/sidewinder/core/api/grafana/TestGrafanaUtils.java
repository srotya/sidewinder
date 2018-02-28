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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.filters.AnyFilter;
import com.srotya.sidewinder.core.filters.Filter;

/**
 * @author ambud
 */
public class TestGrafanaUtils {

	@Test
	public void testExtractTargetsFromJson() {
		List<TargetSeries> targetSeries = new ArrayList<>();
		String query = "{\"panelId\":3,\"range\":{\"from\":\"2017-02-09T21:20:53.328Z\",\"to\":\"2017-02-10T00:20:53.328Z\",\"raw\":{\"from\":\"now-3h\",\"to\":\"now\"}},\"rangeRaw\":{\"from\":\"now-3h\",\"to\":\"now\"},\"interval\":\"15s\",\"intervalMs\":15000,\"targets\":[{\"target\":\"interface\",\"filters\":[{}],\"correlate\":false,\"field\":\"if_octets_rx\",\"refId\":\"A\",\"type\":\"timeserie\"}],\"format\":\"json\",\"maxDataPoints\":640}";
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonObject object = gson.fromJson(query, JsonObject.class);
		System.out.println(gson.toJson(object));
		GrafanaUtils.extractTargetsFromJson(object, targetSeries);
		assertEquals(1, targetSeries.size());
		assertEquals("interface", targetSeries.get(0).getMeasurementName());
		assertEquals("if_octets_rx", targetSeries.get(0).getFieldName());
		assertEquals(AnyFilter.class, targetSeries.get(0).getTagFilter().getClass());
	}
	
	@Test
	public void testExtractGrafanaAggregation() {
		
	}
	
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

		assertTrue(filter.retain(Arrays.asList("test", "test2")));
		assertTrue(!filter.retain(Arrays.asList("test")));
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

		assertTrue(filter.retain(Arrays.asList("test", "test2", "test3")));
		assertTrue(!filter.retain(Arrays.asList("test")));
	}

}
