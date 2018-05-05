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
package com.srotya.sidewinder.core.api.grafana;

import java.io.IOException;
import java.util.List;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.filters.ComplexTagFilter;
import com.srotya.sidewinder.core.filters.ComplexTagFilter.ComplexFilterType;
import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.functions.Function;
import com.srotya.sidewinder.core.functions.FunctionTable;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.SeriesOutput;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.InvalidFilterException;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class GrafanaUtils {

	private static final Logger logger = Logger.getLogger(GrafanaUtils.class.getName());

	private GrafanaUtils() {
	}

	public static void queryAndGetData(StorageEngine engine, String dbName, long startTs, long endTs,
			List<Target> output, TargetSeries targetSeriesEntry) throws IOException {
		List<SeriesOutput> points;
		try {
			points = engine.queryDataPoints(dbName, targetSeriesEntry.getMeasurementName(),
					targetSeriesEntry.getFieldName(), startTs, endTs, targetSeriesEntry.getTagFilter(), null,
					targetSeriesEntry.getAggregationFunction());
		} catch (ItemNotFoundException e) {
			throw new NotFoundException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new BadRequestException(e.getMessage());
		}
		if (points != null) {
			for (SeriesOutput entry : points) {
				Target tar = new Target(entry.toString());
				List<DataPoint> dps = entry.getDataPoints();
				if (dps != null) {
					for (DataPoint point : dps) {
						if (!entry.isFp()) {
							tar.getDatapoints().add(new Number[] { point.getLongValue(), point.getTimestamp() });
						} else {
							tar.getDatapoints().add(new Number[] { point.getValue(), point.getTimestamp() });
						}
					}
				}
				tar.sort();
				output.add(tar);
			}
		}
	}

	/**
	 * Extract target series from Grafana Json query payload
	 * 
	 * @param json
	 * @param targetSeries
	 * @throws InvalidFilterException 
	 */
	public static void extractTargetsFromJson(JsonObject json, List<TargetSeries> targetSeries) throws InvalidFilterException {
		JsonArray targets = json.get("targets").getAsJsonArray();
		for (int i = 0; i < targets.size(); i++) {
			JsonObject jsonElement = targets.get(i).getAsJsonObject();
			if (jsonElement == null) {
				continue;
			}
			if (jsonElement.has("target") && jsonElement.has("field")) {
				TagFilter filter = extractGrafanaFilter(jsonElement);
				Function aggregationFunction = extractGrafanaAggregation(jsonElement);
				boolean correlate = false;
				if (jsonElement.has("correlate")) {
					correlate = jsonElement.get("correlate").getAsBoolean();
				}
				TargetSeries e = new TargetSeries(jsonElement.get("target").getAsString(),
						jsonElement.get("field").getAsString(), filter, aggregationFunction, correlate);
				targetSeries.add(e);
				logger.log(Level.FINE, () -> "Parsed and extracted target:" + e);
			} else if (jsonElement.has("raw") && jsonElement.get("rawQuery").getAsBoolean()) {
				// raw query recieved
				TargetSeries e = MiscUtils.extractTargetFromQuery(jsonElement.get("raw").getAsString());
				logger.log(Level.FINE, () -> "Parsed and extracted raw query:" + e);
				if (e != null) {
					targetSeries.add(e);
				}
			}
		}
	}

	public static Function extractGrafanaAggregation(JsonObject jsonElement) {
		if (!jsonElement.has("aggregator")) {
			return null;
		}
		JsonObject obj = jsonElement.get("aggregator").getAsJsonObject();
		if (!obj.has("name") || !obj.has("args")) {
			return null;
		}
		String name = obj.get("name").getAsString();
		if (name.equalsIgnoreCase("none")) {
			return null;
		}
		int multipleFactor = 1;
		if (obj.has("unit")) {
			multipleFactor = toSeconds(obj.get("unit").getAsString());
		}
		Class<? extends Function> lookupFunction = FunctionTable.get().lookupFunction(name);
		if (lookupFunction != null) {
			try {
				Function instance = (Function) lookupFunction.newInstance();
				JsonArray ary = obj.get("args").getAsJsonArray();
				Object[] args = new Object[ary.size()];
				for (JsonElement element : ary) {
					JsonObject arg = element.getAsJsonObject();
					// ignore invalid aggregation function
					if (!arg.has("index") || !arg.has("value")) {
						return null;
					}
					int index = arg.get("index").getAsInt();
					switch (arg.get("type").getAsString().toLowerCase()) {
					case "string":
						args[index] = arg.get("value").getAsString();
						break;
					case "int":
						args[index] = arg.get("value").getAsInt() * multipleFactor;
						break;
					case "long":
						args[index] = arg.get("value").getAsLong() * multipleFactor;
						break;
					case "double":
						args[index] = arg.get("value").getAsDouble();
						break;
					}
				}
				instance.init(args);
				return instance;
			} catch (Exception e) {
				logger.log(Level.FINE, "Failed to extract aggregate function:" + jsonElement, e);
			}
		}
		return null;
	}

	public static TagFilter extractGrafanaFilter(JsonObject element) throws InvalidFilterException {
		Stack<TagFilter> predicateStack = new Stack<>();
		JsonArray array = element.get("filters").getAsJsonArray();
		for (int i = 0; i < array.size(); i++) {
			JsonObject obj = array.get(i).getAsJsonObject();
			if (!obj.has("value")) {
				continue;
			}
			String val = obj.get("value").getAsString();
			if (obj.has("type")) {
				if (predicateStack.isEmpty()) {
					// error
					System.err.println("Error empty stack");
				} else {
					TagFilter pop = predicateStack.pop();
					if (val.equalsIgnoreCase("and")) {
						ComplexTagFilter andFilter = new ComplexTagFilter(ComplexFilterType.AND);
						andFilter.addFilter(pop);
						predicateStack.push(andFilter);
					} else if (val.equalsIgnoreCase("or")) {
						ComplexTagFilter orFilter = new ComplexTagFilter(ComplexFilterType.OR);
						orFilter.addFilter(pop);
						predicateStack.push(orFilter);
					} else {
						// error
						System.err.println("Error stack is not empty");
					}
				}
			} else {
				SimpleTagFilter filter = MiscUtils.buildSimpleFilter(obj.get("key").getAsString()
						+ obj.get("operator").getAsString() + obj.get("value").getAsString());
				logger.log(Level.FINE, () -> "Simple filter:" + filter);
				if (predicateStack.isEmpty()) {
					predicateStack.push(filter);
				} else {
					if (predicateStack.peek() instanceof SimpleTagFilter) {
						// error
						System.err.println("Stack contains bad filter");
					} else {
						((ComplexTagFilter) predicateStack.peek()).addFilter(filter);
					}
				}
			}
		}
		if (predicateStack.isEmpty()) {
			return null;
		} else {
			return predicateStack.pop();
		}
	}

	public static int toSeconds(String unit) {
		int secs = 1;
		switch (unit) {
		case "mins":
			secs = 60;
			break;
		case "hours":
			secs = 60 * 60;
			break;
		case "days":
			secs = 60 * 60 * 24;
			break;
		case "weeks":
			secs = 60 * 60 * 24 * 7;
			break;
		case "months":
			secs = 60 * 60 * 24 * 30;
			break;
		case "years":
			secs = 60 * 60 * 24 * 365;
			break;
		}
		return secs;
	}

}
