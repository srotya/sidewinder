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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.NotFoundException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.ByteUtils;

/**
 * @author ambud
 */
public class GrafanaUtils {

	private GrafanaUtils() {
	}

	public static void queryAndGetData(StorageEngine engine, String dbName, long startTs, long endTs,
			List<Target> output, TargetSeries targetSeriesEntry) {
		Map<String, List<DataPoint>> points;
		try {
			// TODO fix query point
			points = engine.queryDataPoints(dbName, targetSeriesEntry.getMeasurementName(),
					targetSeriesEntry.getFieldName(), startTs, endTs, null, null);
		} catch (ItemNotFoundException e) {
			throw new NotFoundException(e.getMessage());
		}
		for (Entry<String, List<DataPoint>> entry : points.entrySet()) {
			List<DataPoint> dataPoints = entry.getValue();
			Target tar = new Target(entry.getKey());
			for (DataPoint dataPoint : dataPoints) {
				if (!dataPoint.isFp()) {
					tar.getDatapoints().add(new Number[] { dataPoint.getLongValue(), dataPoint.getTimestamp() });
				} else {
					tar.getDatapoints().add(new Number[] { dataPoint.getValue(), dataPoint.getTimestamp() });
				}
			}
			output.add(tar);
		}
	}

	public static void extractTargetsFromJson(JsonObject json, List<TargetSeries> targetSeries) {
		JsonArray targets = json.get("targets").getAsJsonArray();
		for (int i = 0; i < targets.size(); i++) {
			JsonObject jsonElement = targets.get(i).getAsJsonObject();
			if (jsonElement != null) {
				targetSeries.add(new TargetSeries(jsonElement.get("target").getAsString(),
						jsonElement.get("field").getAsString(),
						ByteUtils.jsonArrayToStringList(jsonElement.get("filters").getAsJsonArray()),
						jsonElement.get("correlate").getAsBoolean()));
			}
		}
	}

}
