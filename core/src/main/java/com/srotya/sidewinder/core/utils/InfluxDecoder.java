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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Point.Builder;

/**
 * @author ambud
 */
public class InfluxDecoder {

	private static final int LENGTH_OF_MILLISECOND_TS = 13;
	private static final Logger logger = Logger.getLogger(InfluxDecoder.class.getName());

	public static List<Point> pointsFromString(String dbName, String payload) {
		List<Point> dps = new ArrayList<>();
		String[] splits = payload.split("[\\r\\n]+");
		for (String split : splits) {
			try {
				String[] parts = split.split("\\s+");
				if (parts.length < 2 || parts.length > 3) {
					// invalid datapoint => drop
					continue;
				}
				long timestamp = System.currentTimeMillis();
				if (parts.length == 3) {
					timestamp = Long.parseLong(parts[2]);
					if (parts[2].length() > LENGTH_OF_MILLISECOND_TS) {
						timestamp = timestamp / (1000 * 1000);
					}
				} else {
					System.out.println("DB timestamp");
				}
				String[] key = parts[0].split(",");
				String measurementName = key[0];
				Set<String> tTags = new HashSet<>();
				for (int i = 1; i < key.length; i++) {
					tTags.add(key[i]);
				}
				List<String> tags = new ArrayList<>(tTags);
				String[] fields = parts[1].split(",");
				for (String field : fields) {
					String[] fv = field.split("=");
					String valueFieldName = fv[0];
					if (!fv[1].endsWith("i")) {
						Builder builder = Point.newBuilder();
						double value = Double.parseDouble(fv[1]);
						builder.setDbName(dbName);
						builder.setMeasurementName(measurementName);
						builder.setValueFieldName(valueFieldName);
						builder.setValue(Double.doubleToLongBits(value));
						builder.addAllTags(tags);
						builder.setTimestamp(timestamp);
						builder.setFp(true);
						dps.add(builder.build());
					} else {
						Builder builder = Point.newBuilder();
						fv[1] = fv[1].substring(0, fv[1].length() - 1);
						long value = Long.parseLong(fv[1]);
						builder.setDbName(dbName);
						builder.setMeasurementName(measurementName);
						builder.setValueFieldName(valueFieldName);
						builder.setValue(value);
						builder.addAllTags(tags);
						builder.setTimestamp(timestamp);
						builder.setFp(false);
						dps.add(builder.build());
					}
				}
			} catch (Exception e) {
				logger.fine("Rejected:" + split);
			}
		}
		return dps;
	}

}
