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
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Point.Builder;
import com.srotya.sidewinder.core.rpc.Tag;

/**
 * @author ambud
 */
public class InfluxDecoder {

	private static final Splitter TAG = Splitter.on('=');
	private static final Splitter SPACE = Splitter.on(Pattern.compile("\\s+"));
	private static final Splitter COMMA = Splitter.on(',');
	private static final Splitter NEWLINE = Splitter.on('\n');
	private static final int LENGTH_OF_MILLISECOND_TS = 13;
	private static final Logger logger = Logger.getLogger(InfluxDecoder.class.getName());

	public static List<Point> pointsFromString(String dbName, String payload) {
		List<Point> dps = new ArrayList<>();
		try {
			Iterable<String> splits = NEWLINE.splitToList(payload);
			for (String split : splits) {
				List<String> parts = SPACE.splitToList(split);
				if (parts.size() < 2 || parts.size() > 3) {
					// invalid datapoint => drop
					continue;
				}
				long timestamp = System.currentTimeMillis();
				if (parts.size() == 3) {
					timestamp = Long.parseLong(parts.get(2));
					if (parts.get(2).length() > LENGTH_OF_MILLISECOND_TS) {
						timestamp = timestamp / (1000 * 1000);
					}
				} else {
					logger.info("Bad datapoint timestamp:" + parts.size());
				}
				List<String> key = COMMA.splitToList(parts.get(0));
				String measurementName = key.get(0);
				Set<Tag> tTags = new HashSet<>();
				for (int i = 1; i < key.size(); i++) {
					// Matcher matcher = TAG_PATTERN.matcher(key[i]);
					// if (matcher.find()) {
					// tTags.add(Tag.newBuilder().setTagKey(matcher.group(1)).setTagValue(matcher.group(2)).build());
					// }

					List<String> s = TAG.splitToList(key.get(i));
					tTags.add(Tag.newBuilder().setTagKey(s.get(0)).setTagValue(s.get(1)).build());
				}
				List<Tag> tags = new ArrayList<>(tTags);
				List<String> fields = COMMA.splitToList(parts.get(1));
				Builder builder = Point.newBuilder();
				builder.setDbName(dbName);
				builder.setMeasurementName(measurementName);
				builder.addAllTags(tags);
				builder.setTimestamp(timestamp);
				for (String field : fields) {
					String[] fv = field.split("=");
					String valueFieldName = fv[0];
					if (!fv[1].endsWith("i")) {
						double value = Double.parseDouble(fv[1]);
						builder.addValueFieldName(valueFieldName);
						builder.addValue(Double.doubleToLongBits(value));
						builder.addFp(true);
					} else {
						fv[1] = fv[1].substring(0, fv[1].length() - 1);
						long value = Long.parseLong(fv[1]);
						builder.addValueFieldName(valueFieldName);
						builder.addValue(value);
						builder.addFp(false);
					}
				}
				dps.add(builder.build());
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.fine("Rejected:" + payload);
		}
		return dps;
	}

}
