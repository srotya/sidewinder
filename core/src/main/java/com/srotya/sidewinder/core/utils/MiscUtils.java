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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * Miscellaneous utility functions.
 * 
 * @author ambud
 */
public class MiscUtils {

	private MiscUtils() {
	}

	public static List<String> readAllLines(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<String> lines = new ArrayList<>();
		String temp = null;
		while ((temp = reader.readLine()) != null) {
			lines.add(temp);
		}
		reader.close();
		return lines;
	}

	public static DataPoint buildDataPoint(String dbName, String measurementName, String valueFieldName,
			List<String> tags, long timestamp, long value) {
		DataPoint dp = new DataPoint();
		dp.setDbName(dbName);
		dp.setFp(false);
		dp.setLongValue(value);
		dp.setMeasurementName(measurementName);
		dp.setTags(tags);
		dp.setTimestamp(timestamp);
		dp.setValueFieldName(valueFieldName);
		return dp;
	}

	public static DataPoint buildDataPoint(String dbName, String measurementName, String valueFieldName,
			List<String> tags, long timestamp, double value) {
		DataPoint dp = new DataPoint();
		dp.setDbName(dbName);
		dp.setFp(true);
		dp.setValue(value);
		dp.setMeasurementName(measurementName);
		dp.setTags(tags);
		dp.setTimestamp(timestamp);
		dp.setValueFieldName(valueFieldName);
		return dp;
	}

	public static DataPoint buildDataPoint(long timestamp, long value) {
		DataPoint dp = new DataPoint();
		dp.setTimestamp(timestamp);
		dp.setLongValue(value);
		dp.setFp(false);
		return dp;
	}

	public static DataPoint buildDataPoint(long timestamp, double value) {
		DataPoint dp = new DataPoint();
		dp.setTimestamp(timestamp);
		dp.setValue(value);
		dp.setFp(true);
		return dp;
	}

	public static void delete(File file) throws IOException {
		if (file.isDirectory()) {
			// directory is empty, then delete it
			if (file.list().length == 0) {
				file.delete();
				System.out.println("Directory is deleted : " + file.getAbsolutePath());
			} else {
				// list all the directory contents
				String files[] = file.list();
				for (String temp : files) {
					// construct the file structure
					File fileDelete = new File(file, temp);
					// recursive delete
					delete(fileDelete);
				}
				// check the directory again, if empty then delete it
				if (file.list().length == 0) {
					file.delete();
					System.out.println("Directory is deleted : " + file.getAbsolutePath());
				}
			}
		} else {
			// if file, then delete it
			file.delete();
			System.out.println("File is deleted : " + file.getAbsolutePath());
		}
	}

	public static String tagToString(List<String> tags) {
		StringBuilder builder = new StringBuilder();
		for (String tag : tags) {
			builder.append("/");
			builder.append(tag);
		}
		return builder.toString();
	}

	public static DataPoint pointToDataPoint(Point point) {
		DataPoint dp = new DataPoint();
		dp.setDbName(point.getDbName());
		dp.setFp(point.getFp());
		dp.setLongValue(point.getValue());
		dp.setMeasurementName(point.getMeasurementName());
		dp.setTags(new ArrayList<>(point.getTagsList()));
		dp.setTimestamp(point.getTimestamp());
		dp.setValueFieldName(point.getValueFieldName());
		return dp;
	}

}
