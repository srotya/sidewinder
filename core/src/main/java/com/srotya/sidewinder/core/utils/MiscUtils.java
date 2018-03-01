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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.BadRequestException;

import com.srotya.sidewinder.core.api.grafana.TargetSeries;
import com.srotya.sidewinder.core.filters.ComplexTagFilter;
import com.srotya.sidewinder.core.filters.ComplexTagFilter.ComplexFilterType;
import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.SimpleTagFilter.FilterType;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.functions.Function;
import com.srotya.sidewinder.core.functions.FunctionTable;
import com.srotya.sidewinder.core.functions.multiseries.ChainFunction;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Point.Builder;
import com.srotya.sidewinder.core.storage.DataPoint;

/**
 * Miscellaneous utility functions.
 * 
 * @author ambud
 */
public class MiscUtils {

	private static final Pattern NUMBER = Pattern.compile("\\d+(\\.\\d+)?");
	private static final Pattern EXPRESSION = Pattern.compile("([a-zA-Z0-9\\-\\_]+)(=|<=|>=|<|>)([a-zA-Z0-9\\-\\_]+)");

	private MiscUtils() {
	}

	public static String[] splitAndNormalizeString(String input) {
		return input.split(",\\s+");
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

	public static DataPoint buildDataPoint(long timestamp, long value) {
		DataPoint dp = new DataPoint();
		dp.setTimestamp(timestamp);
		dp.setLongValue(value);
		return dp;
	}

	public static DataPoint buildDataPoint(long timestamp, double value) {
		DataPoint dp = new DataPoint();
		dp.setTimestamp(timestamp);
		dp.setValue(value);
		return dp;
	}

	public static void ls(File file) throws IOException {
		if (file.isDirectory()) {
			for (File file2 : file.listFiles()) {
				System.out.println(file2.getAbsolutePath());
			}
		} else {
			System.out.println(file.getAbsolutePath());
		}
	}

	public static boolean delete(File file) throws IOException {
		if (file.isDirectory()) {
			// directory is empty, then delete it
			if (file.list().length == 0) {
				return file.delete();
			} else {
				// list all the directory contents
				String files[] = file.list();
				boolean result = false;
				for (String temp : files) {
					// construct the file structure
					File fileDelete = new File(file, temp);
					// recursive delete
					result = delete(fileDelete);
					if (!result) {
						return false;
					}
				}
				// check the directory again, if empty then delete it
				if (file.list().length == 0) {
					file.delete();
				}
				return result;
			}
		} else {
			// if file, then delete it
			return file.delete();
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
		pointToDataPoint(dp, point);
		return dp;
	}

	public static void pointToDataPoint(DataPoint dp, Point point) {
		if (point.getFp()) {
			dp.setValue(Double.doubleToLongBits(point.getValue()));
		} else {
			dp.setLongValue(point.getValue());
		}
		dp.setTimestamp(point.getTimestamp());
	}

	public static TagFilter buildTagFilter(String tagFilter) throws InvalidFilterException {
		String[] tagSet = tagFilter.split("(&|\\|)");
		try {
			Stack<TagFilter> predicateStack = new Stack<>();
			for (int i = 0; i < tagSet.length; i++) {
				String item = tagSet[i];
				if (predicateStack.isEmpty()) {
					SimpleTagFilter filter = buildSimpleFilter(item);
					predicateStack.push(filter);
				} else {
					TagFilter pop = predicateStack.pop();
					char operator = tagFilter.charAt(tagFilter.indexOf(tagSet[i]) - 1);
					switch (operator) {
					case '|':
						predicateStack.push(new ComplexTagFilter(ComplexFilterType.OR,
								(Arrays.asList(pop, buildSimpleFilter(item)))));
						break;
					case '&':
						predicateStack.push(new ComplexTagFilter(ComplexFilterType.AND,
								(Arrays.asList(pop, buildSimpleFilter(item)))));
						break;
					}
				}
			}

			if (predicateStack.isEmpty()) {
				return null;
			} else {
				return predicateStack.pop();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new InvalidFilterException();
		}
	}

	public static SimpleTagFilter buildSimpleFilter(String item) {
		Matcher matcher = EXPRESSION.matcher(item);
		if (!matcher.matches()) {
			throw new IllegalArgumentException("Invalid expression:" + item);
		}
		FilterType type = null;
		switch (matcher.group(2)) {
		case "=":
			type = FilterType.EQUALS;
			break;
		case ">=":
			type = FilterType.GREATER_THAN_EQUALS;
			break;
		case ">":
			type = FilterType.GREATER_THAN;
			break;
		case "<":
			type = FilterType.LESS_THAN;
			break;
		case "<=":
			type = FilterType.LESS_THAN_EQUALS;
			break;
		}
		SimpleTagFilter filter = new SimpleTagFilter(type, matcher.group(1), matcher.group(3));
		return filter;
	}

	public static Function createFunctionChain(String[] parts, int startIndex)
			throws InstantiationException, IllegalAccessException, Exception {
		Function[] arguments = new Function[parts.length - startIndex];
		for (int k = startIndex, p = 0; k < parts.length; k++, p++) {
			String[] args = parts[k].split(",");
			Class<? extends Function> lookupFunction = FunctionTable.get().lookupFunction(args[0]);
			if (lookupFunction == null) {
				throw new BadRequestException("Unknown function:" + args[0]);
			}
			Function instance = (Function) lookupFunction.newInstance();
			if (args.length - 1 < instance.getNumberOfArgs()) {
				throw new BadRequestException("Insufficient arguments for aggregation function, needed:"
						+ instance.getNumberOfArgs() + ", found:" + (args.length - 1));
			}
			Object[] ary = new Object[args.length - 1];
			for (int i = 1; i < args.length; i++) {
				Matcher matcher = NUMBER.matcher(args[i]);
				if (matcher.matches()) {
					if (matcher.group(1) != null) {
						ary[i - 1] = Double.parseDouble(args[i]);
					} else {
						ary[i - 1] = Integer.parseInt(args[i]);
					}
				} else {
					ary[i - 1] = args[i];
				}
			}
			instance.init(ary);
			arguments[p] = instance;
		}
		ChainFunction function = new ChainFunction();
		function.init(arguments);
		return function;
	}

	public static TargetSeries extractTargetFromQuery(String query) {
		if (query == null || query.isEmpty()) {
			return null;
		}
		String[] queryParts = query.split("<=?");
		if (queryParts.length > 1) {
			query = queryParts[1];
		}

		String[] parts = query.split("=>");
		// select part
		query = parts[0];
		String[] splits = query.split("\\.");
		if (splits.length < 2) {
			throw new BadRequestException(
					"Invalid query string:" + query + ". Must contain measurement and value field name");
		}
		String measurementName = splits[0];
		String valueFieldName = splits[1];

		TagFilter tagFilter = null;
		if (splits.length >= 3) {
			try {
				tagFilter = buildTagFilter(splits[2]);
			} catch (InvalidFilterException e) {
				throw new BadRequestException(e);
			}
		}
		Function aggregationFunction = null;
		if (parts.length > 1) {
			try {
				aggregationFunction = createFunctionChain(parts, 1);
			} catch (Exception e) {
				throw new BadRequestException(e);
			}
		}

		return new TargetSeries(measurementName, valueFieldName, tagFilter, aggregationFunction, false);
	}

	public static Point buildDataPoint(String dbName, String measurementName, String valueFieldName, List<String> tags,
			long timestamp, long value) {
		return buildDP(dbName, measurementName, valueFieldName, tags, timestamp, value, false);
	}

	public static Point buildDP(String dbName, String measurementName, String valueFieldName, List<String> tags,
			long timestamp, long value, boolean fp) {
		Builder builder = Point.newBuilder();
		builder.setDbName(dbName);
		builder.setMeasurementName(measurementName);
		builder.setValueFieldName(valueFieldName);
		builder.addAllTags(tags);
		builder.setTimestamp(timestamp);
		builder.setValue(value);
		builder.setFp(fp);
		return builder.build();
	}

	public static Point buildDataPoint(String dbName, String measurementName, String valueFieldName, List<String> tags,
			long timestamp, double value) {
		return buildDP(dbName, measurementName, valueFieldName, tags, timestamp, Double.doubleToLongBits(value), true);
	}

	public static String printBuffer(ByteBuffer buffer, int counter) {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < counter; i++) {
			String line = MiscUtils.getStringFromBuffer(buffer);
			buf.append("\n" + i + " " + line);
		}
		return buf.toString();
	}

	public static void writeStringToBuffer(String str, ByteBuffer buf) {
		buf.putShort((short) str.length());
		buf.put(str.getBytes());
	}

	public static String getStringFromBuffer(ByteBuffer buf) {
		short length = buf.getShort();
		byte[] dst = new byte[length];
		buf.get(dst);
		return new String(dst);
	}

}
