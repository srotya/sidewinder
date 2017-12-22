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
package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.srotya.sidewinder.core.aggregators.AggregationFunction;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;

/**
 * @author ambud
 */
public interface Measurement {

	public static final String USE_QUERY_POOL = "use.query.pool";
	public static final String FIELD_TAG_SEPARATOR = "#";
	public static final String TAG_SEPARATOR = "_";

	public void configure(Map<String, String> conf, StorageEngine engine, String measurementName,
			String baseIndexDirectory, String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool)
			throws IOException;

	public Collection<TimeSeries> getTimeSeries();

	public Set<String> getSeriesKeys();
	// public Map<String, TimeSeries> getTimeSeriesMap();

	public TimeSeries getSeriesFromKey(String key);

	public TagIndex getTagIndex();

	public void loadTimeseriesFromMeasurements() throws IOException;

	public void close() throws IOException;

	public BufferObject createNewBuffer(String seriesId, String tsBucket) throws IOException;

	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException;

	public static void indexRowKey(TagIndex tagIndex, String rowKey, List<String> tags) throws IOException {
		for (String tag : tags) {
			tagIndex.index(tag, rowKey);
		}
	}

	public default String encodeTagsToString(TagIndex tagIndex, List<String> tags) throws IOException {
		StringBuilder builder = new StringBuilder(tags.size() * 5);
		builder.append(tagIndex.mapTag(tags.get(0)));
		for (int i = 1; i < tags.size(); i++) {
			String tag = tags.get(i);
			builder.append(TAG_SEPARATOR);
			builder.append(tagIndex.mapTag(tag));
		}
		return builder.toString();
	}

	public default String constructSeriesId(String valueFieldName, List<String> tags, TagIndex index)
			throws IOException {
		String encodeTagsToString = encodeTagsToString(index, tags);
		StringBuilder rowKeyBuilder = new StringBuilder(valueFieldName.length() + 1 + encodeTagsToString.length());
		rowKeyBuilder.append(valueFieldName);
		rowKeyBuilder.append(FIELD_TAG_SEPARATOR);
		rowKeyBuilder.append(encodeTagsToString);
		return rowKeyBuilder.toString();
	}

	public static List<String> decodeStringToTags(TagIndex tagLookupTable, String tagString) throws IOException {
		List<String> tagList = new ArrayList<>();
		if (tagString == null || tagString.isEmpty()) {
			return tagList;
		}
		for (String tag : tagString.split(TAG_SEPARATOR)) {
			tagList.add(tagLookupTable.getTagMapping(tag));
		}
		return tagList;
	}

	public String getMeasurementName();

	public default List<List<String>> getTagsForMeasurement(String valueFieldPattern) throws Exception {
		String tmpOriginal = valueFieldPattern;
		Set<String> keySet = new HashSet<>();
		if (valueFieldPattern.endsWith("$")) {
			valueFieldPattern = valueFieldPattern.substring(0, valueFieldPattern.length() - 1);
		}
		Pattern p = null;
		try {
			valueFieldPattern += ".*";
			p = Pattern.compile(valueFieldPattern);
		} catch (Exception e) {
			throw new IOException("Invalid regex pattern for value field name:" + e.getMessage());
		}
		for (String entry : getSeriesKeys()) {
			if (p.matcher(entry).matches()) {
				keySet.add(entry);
			}
		}
		try {
			p = Pattern.compile(tmpOriginal);
		} catch (Exception e) {
			throw new IOException("Invalid regex pattern for value field name:" + e.getMessage());
		}
		List<List<String>> tagList = new ArrayList<>();
		for (String entry : keySet) {
			String[] keys = entry.split(FIELD_TAG_SEPARATOR);
			if (!p.matcher(keys[0]).matches()) {
				continue;
			}
			List<String> tags = decodeStringToTags(getTagIndex(), keys[1]);
			tagList.add(tags);
		}
		return tagList;
	}

	public default Set<String> getTagFilteredRowKeys(String valueFieldNamePattern, Filter<List<String>> tagFilterTree,
			List<String> rawTags) throws IOException {
		Pattern p = null;
		try {
			p = Pattern.compile(valueFieldNamePattern);
		} catch (Exception e) {
			throw new IOException("Invalid regex for value field name:" + e.getMessage());
		}
		Set<String> filteredSeries = getSeriesIdsWhereTags(rawTags);
		for (Iterator<String> iterator = filteredSeries.iterator(); iterator.hasNext();) {
			String rowKey = iterator.next();
			String[] keys = rowKey.split(FIELD_TAG_SEPARATOR);
			if (keys.length != 2) {
				// field encoding
				getLogger().severe("Invalid series tag encode, series ingested without tag field encoding");
				iterator.remove();
				continue;
			}
			if (!p.matcher(keys[0]).matches()) {
				iterator.remove();
				continue;
			}
			List<String> seriesTags = null;
			if (keys.length > 1) {
				seriesTags = decodeStringToTags(getTagIndex(), keys[1]);
			} else {
				seriesTags = new ArrayList<>();
			}
			if (!tagFilterTree.isRetain(seriesTags)) {
				iterator.remove();
			}
		}
		return filteredSeries;
	}

	public default void collectGarbage() throws IOException {
		runOptimizationOperation("garbage collection", ts -> {
			try {
				return ts.collectGarbage();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public default void compact() throws IOException {
		runOptimizationOperation("compacting", ts -> {
			try {
				return ts.compact();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public default void runOptimizationOperation(String operation, Function<TimeSeries, List<Writer>> op)
			throws IOException {
		getLock().lock();
		try {
			Set<String> cleanupList = new HashSet<>();
			for (TimeSeries entry : getTimeSeries()) {
				try {
					List<Writer> garbage = op.apply(entry);
					for (Writer timeSeriesBucket : garbage) {
						getLogger().fine("Buffers " + operation + " for bucket:" + entry.getSeriesId() + "\tOffset:"
								+ timeSeriesBucket.currentOffset());
					}
					getLogger().fine("Buffers " + operation + " for time series:" + entry.getSeriesId());
				} catch (Exception e) {
					getLogger().log(Level.SEVERE, "Error collecing " + operation, e);
				}
			}
			// cleanup these buffer ids
			if (cleanupList.size() > 0) {
				getLogger().info("For measurement:" + getMeasurementName() + " buffers compacted for:"
						+ cleanupList.size() + " buffers");
			}
			cleanupBufferIds(cleanupList);
		} finally {
			getLock().unlock();
		}
	}

	public void cleanupBufferIds(Set<String> cleanupList) throws IOException;

	public default TimeSeries getTimeSeries(String valueFieldName, List<String> tags) throws IOException {
		Collections.sort(tags);
		String rowKey = constructSeriesId(valueFieldName, tags, getTagIndex());
		// check and create timeseries
		TimeSeries timeSeries = getSeriesFromKey(rowKey);
		return timeSeries;
	}

	public default Set<String> getFieldsForMeasurement() {
		Set<String> results = new HashSet<>();
		Set<String> keySet = getSeriesKeys();
		for (String key : keySet) {
			String[] splits = key.split(FIELD_TAG_SEPARATOR);
			if (splits.length == 2) {
				results.add(splits[0]);
			}
		}
		return results;
	}

	public default Set<String> getSeriesIdsWhereTags(List<String> tags) throws IOException {
		Set<String> series = new HashSet<>();
		for (String tag : tags) {
			Collection<String> keys = getTagIndex().searchRowKeysForTag(tag);
			if (keys != null) {
				series.addAll(keys);
			}
		}
		return series;
	}

	public default void queryDataPoints(String valueFieldNamePattern, long startTime, long endTime,
			List<String> tagList, Filter<List<String>> tagFilter, Predicate valuePredicate,
			AggregationFunction aggregationFunction, Set<SeriesQueryOutput> resultMap) throws IOException {
		Set<String> rowKeys = null;
		final Pattern p;
		try {
			p = Pattern.compile(valueFieldNamePattern);
		} catch (Exception e) {
			throw new IOException("Invalid regex for value field name:" + e.getMessage());
		}
		if (tagList == null || tagList.size() == 0) {
			rowKeys = getSeriesKeys();
		} else {
			rowKeys = getTagFilteredRowKeys(valueFieldNamePattern, tagFilter, tagList);
		}
		Stream<String> stream = rowKeys.stream();
		if (useQueryPool()) {
			stream = stream.parallel();
		}
		stream.forEach(entry -> {
			try {
				populateDataPoints(entry, startTime, endTime, aggregationFunction, valuePredicate, p, resultMap);
			} catch (Exception e) {
				e.printStackTrace();
				getLogger().severe("Failed to query data points");
			}
		});
	}

	public default void populateDataPoints(String entry, long startTime, long endTime,
			AggregationFunction aggregationFunction, Predicate valuePredicate, Pattern p,
			Set<SeriesQueryOutput> resultMap) throws IOException {
		String[] keys = entry.split(FIELD_TAG_SEPARATOR);
		if (!p.matcher(keys[0]).matches()) {
			return;
		}
		List<DataPoint> points = null;
		List<String> seriesTags = null;
		if (keys.length > 1) {
			seriesTags = decodeStringToTags(getTagIndex(), keys[1]);
		} else {
			seriesTags = new ArrayList<>();
		}
		TimeSeries value = getSeriesFromKey(entry);
		if (value == null) {
			getLogger().severe("Invalid time series value " + entry + "\t" + "\t" + "\n\n");
			return;
		}
		points = value.queryDataPoints(keys[0], seriesTags, startTime, endTime, valuePredicate);
		if (aggregationFunction != null) {
			points = aggregationFunction.aggregateDataPoints(points);
			// points = aggregationFunction.aggregatePoints(points, value.isFp());
		}
		if (points == null) {
			points = new ArrayList<>();
		}
		if (points.size() > 0) {
			SeriesQueryOutput seriesQueryOutput = new SeriesQueryOutput(getMeasurementName(), keys[0], seriesTags);
			seriesQueryOutput.setFp(value.isFp());
			seriesQueryOutput.setDataPoints(points);
			resultMap.add(seriesQueryOutput);
		}
	}

	public default void queryPoints(String valueFieldNamePattern, long startTime, long endTime, List<String> tagList,
			Filter<List<String>> tagFilter, Predicate valuePredicate, AggregationFunction aggregationFunction,
			Set<SeriesQueryOutput> resultMap) throws IOException {
		Set<String> rowKeys = null;
		Pattern fieldNamePattern;
		try {
			fieldNamePattern = Pattern.compile(valueFieldNamePattern);
		} catch (Exception e) {
			throw new IOException("Invalid regex for value field name:" + e.getMessage());
		}
		if (tagList == null || tagList.size() == 0) {
			rowKeys = getSeriesKeys();
		} else {
			rowKeys = getTagFilteredRowKeys(valueFieldNamePattern, tagFilter, tagList);
		}
		Stream<String> stream = rowKeys.stream();
		if (useQueryPool()) {
			stream = stream.parallel();
		}
		stream.forEach(entry -> {
			try {
				populatePoints(entry, startTime, endTime, aggregationFunction, valuePredicate, fieldNamePattern,
						resultMap);
			} catch (IOException e) {
				getLogger().severe("Failed to query data points");
			}
		});
	}

	public default void populatePoints(String entry, long startTime, long endTime,
			AggregationFunction aggregationFunction, Predicate valuePredicate, Pattern p,
			Set<SeriesQueryOutput> resultMap) throws IOException {
		String[] keys = entry.split(FIELD_TAG_SEPARATOR);
		if (!p.matcher(keys[0]).matches()) {
			return;
		}
		List<long[]> points = null;
		List<String> seriesTags = null;
		if (keys.length > 1) {
			seriesTags = decodeStringToTags(getTagIndex(), keys[1]);
		} else {
			seriesTags = new ArrayList<>();
		}
		TimeSeries value = getSeriesFromKey(entry);
		if (value == null) {
			getLogger().severe("Invalid time series value " + entry + "\t" + "\t" + "\n\n");
			return;
		}
		points = value.queryPoints(keys[0], seriesTags, startTime, endTime, valuePredicate);
		if (aggregationFunction != null) {
			points = aggregationFunction.aggregatePoints(points, value.isFp());
		}
		if (points == null) {
			points = new ArrayList<>();
		}
		if (points.size() > 0) {
			SeriesQueryOutput seriesQueryOutput = new SeriesQueryOutput(getMeasurementName(), keys[0], seriesTags);
			seriesQueryOutput.setFp(value.isFp());
			seriesQueryOutput.setPoints(points);
			resultMap.add(seriesQueryOutput);
		}
	}

	public default void queryReaders(String valueFieldName, long startTime, long endTime,
			LinkedHashMap<Reader, Boolean> readers) throws IOException {
		for (String entry : getSeriesKeys()) {
			TimeSeries series = getSeriesFromKey(entry);
			String[] keys = entry.split(FIELD_TAG_SEPARATOR);
			if (keys.length != 2) {
				getLogger().log(Level.SEVERE, "Invalid situation, series ingested without tag");
				// field encoding
				continue;
			}
			if (!keys[0].equals(valueFieldName)) {
				continue;
			}
			List<String> seriesTags = null;
			if (keys.length > 1) {
				seriesTags = decodeStringToTags(getTagIndex(), keys[1]);
			} else {
				seriesTags = new ArrayList<>();
			}
			for (Reader reader : series.queryReader(valueFieldName, seriesTags, startTime, endTime, null)) {
				readers.put(reader, series.isFp());
			}
		}
	}

	public default Collection<String> getTags() throws IOException {
		return getTagIndex().getTags();
	}

	public Logger getLogger();

	public SortedMap<String, List<Writer>> createNewBucketMap(String seriesId);

	public ReentrantLock getLock();

	public boolean useQueryPool();

	public BufferObject createNewBuffer(String seriesId, String tsBucket, int newSize) throws IOException;

}
