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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.aggregators.AggregationFunction;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.compression.Reader;

/**
 * @author ambud
 */
public interface Measurement {

	public static final String FIELD_TAG_SEPARATOR = "#";
	public static final String TAG_SEPARATOR = "_";
	
	public void configure(Map<String, String> conf, String measurementName, String baseIndexDirectory,
			String dataDirectory, DBMetadata metadata,
			ScheduledExecutorService bgTaskPool) throws IOException;

	public Collection<TimeSeries> getTimeSeries();

	public Map<String, TimeSeries> getTimeSeriesMap();

	public TagIndex getTagIndex();

	public void loadTimeseriesFromMeasurements() throws IOException;

	public void delete() throws IOException;

	public ByteBuffer createNewBuffer() throws IOException;

//	public List<ByteBuffer> getBufTracker();

	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp, Map<String, String> conf) throws IOException;

	public static void indexRowKey(TagIndex memTagLookupTable, String rowKey, List<String> tags) throws IOException {
		for (String tag : tags) {
			memTagLookupTable.index(tag, rowKey);
		}
	}
	
	public default String encodeTagsToString(TagIndex tagLookupTable, List<String> tags) throws IOException {
		StringBuilder builder = new StringBuilder(tags.size() * 5);
		builder.append(tagLookupTable.createEntry(tags.get(0)));
		for (int i = 1; i < tags.size(); i++) {
			String tag = tags.get(i);
			builder.append(TAG_SEPARATOR);
			builder.append(tagLookupTable.createEntry(tag));
		}
		return builder.toString();
	}
	
	public default String constructRowKey(String valueFieldName, List<String> tags, TagIndex index)
			throws IOException {
		String encodeTagsToString = encodeTagsToString(index, tags);
		StringBuilder rowKeyBuilder = new StringBuilder(valueFieldName.length() + 1 + encodeTagsToString.length());
		rowKeyBuilder.append(valueFieldName);
		rowKeyBuilder.append(FIELD_TAG_SEPARATOR);
		rowKeyBuilder.append(encodeTagsToString);
		String rowKey = rowKeyBuilder.toString();
		indexRowKey(index, rowKey, tags);
		return rowKey;
	}
	
	public default List<String> decodeStringToTags(TagIndex tagLookupTable, String tagString) {
		List<String> tagList = new ArrayList<>();
		if (tagString == null || tagString.isEmpty()) {
			return tagList;
		}
		for (String tag : tagString.split(TAG_SEPARATOR)) {
			tagList.add(tagLookupTable.getEntry(tag));
		}
		return tagList;
	}

	public String getMeasurementName();
	
	public default List<List<String>> getTagsForMeasurement(String valueFieldName) throws Exception {
		Set<String> keySet = new HashSet<>();
		for (Entry<String, TimeSeries> entry : getTimeSeriesMap().entrySet()) {
			if (entry.getKey().startsWith(valueFieldName)) {
				keySet.add(entry.getKey());
			}
		}
		List<List<String>> tagList = new ArrayList<>();
		for (String entry : keySet) {
			String[] keys = entry.split(FIELD_TAG_SEPARATOR);
			if (!keys[0].equals(valueFieldName)) {
				continue;
			}
			List<String> tags = decodeStringToTags(getTagIndex(), keys[1]);
			tagList.add(tags);
		}
		return tagList;
	}
	
	public default Set<String> getTagFilteredRowKeys(String valueFieldName, Filter<List<String>> tagFilterTree,
			List<String> rawTags) throws IOException {
		Set<String> filteredSeries = getSeriesIdsWhereTags(rawTags);
		for (Iterator<String> iterator = filteredSeries.iterator(); iterator.hasNext();) {
			String rowKey = iterator.next();
			if (!rowKey.startsWith(valueFieldName)) {
				continue;
			}
			String[] keys = rowKey.split(FIELD_TAG_SEPARATOR);
			if (keys.length != 2) {
				// field encoding
				getLogger().severe("Invalid series tag encode, series ingested without tag field encoding");
				iterator.remove();
				continue;
			}
			if (!keys[0].equals(valueFieldName)) {
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
	
	public default void garbageCollector() throws IOException {
		for (Entry<String, TimeSeries> entry : getTimeSeriesMap().entrySet()) {
			try {
				List<TimeSeriesBucket> garbage = entry.getValue().collectGarbage();
				for (TimeSeriesBucket timeSeriesBucket : garbage) {
					timeSeriesBucket.delete();
					getLogger().info("Collecting garbage for bucket:" + entry.getKey());
				}
				getLogger().info("Collecting garbage for time series:" + entry.getKey());
			} catch (IOException e) {
				getLogger().log(Level.SEVERE, "Error collecing garbage", e);
			}
		}
	}
	
	public default TimeSeries getTimeSeries(String valueFieldName, List<String> tags) throws IOException {
		Collections.sort(tags);
		String rowKey = constructRowKey(valueFieldName, tags, getTagIndex());
		// check and create timeseries
		TimeSeries timeSeries = getTimeSeriesMap().get(rowKey);
		return timeSeries;
	}
	
	public default Set<String> getFieldsForMeasurement() {
		Set<String> results = new HashSet<>();
		Set<String> keySet = getTimeSeriesMap().keySet();
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
			Set<String> keys = getTagIndex().searchRowKeysForTag(tag);
			if (keys != null) {
				series.addAll(keys);
			}
		}
		return series;
	}
	
	public default void queryDataPoints(String valueFieldName, long startTime, long endTime, List<String> tagList,
			Filter<List<String>> tagFilter, Predicate valuePredicate, AggregationFunction aggregationFunction,
			Set<SeriesQueryOutput> resultMap) throws IOException {
		Set<String> rowKeys = null;
		if (tagList == null || tagList.size() == 0) {
			rowKeys = getTimeSeriesMap().keySet();
		} else {
			rowKeys = getTagFilteredRowKeys(valueFieldName, tagFilter, tagList);
		}
		for (String entry : rowKeys) {
			TimeSeries value = getTimeSeriesMap().get(entry);
			String[] keys = entry.split(FIELD_TAG_SEPARATOR);
			if (!keys[0].equals(valueFieldName)) {
				continue;
			}
			List<DataPoint> points = null;
			List<String> seriesTags = null;
			if (keys.length > 1) {
				seriesTags = decodeStringToTags(getTagIndex(), keys[1]);
			} else {
				seriesTags = new ArrayList<>();
			}
			points = value.queryDataPoints(keys[0], seriesTags, startTime, endTime, valuePredicate);
			if (aggregationFunction != null) {
				points = aggregationFunction.aggregate(points);
			}
			if (points == null) {
				points = new ArrayList<>();
			}
			if (points.size() > 0) {
				SeriesQueryOutput seriesQueryOutput = new SeriesQueryOutput(getMeasurementName(), keys[0], seriesTags);
				seriesQueryOutput.setDataPoints(points);
				resultMap.add(seriesQueryOutput);
			}
		}
	}

	public default void queryReaders(String valueFieldName, long startTime, long endTime,
			LinkedHashMap<Reader, Boolean> readers) throws IOException {
		for (String entry : getTimeSeriesMap().keySet()) {
			TimeSeries series = getTimeSeriesMap().get(entry);
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
	
	public default Set<String> getTags() {
		return getTagIndex().getTags();
	}
	
	public Logger getLogger();

}
