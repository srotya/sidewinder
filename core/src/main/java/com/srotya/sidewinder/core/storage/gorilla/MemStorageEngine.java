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
package com.srotya.sidewinder.core.storage.gorilla;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.Callback;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

/**
 * In-memory Timeseries {@link StorageEngine} implementation that uses the
 * following hierarchy:
 * <ul>
 * <li>Database
 * <ul>
 * <li>Measurement
 * <ul>
 * <li>Time Series</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * </ul>
 * 
 * {@link TimeSeriesBucket} is uses compressed in-memory representation of the
 * actual data. Periodic checks against size ensure that Sidewinder server
 * doesn't run out of memory. Each timeseries has a <br>
 * <br>
 * 
 * 
 * 
 * @author ambud
 */
public class MemStorageEngine implements StorageEngine {

	public static final int DEFAULT_TIME_BUCKET_CONSTANT = 4096;
	public static final String RETENTION_HOURS = "default.series.retention.hours";
	public static final int DEFAULT_RETENTION_HOURS = (int) Math
			.ceil((((double) DEFAULT_TIME_BUCKET_CONSTANT) * 24 / 60) / 60);
	private static final String FIELD_TAG_SEPARATOR = "#";
	private static final String TAG_SEPARATOR = "_";
	private static final Logger logger = Logger.getLogger(MemStorageEngine.class.getName());
	private static RejectException NOT_FOUND_EXCEPTION = new RejectException("Item not found");
	private static RejectException INVALID_DATAPOINT_EXCEPTION = new RejectException("Floating point mismatch");
	private Map<String, Map<String, SortedMap<String, TimeSeries>>> databaseMap;
	private AtomicInteger counter = new AtomicInteger(0);
	private Map<String, Map<String, MemTagIndex>> tagLookupTable;
	private Map<String, Integer> databaseRetentionPolicyMap;
	private int defaultRetentionHours;

	@Override
	public void configure(Map<String, String> conf) throws IOException {
		this.defaultRetentionHours = Integer
				.parseInt(conf.getOrDefault(RETENTION_HOURS, String.valueOf(DEFAULT_RETENTION_HOURS)));
		logger.info("Setting default timeseries retention hours policy to:" + defaultRetentionHours);
		tagLookupTable = new ConcurrentHashMap<>();
		databaseMap = new ConcurrentHashMap<>();
		databaseRetentionPolicyMap = new ConcurrentHashMap<>();
		Executors.newSingleThreadScheduledExecutor(new BackgrounThreadFactory()).scheduleAtFixedRate(() -> {
			for (Map<String, SortedMap<String, TimeSeries>> map : databaseMap.values()) {
				for (SortedMap<String, TimeSeries> sortedMap : map.values()) {
					for (TimeSeries timeSeries : sortedMap.values()) {
						timeSeries.collectGarbage();
					}
				}
			}
		}, 0, 60, TimeUnit.SECONDS);
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int retentionHours) {
		TimeSeries series = getOrCreateTimeSeries(dbName, measurementName, valueFieldName, tags,
				DEFAULT_TIME_BUCKET_CONSTANT, true);
		series.setRetentionHours(retentionHours);
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, int retentionHours) {
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
			if (seriesMap != null) {
				for (TimeSeries series : seriesMap.values()) {
					series.setRetentionHours(retentionHours);
				}
			}
		}
	}

	@Override
	public void updateDefaultTimeSeriesRetentionPolicy(String dbName, int retentionHours) {
		databaseRetentionPolicyMap.put(dbName, retentionHours);
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, int retentionHours) {
		databaseRetentionPolicyMap.put(dbName, retentionHours);
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			for (SortedMap<String, TimeSeries> sortedMap : measurementMap.values()) {
				for (TimeSeries timeSeries : sortedMap.values()) {
					timeSeries.setRetentionHours(retentionHours);
				}
			}
		}
	}
	
	public List<Reader> queryReaders(String dbName, String measurementName, String valueFieldName, long startTime, long endTime) throws Exception {
		List<Reader> readers = new ArrayList<>();
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
			if (seriesMap != null) {
				MemTagIndex memTagIndex = getOrCreateMemTagIndex(dbName, measurementName);
				for (String entry : seriesMap.keySet()) {
					TimeSeries series = seriesMap.get(entry);
					String[] keys = entry.split(FIELD_TAG_SEPARATOR);
					if (keys.length != 2) {
						// TODO report major error, series ingested without tag
						// field encoding
						continue;
					}
					if (!keys[0].equals(valueFieldName)) {
						continue;
					}
					List<String> seriesTags = decodeStringToTags(memTagIndex, keys[1]);
					readers.addAll(series.queryReader(valueFieldName, seriesTags, startTime, endTime, null));
				}
			} else {
				throw new ItemNotFoundException("Measurement " + measurementName + " not found");
			}
		} else {
			throw new ItemNotFoundException("Database " + dbName + " not found");
		}
		return readers;
	}

	@Override
	public Map<String, List<DataPoint>> queryDataPoints(String dbName, String measurementName, String valueFieldName,
			long startTime, long endTime, List<String> tags, Predicate valuePredicate) throws ItemNotFoundException {
		Map<String, List<DataPoint>> resultMap = new HashMap<>();
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
			if (seriesMap != null) {
				MemTagIndex memTagIndex = getOrCreateMemTagIndex(dbName, measurementName);
				Set<String> rowKeys = new HashSet<>();
				if (tags != null && tags.size() > 0) {
					for (String tag : tags) {
						Set<String> temp = memTagIndex.searchRowKeysForTag(tag);
						rowKeys.addAll(temp);
					}
				} else {
					rowKeys.addAll(seriesMap.keySet());
				}
				for (String entry : rowKeys) {
					TimeSeries value = seriesMap.get(entry);
					String[] keys = entry.split(FIELD_TAG_SEPARATOR);
					if (keys.length != 2) {
						// TODO report major error, series ingested without tag
						// field encoding
						continue;
					}
					if (!keys[0].equals(valueFieldName)) {
						continue;
					}
					List<DataPoint> points = new ArrayList<>();
					List<String> seriesTags = decodeStringToTags(memTagIndex, keys[1]);
					points.addAll(value.queryDataPoints(keys[0], seriesTags, startTime, endTime, valuePredicate));
					if (points.size() > 0) {
						resultMap.put(measurementName + "-" + keys[0] + tagToString(seriesTags), points);
					}
				}
			} else {
				throw new ItemNotFoundException("Measurement " + measurementName + " not found");
			}
		} else {
			throw new ItemNotFoundException("Database " + dbName + " not found");
		}
		return resultMap;
	}

	public static String tagToString(List<String> tags) {
		StringBuilder builder = new StringBuilder();
		for (String tag : tags) {
			builder.append("/");
			builder.append(tag);
		}
		return builder.toString();
	}

	@Override
	public Set<String> getMeasurementsLike(String dbName, String partialMeasurementName) throws IOException {
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		partialMeasurementName = partialMeasurementName.trim();
		if (partialMeasurementName.isEmpty()) {
			return measurementMap.keySet();
		} else {
			Set<String> filteredSeries = new HashSet<>();
			for (String measurementName : measurementMap.keySet()) {
				if (measurementName.contains(partialMeasurementName)) {
					filteredSeries.add(measurementName);
				}
			}
			return filteredSeries;
		}
	}

	@Override
	public void writeDataPoint(String dbName, DataPoint dp) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, dp.getMeasurementName(), dp.getValueFieldName(),
				dp.getTags(), DEFAULT_TIME_BUCKET_CONSTANT, dp.isFp());
		if (dp.isFp() != timeSeries.isFp()) {
			// drop this datapoint, mixed series are not allowed
			throw INVALID_DATAPOINT_EXCEPTION;
		}
		if (dp.isFp()) {
			timeSeries.addDataPoint(TimeUnit.MILLISECONDS, dp.getTimestamp(), dp.getValue());
		} else {
			timeSeries.addDataPoint(TimeUnit.MILLISECONDS, dp.getTimestamp(), dp.getLongValue());
		}
		counter.incrementAndGet();
	}

	@Override
	public void writeSeries(String dbName, String measurementName, String valueFieldName, List<String> tags,
			TimeUnit unit, long timestamp, long value, Callback callback) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName, valueFieldName, tags,
				DEFAULT_TIME_BUCKET_CONSTANT, false);
		timeSeries.addDataPoint(unit, timestamp, value);
		counter.incrementAndGet();
	}

	public static String encodeTagsToString(MemTagIndex tagLookupTable, List<String> tags) {
		StringBuilder builder = new StringBuilder(tags.size() * 5);
		for (String tag : tags) {
			builder.append(tagLookupTable.createEntry(tag));
			builder.append(TAG_SEPARATOR);
		}
		return builder.toString();
	}

	public static List<String> decodeStringToTags(MemTagIndex tagLookupTable, String tagString) {
		List<String> tagList = new ArrayList<>();
		for (String tag : tagString.split(TAG_SEPARATOR)) {
			tagList.add(tagLookupTable.getEntry(tag));
		}
		return tagList;
	}

	@Override
	public Map<String, SortedMap<String, TimeSeries>> getOrCreateDatabase(String dbName) {
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			measurementMap = new ConcurrentSkipListMap<>();
			databaseMap.put(dbName, measurementMap);
			databaseRetentionPolicyMap.put(dbName, defaultRetentionHours);
			logger.info("Created new database:" + dbName + "\t" + defaultRetentionHours);
		}
		return measurementMap;
	}

	@Override
	public Map<String, SortedMap<String, TimeSeries>> getOrCreateDatabase(String dbName, int retentionPolicy) {
		Map<String, SortedMap<String, TimeSeries>> map = getOrCreateDatabase(dbName);
		updateTimeSeriesRetentionPolicy(dbName, retentionPolicy);
		return map;
	}

	@Override
	public SortedMap<String, TimeSeries> getOrCreateMeasurement(String dbName, String measurementName) {
		Map<String, SortedMap<String, TimeSeries>> measurementMap = getOrCreateDatabase(dbName);
		SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
		if (seriesMap == null) {
			seriesMap = new ConcurrentSkipListMap<>();
			measurementMap.put(measurementName, seriesMap);
			logger.fine("Created new measurement:" + measurementName);
		}
		return seriesMap;
	}

	protected SortedMap<String, TimeSeries> getOrCreateMeasurement(
			Map<String, SortedMap<String, TimeSeries>> measurementMap, String measurementName) {
		SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
		if (seriesMap == null) {
			seriesMap = new ConcurrentSkipListMap<>();
			measurementMap.put(measurementName, seriesMap);
			logger.fine("Created new measurement:" + measurementName);
		}
		return seriesMap;
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int timeBucketSize, boolean fp) {
		Collections.sort(tags);

		String rowKey = constructRowKey(dbName, measurementName, valueFieldName, tags);

		// check and create database map
		Map<String, SortedMap<String, TimeSeries>> dbMap = getOrCreateDatabase(dbName);

		// check and create measurement map
		SortedMap<String, TimeSeries> measurementMap = getOrCreateMeasurement(dbMap, measurementName);

		// check and create timeseries
		TimeSeries timeSeries = measurementMap.get(rowKey);
		if (timeSeries == null) {
			timeSeries = new TimeSeries(measurementName + "_" + rowKey, databaseRetentionPolicyMap.get(dbName),
					timeBucketSize, fp);
			measurementMap.put(rowKey, timeSeries);
			logger.fine("Created new timeseries:" + timeSeries + " for measurement:" + measurementName + "\t" + rowKey
					+ "\t" + databaseRetentionPolicyMap.get(dbName));
		}
		return timeSeries;
	}

	@Override
	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName)
			throws RejectException {
		Map<String, SortedMap<String, TimeSeries>> dbMap = getOrCreateDatabase(dbName);

		// check and create measurement map
		SortedMap<String, TimeSeries> measurementMap = getOrCreateMeasurement(dbMap, measurementName);

		SortedMap<String, TimeSeries> subMap = measurementMap.subMap(valueFieldName,
				valueFieldName + Character.MAX_VALUE);

		if (!subMap.isEmpty()) {
			return subMap.values().iterator().next().isFp();
		} else {
			throw NOT_FOUND_EXCEPTION;
		}
	}

	public String constructRowKey(String dbName, String measurementName, String valueFieldName, List<String> tags) {
		MemTagIndex memTagLookupTable = getOrCreateMemTagIndex(dbName, measurementName);
		String encodeTagsToString = encodeTagsToString(memTagLookupTable, tags);
		StringBuilder rowKeyBuilder = new StringBuilder(valueFieldName.length() + 1 + encodeTagsToString.length());
		rowKeyBuilder.append(valueFieldName);
		rowKeyBuilder.append(FIELD_TAG_SEPARATOR);
		rowKeyBuilder.append(encodeTagsToString);
		String rowKey = rowKeyBuilder.toString();
		indexRowKey(memTagLookupTable, rowKey, tags);
		return rowKey;
	}

	protected void indexRowKey(MemTagIndex memTagLookupTable, String rowKey, List<String> tags) {
		for (String tag : tags) {
			memTagLookupTable.index(tag, rowKey);
		}
	}

	private MemTagIndex getOrCreateMemTagIndex(String dbName, String measurementName) {
		Map<String, MemTagIndex> lookupMap = tagLookupTable.get(dbName);
		if (lookupMap == null) {
			lookupMap = new ConcurrentHashMap<>();
			tagLookupTable.put(dbName, lookupMap);
		}

		MemTagIndex memTagLookupTable = lookupMap.get(measurementName);
		if (memTagLookupTable == null) {
			memTagLookupTable = new MemTagIndex();
			lookupMap.put(measurementName, memTagLookupTable);
		}
		return memTagLookupTable;
	}

	@Override
	public void writeSeries(String dbName, String measurementName, String valueFieldName, List<String> tags,
			TimeUnit unit, long timestamp, double value, Callback callback) throws IOException {
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName, valueFieldName, tags,
				DEFAULT_TIME_BUCKET_CONSTANT, true);
		timeSeries.addDataPoint(unit, timestamp, value);
		counter.incrementAndGet();
	}

	@Override
	public Set<String> getDatabases() throws Exception {
		return databaseMap.keySet();
	}

	@Override
	public Set<String> getAllMeasurementsForDb(String dbName) throws Exception {
		if (checkIfExists(dbName)) {
			return databaseMap.get(dbName).keySet();
		} else {
			throw NOT_FOUND_EXCEPTION;
		}
	}

	@Override
	public void deleteAllData() throws Exception {
		// Extremely dangerous operation
		databaseMap.clear();
	}

	@Override
	public boolean checkIfExists(String dbName) throws Exception {
		return databaseMap.containsKey(dbName);
	}

	@Override
	public void dropDatabase(String dbName) throws Exception {
		databaseMap.remove(dbName);
	}

	@Override
	public void dropMeasurement(String dbName, String measurementName) throws Exception {
		databaseMap.get(dbName).remove(measurementName);
	}

	/**
	 * Function for unit testing
	 * 
	 * @param dbName
	 * @param measurementName
	 * @return
	 */
	protected SortedMap<String, TimeSeries> getSeriesMap(String dbName, String measurementName) {
		return databaseMap.get(dbName).get(measurementName);
	}

	@Override
	public void connect() throws IOException {
	}

	@Override
	public void disconnect() throws IOException {
	}

	@Override
	public boolean checkIfExists(String dbName, String measurement) throws Exception {
		if (checkIfExists(dbName)) {
			return databaseMap.get(dbName).containsKey(measurement);
		} else {
			return false;
		}
	}

	@Override
	public Set<String> getTagsForMeasurement(String dbName, String measurementName) throws Exception {
		Map<String, MemTagIndex> measurementMap = tagLookupTable.get(dbName);
		if (measurementMap == null) {
			throw new ItemNotFoundException("Database " + dbName + " not found");
		}
		MemTagIndex memTagLookupTable = measurementMap.get(measurementName);
		if (memTagLookupTable == null) {
			throw new ItemNotFoundException("Measurement " + measurementName + " not found");
		}
		return memTagLookupTable.getTags();
	}

	@Override
	public Set<String> getFieldsForMeasurement(String dbName, String measurementName) throws Exception {
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			throw new ItemNotFoundException("Database " + dbName + " not found");
		}
		SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
		if (seriesMap == null) {
			throw new ItemNotFoundException("Measurement " + measurementName + " not found");
		}
		Set<String> results = new HashSet<>();
		Set<String> keySet = seriesMap.keySet();
		for (String key : keySet) {
			String[] splits = key.split(FIELD_TAG_SEPARATOR);
			if (splits.length == 2) {
				results.add(splits[0]);
			}
		}
		return results;
	}

}
