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
package com.srotya.sidewinder.core.storage.mem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.aggregators.AggregationFunction;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.SeriesQueryOutput;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeriesBucket;
import com.srotya.sidewinder.core.storage.mem.archival.NoneArchiver;
import com.srotya.sidewinder.core.storage.mem.archival.TimeSeriesArchivalObject;

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
 * doesn't run out of memory. <br>
 * <br>
 * 
 * @author ambud
 */
public class MemStorageEngine implements StorageEngine {

	public static final String MEM_COMPRESSION_CLASS = "mem.compression.class";
	public static final int DEFAULT_TIME_BUCKET_CONSTANT = 4096;
	public static final String RETENTION_HOURS = "default.series.retention.hours";
	public static final int DEFAULT_RETENTION_HOURS = (int) Math
			.ceil((((double) DEFAULT_TIME_BUCKET_CONSTANT) * 24 / 60) / 60);
	private static final String FIELD_TAG_SEPARATOR = "#";
	private static final String TAG_SEPARATOR = "_";
	private static final Logger logger = Logger.getLogger(MemStorageEngine.class.getName());
	private static ItemNotFoundException NOT_FOUND_EXCEPTION = new ItemNotFoundException("Item not found");
	private static RejectException FP_MISMATCH_EXCEPTION = new RejectException("Floating point mismatch");
	private static RejectException INVALID_DATAPOINT_EXCEPTION = new RejectException(
			"Datapoint is missing required values");
	private Map<String, Map<String, SortedMap<String, TimeSeries>>> databaseMap;
	private AtomicInteger counter = new AtomicInteger(0);
	private Map<String, Map<String, MemTagIndex>> tagLookupTable;
	private Map<String, Integer> databaseRetentionPolicyMap;
	private int defaultRetentionHours;
	private Archiver archiver;
	private String compressionFQCN;
	private Map<String, String> conf;

	@Override
	public void configure(Map<String, String> conf, ScheduledExecutorService bgTaskPool) throws IOException {
		this.conf = conf;
		this.defaultRetentionHours = Integer
				.parseInt(conf.getOrDefault(RETENTION_HOURS, String.valueOf(DEFAULT_RETENTION_HOURS)));
		logger.info("Setting default timeseries retention hours policy to:" + defaultRetentionHours);
		tagLookupTable = new ConcurrentHashMap<>();
		databaseMap = new ConcurrentHashMap<>();
		databaseRetentionPolicyMap = new ConcurrentHashMap<>();
		try {
			archiver = (Archiver) Class.forName(conf.getOrDefault("archiver.class", NoneArchiver.class.getName()))
					.newInstance();
			archiver.init(conf);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		conf.put(StorageEngine.PERSISTENCE_DISK, "false");
		compressionFQCN = conf.getOrDefault(MEM_COMPRESSION_CLASS,
				// "com.srotya.sidewinder.core.storage.compression.dod.DodWriter");
				"com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter");
		if (bgTaskPool != null) {
			bgTaskPool.scheduleAtFixedRate(() -> {
				for (Entry<String, Map<String, SortedMap<String, TimeSeries>>> measurementMap : databaseMap
						.entrySet()) {
					String db = measurementMap.getKey();
					for (Entry<String, SortedMap<String, TimeSeries>> measurementEntry : measurementMap.getValue()
							.entrySet()) {
						String measurement = measurementEntry.getKey();
						for (Entry<String, TimeSeries> entry : measurementEntry.getValue().entrySet()) {
							List<TimeSeriesBucket> buckets;
							try {
								buckets = entry.getValue().collectGarbage();
							} catch (IOException e1) {
								continue;
							}
							for (TimeSeriesBucket bucket : buckets) {
								try {
									archiver.archive(
											new TimeSeriesArchivalObject(db, measurement, entry.getKey(), bucket));
								} catch (ArchiveException e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			}, 500, 60, TimeUnit.SECONDS);
		}
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

	/**
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws Exception
	 */
	@Override
	public LinkedHashMap<Reader, Boolean> queryReaders(String dbName, String measurementName, String valueFieldName,
			long startTime, long endTime) throws Exception {
		LinkedHashMap<Reader, Boolean> readers = new LinkedHashMap<>();
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
					List<String> seriesTags = null;
					if (keys.length > 1) {
						seriesTags = decodeStringToTags(memTagIndex, keys[1]);
					} else {
						seriesTags = new ArrayList<>();
					}
					for (Reader reader : series.queryReader(valueFieldName, seriesTags, startTime, endTime, null)) {
						readers.put(reader, series.isFp());
					}
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
	public Set<SeriesQueryOutput> queryDataPoints(String dbName, String measurementName, String valueFieldName,
			long startTime, long endTime, List<String> tagList, Filter<List<String>> tagFilter,
			Predicate valuePredicate, AggregationFunction aggregationFunction) throws ItemNotFoundException {
		Set<SeriesQueryOutput> resultMap = new HashSet<>();
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
			if (seriesMap != null) {
				MemTagIndex memTagIndex = getOrCreateMemTagIndex(dbName, measurementName);
				Set<String> rowKeys = null;
				if (tagList == null || tagList.size() == 0) {
					rowKeys = seriesMap.keySet();
				} else {
					rowKeys = getTagFilteredRowKeys(dbName, measurementName, valueFieldName, tagFilter, tagList);
				}
				for (String entry : rowKeys) {
					TimeSeries value = seriesMap.get(entry);
					String[] keys = entry.split(FIELD_TAG_SEPARATOR);
					if (!keys[0].equals(valueFieldName)) {
						continue;
					}
					List<DataPoint> points = null;
					List<String> seriesTags = null;
					if (keys.length > 1) {
						seriesTags = decodeStringToTags(memTagIndex, keys[1]);
					} else {
						seriesTags = new ArrayList<>();
					}
					try {
						points = value.queryDataPoints(keys[0], seriesTags, startTime, endTime, valuePredicate);
					} catch (IOException e) {
						logger.log(Level.SEVERE, "Error querying datapoints", e);
					}
					if (aggregationFunction != null) {
						points = aggregationFunction.aggregate(points);
					}
					if (points == null) {
						points = new ArrayList<>();
					}
					if (points.size() > 0) {
						resultMap.add(new SeriesQueryOutput(measurementName, keys[0], seriesTags, points));
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

	@Override
	public Set<String> getMeasurementsLike(String dbName, String partialMeasurementName) throws Exception {
		if (!checkIfExists(dbName)) {
			throw NOT_FOUND_EXCEPTION;
		}
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

	public static void validateDataPoint(String dbName, String measurementName, String valueFieldName,
			List<String> tags, TimeUnit unit) throws RejectException {
		if (dbName == null || measurementName == null || valueFieldName == null || tags == null || unit == null) {
			throw INVALID_DATAPOINT_EXCEPTION;
		}
	}

	@Override
	public void writeDataPoint(DataPoint dp) throws IOException {
		validateDataPoint(dp.getDbName(), dp.getMeasurementName(), dp.getValueFieldName(), dp.getTags(),
				TimeUnit.MILLISECONDS);
		TimeSeries timeSeries = getOrCreateTimeSeries(dp.getDbName(), dp.getMeasurementName(), dp.getValueFieldName(),
				dp.getTags(), DEFAULT_TIME_BUCKET_CONSTANT, dp.isFp());
		if (dp.isFp() != timeSeries.isFp()) {
			// drop this datapoint, mixed series are not allowed
			throw FP_MISMATCH_EXCEPTION;
		}
		if (dp.isFp()) {
			timeSeries.addDataPoint(TimeUnit.MILLISECONDS, dp.getTimestamp(), dp.getValue());
		} else {
			timeSeries.addDataPoint(TimeUnit.MILLISECONDS, dp.getTimestamp(), dp.getLongValue());
		}
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
		if (tagString == null || tagString.isEmpty()) {
			return tagList;
		}
		for (String tag : tagString.split(TAG_SEPARATOR)) {
			tagList.add(tagLookupTable.getEntry(tag));
		}
		return tagList;
	}

	@Override
	public Map<String, SortedMap<String, TimeSeries>> getOrCreateDatabase(String dbName) {
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			synchronized (databaseMap) {
				if ((measurementMap = databaseMap.get(dbName)) == null) {
					measurementMap = new ConcurrentHashMap<>();
					databaseMap.put(dbName, measurementMap);
					databaseRetentionPolicyMap.put(dbName, defaultRetentionHours);
					logger.info("Created new database:" + dbName + "\t with retention period:" + defaultRetentionHours
							+ " hours");
				}
			}
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
		return getOrCreateMeasurement(measurementMap, measurementName);
	}

	protected SortedMap<String, TimeSeries> getOrCreateMeasurement(
			Map<String, SortedMap<String, TimeSeries>> measurementMap, String measurementName) {
		SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
		if (seriesMap == null) {
			synchronized (measurementMap) {
				if ((seriesMap = measurementMap.get(measurementName)) == null) {
					seriesMap = new ConcurrentSkipListMap<>();
					measurementMap.put(measurementName, seriesMap);
					logger.info("Created new measurement:" + measurementName);
				}
			}
		}
		return seriesMap;
	}

	@Override
	public boolean checkTimeSeriesExists(String dbName, String measurementName, String valueFieldName,
			List<String> tags) throws Exception {
		Collections.sort(tags);

		if (!checkIfExists(dbName)) {
			return false;
		}
		// check and create database map
		Map<String, SortedMap<String, TimeSeries>> dbMap = getOrCreateDatabase(dbName);

		if (!checkIfExists(dbName, measurementName)) {
			return false;
		}
		// check and create measurement map
		SortedMap<String, TimeSeries> measurementMap = getOrCreateMeasurement(dbMap, measurementName);

		String rowKey = constructRowKey(dbName, measurementName, valueFieldName, tags);
		// check and create timeseries
		TimeSeries timeSeries = measurementMap.get(rowKey);

		return timeSeries != null;
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
			synchronized (measurementMap) {
				if ((timeSeries = measurementMap.get(rowKey)) == null) {
					timeSeries = new TimeSeries(compressionFQCN, measurementName + "_" + rowKey,
							databaseRetentionPolicyMap.get(dbName), timeBucketSize, fp, conf);
					measurementMap.put(rowKey, timeSeries);
					logger.fine("Created new timeseries:" + timeSeries + " for measurement:" + measurementName + "\t"
							+ rowKey + "\t" + databaseRetentionPolicyMap.get(dbName));
				}
			}
		}
		return timeSeries;
	}

	@Override
	public TimeSeries getTimeSeries(String dbName, String measurementName, String valueFieldName, List<String> tags) {
		Collections.sort(tags);

		String rowKey = constructRowKey(dbName, measurementName, valueFieldName, tags);

		// check and create database map
		Map<String, SortedMap<String, TimeSeries>> dbMap = getOrCreateDatabase(dbName);

		// check and create measurement map
		SortedMap<String, TimeSeries> measurementMap = getOrCreateMeasurement(dbMap, measurementName);

		// check and create timeseries
		return measurementMap.get(rowKey);
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

	protected String constructRowKey(String dbName, String measurementName, String valueFieldName, List<String> tags) {
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

	public Set<String> getSeriesIdsWhereTags(String dbName, String measurementName, List<String> tags) {
		Set<String> series = new HashSet<>();
		MemTagIndex memTagLookupTable = getOrCreateMemTagIndex(dbName, measurementName);
		for (String tag : tags) {
			Set<String> keys = memTagLookupTable.searchRowKeysForTag(tag);
			if (keys != null) {
				series.addAll(keys);
			}
		}
		return series;
	}

	public Set<String> getTagFilteredRowKeys(String dbName, String measurementName, String valueFieldName,
			Filter<List<String>> tagFilterTree, List<String> rawTags) {
		Set<String> filteredSeries = getSeriesIdsWhereTags(dbName, measurementName, rawTags);
		for (Iterator<String> iterator = filteredSeries.iterator(); iterator.hasNext();) {
			String rowKey = iterator.next();
			if (!rowKey.startsWith(valueFieldName)) {
				continue;
			}
			String[] keys = rowKey.split(FIELD_TAG_SEPARATOR);
			if (keys.length != 2) {
				// TODO report major error, series ingested without tag
				// field encoding
				logger.severe("Invalid series tag encode, series ingested without tag field encoding");
				iterator.remove();
				continue;
			}
			if (!keys[0].equals(valueFieldName)) {
				iterator.remove();
				continue;
			}
			List<String> seriesTags = null;
			if (keys.length > 1) {
				seriesTags = decodeStringToTags(getOrCreateMemTagIndex(dbName, measurementName), keys[1]);
			} else {
				seriesTags = new ArrayList<>();
			}
			if (!tagFilterTree.isRetain(seriesTags)) {
				iterator.remove();
			}
		}
		return filteredSeries;
	}

	protected void indexRowKey(MemTagIndex memTagLookupTable, String rowKey, List<String> tags) {
		for (String tag : tags) {
			memTagLookupTable.index(tag, rowKey);
		}
	}

	protected MemTagIndex getOrCreateMemTagIndex(String dbName, String measurementName) {
		Map<String, MemTagIndex> lookupMap = tagLookupTable.get(dbName);
		if (lookupMap == null) {
			synchronized (tagLookupTable) {
				if ((lookupMap = tagLookupTable.get(dbName)) == null) {
					lookupMap = new ConcurrentHashMap<>();
					tagLookupTable.put(dbName, lookupMap);
				}
			}
		}

		MemTagIndex memTagLookupTable = lookupMap.get(measurementName);
		if (memTagLookupTable == null) {
			synchronized (lookupMap) {
				if ((memTagLookupTable = lookupMap.get(measurementName)) == null) {
					memTagLookupTable = new MemTagIndex();
					lookupMap.put(measurementName, memTagLookupTable);
				}
			}
		}
		return memTagLookupTable;
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
	public List<List<String>> getTagsForMeasurement(String dbName, String measurementName, String valueFieldName)
			throws Exception {
		Map<String, MemTagIndex> measurementMap = tagLookupTable.get(dbName);
		if (measurementMap == null) {
			throw new ItemNotFoundException("Database " + dbName + " not found");
		}
		MemTagIndex memTagLookupTable = measurementMap.get(measurementName);
		if (memTagLookupTable == null) {
			throw new ItemNotFoundException("Measurement " + measurementName + " not found");
		}
		SortedMap<String, TimeSeries> subMap = databaseMap.get(dbName).get(measurementMap).subMap(valueFieldName,
				valueFieldName + Character.MAX_VALUE);
		List<List<String>> tagList = new ArrayList<>();
		for (Entry<String, TimeSeries> entry : subMap.entrySet()) {
			String[] keys = entry.getKey().split(FIELD_TAG_SEPARATOR);
			if (!keys[0].equals(valueFieldName)) {
				continue;
			}
			List<String> tags = decodeStringToTags(memTagLookupTable, keys[1]);
			tagList.add(tags);
		}
		return tagList;
	}

	@Override
	public Set<String> getFieldsForMeasurement(String dbName, String measurementName) throws Exception {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
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

	/*
	 * @Override public void writeDataPoint(String dbName, String
	 * measurementName, String valueFieldName, List<String> tags, TimeUnit unit,
	 * long timestamp, long value) throws IOException {
	 * validateDataPoint(dbName, measurementName, valueFieldName, tags, unit);
	 * TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName,
	 * valueFieldName, tags, DEFAULT_TIME_BUCKET_CONSTANT, false);
	 * timeSeries.addDataPoint(unit, timestamp, value);
	 * counter.incrementAndGet(); }
	 * 
	 * @Override public void writeDataPoint(String dbName, String
	 * measurementName, String valueFieldName, List<String> tags, TimeUnit unit,
	 * long timestamp, double value) throws IOException {
	 * validateDataPoint(dbName, measurementName, valueFieldName, tags, unit);
	 * TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName,
	 * valueFieldName, tags, DEFAULT_TIME_BUCKET_CONSTANT, true);
	 * timeSeries.addDataPoint(unit, timestamp, value);
	 * counter.incrementAndGet(); }
	 */

}
