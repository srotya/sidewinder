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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.codahale.metrics.Counter;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.functions.Function;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.compression.Reader;

/**
 * Interface for Timeseries Storage Engine
 * 
 * @author ambud
 */
public interface StorageEngine {

	public static final ItemNotFoundException NOT_FOUND_EXCEPTION = new ItemNotFoundException("Item not found");
	public static RejectException FP_MISMATCH_EXCEPTION = new RejectException("Floating point mismatch");
	public static RejectException INVALID_DATAPOINT_EXCEPTION = new RejectException(
			"Datapoint is missing required values");
	public static final String DEFAULT_COMPRESSION_CODEC = "byzantine";
	public static final String COMPRESSION_CODEC = "compression.codec";
	public static final String COMPACTION_CODEC = "compaction.codec";
	public static final String DEFAULT_COMPACTION_CODEC = "gorilla";
	public static final int DEFAULT_TIME_BUCKET_CONSTANT = 32768;
	public static final String DEFAULT_BUCKET_SIZE = "default.bucket.size";
	public static final String RETENTION_HOURS = "default.series.retention.hours";
	public static final int DEFAULT_RETENTION_HOURS = (int) Math
			.ceil((((double) DEFAULT_TIME_BUCKET_CONSTANT) * 24 / 60) / 60);
	public static final String PERSISTENCE_DISK = "persistence.disk";
	public static final String ARCHIVER_CLASS = "archiver.class";
	public static final String GC_ENABLED = "gc.enabled";
	public static final String GC_DELAY = "gc.delay";
	public static final String GC_FREQUENCY = "gc.frequency";
	public static final String DEFAULT_GC_FREQUENCY = "60";
	public static final String DEFAULT_GC_DELAY = "60";
	public static final String COMPACTION_ENABLED = "compaction.enabled";
	public static final String DEFAULT_COMPACTION_ENABLED = "false";
	public static final String COMPACTION_FREQUENCY = "compaction.frequency";
	public static final String DEFAULT_COMPACTION_FREQUENCY = "1800";
	public static final String COMPACTION_DELAY = "compaction.delay";
	public static final String DEFAULT_COMPACTION_DELAY = "1800";
	public static final String COMPACTION_ON_START = "compaction.onstart";
	public static final String DEFAULT_COMPACTION_ON_START = "false";
	public static final String COMPACTION_RATIO = "compaction.ratio";
	public static final String DEFAULT_COMPACTION_RATIO = "0.8";

	/**
	 * @param conf
	 * @param bgTaskPool
	 * @throws IOException
	 */
	public void configure(Map<String, String> conf, ScheduledExecutorService bgTaskPool) throws IOException;

	/**
	 * Connect to the storage engine
	 * 
	 * @throws IOException
	 */
	public void connect() throws IOException;

	/**
	 * Disconnect from the storage engine
	 * 
	 * @throws IOException
	 */
	public void disconnect() throws IOException;

	public default void writeDataPoint(String dbName, String measurementName, String valueFieldName, List<Tag> tags,
			long timestamp, long value, boolean fp, boolean preSorted) throws IOException {
		StorageEngine.validateDataPoint(dbName, measurementName, valueFieldName, tags, TimeUnit.MILLISECONDS);
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName, valueFieldName, tags,
				getDefaultTimebucketSize(), fp, preSorted);
		if (timeSeries.isFp() != fp) {
			// drop this datapoint, mixed series are not allowed
			throw FP_MISMATCH_EXCEPTION;
		}
		if (fp) {
			timeSeries.addDataPoint(TimeUnit.MILLISECONDS, timestamp, Double.longBitsToDouble(value));
		} else {
			timeSeries.addDataPoint(TimeUnit.MILLISECONDS, timestamp, value);
		}
		getCounter().inc();
	}

	public default void writeDataPoint(String dbName, String measurementName, String valueFieldName, List<Tag> tags,
			long timestamp, long value, boolean preSorted) throws IOException {
		// valueFieldName = valueFieldName.intern();
		StorageEngine.validateDataPoint(dbName, measurementName, valueFieldName, tags, TimeUnit.MILLISECONDS);
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName, valueFieldName, tags,
				getDefaultTimebucketSize(), false, preSorted);
		if (timeSeries.isFp()) {
			// drop this datapoint, mixed series are not allowed
			throw FP_MISMATCH_EXCEPTION;
		}
		timeSeries.addDataPoint(TimeUnit.MILLISECONDS, timestamp, value);
		getCounter().inc();
	}

	public default void writeDataPoint(String dbName, String measurementName, String valueFieldName, List<Tag> tags,
			long timestamp, double value, boolean preSorted) throws IOException {
		// valueFieldName = valueFieldName.intern();
		StorageEngine.validateDataPoint(dbName, measurementName, valueFieldName, tags, TimeUnit.MILLISECONDS);
		TimeSeries timeSeries = getOrCreateTimeSeries(dbName, measurementName, valueFieldName, tags,
				getDefaultTimebucketSize(), true, preSorted);
		if (!timeSeries.isFp()) {
			// drop this datapoint, mixed series are not allowed
			throw FP_MISMATCH_EXCEPTION;
		}
		timeSeries.addDataPoint(TimeUnit.MILLISECONDS, timestamp, value);
		getCounter().inc();
	}

	/**
	 * Query timeseries from the storage engine given the supplied attributes.
	 * 
	 * @param dbName
	 * @param measurementPattern
	 * @param valueFieldPattern
	 * @param startTime
	 * @param endTime
	 * @param tagFilter
	 * @param valuePredicate
	 * @param function
	 * @return
	 * @throws IOException
	 */
	public default List<Series> queryDataPoints(String dbName, String measurementPattern, String valueFieldPattern,
			long startTime, long endTime, TagFilter tagFilter, Predicate valuePredicate, Function function)
			throws IOException {
		if (!checkIfExists(dbName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Set<String> measurementsLike = getMeasurementsLike(dbName, measurementPattern);
		List<Series> resultList = Collections.synchronizedList(new ArrayList<>());
		getLogger().finer(() -> "Querying points for:" + measurementsLike + " " + measurementPattern);
		for (String measurement : measurementsLike) {
			Measurement measurementObj = getDatabaseMap().get(dbName).get(measurement);
			measurementObj.queryDataPoints(valueFieldPattern, startTime, endTime, tagFilter, valuePredicate,
					resultList);
		}
		if (function != null) {
			resultList = function.apply(resultList);
		}
		return resultList;
	}

	public default List<Series> queryDataPoints(String dbName, String measurementPattern, String valueFieldPattern,
			long startTime, long endTime, TagFilter tagFilter, Predicate valuePredicate) throws IOException {
		return queryDataPoints(dbName, measurementPattern, valueFieldPattern, startTime, endTime, tagFilter,
				valuePredicate, null);
	}

	public default List<Series> queryDataPoints(String dbName, String measurementPattern, String valueFieldPattern,
			long startTime, long endTime, TagFilter tagFilter) throws IOException {
		return queryDataPoints(dbName, measurementPattern, valueFieldPattern, startTime, endTime, tagFilter, null,
				null);
	}

	/**
	 * List measurements containing the supplied keyword
	 * 
	 * @param dbName
	 * @param partialMeasurementName
	 * @return measurements
	 * @throws IOException
	 */
	public default Set<String> getMeasurementsLike(String dbName, String partialMeasurementName) throws IOException {
		if (!checkIfExists(dbName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Map<String, Measurement> measurementMap = getDatabaseMap().get(dbName);
		partialMeasurementName = partialMeasurementName.trim();
		if (partialMeasurementName.isEmpty()) {
			return measurementMap.keySet();
		} else {
			Pattern p;
			try {
				p = Pattern.compile(partialMeasurementName);
			} catch (Exception e) {
				throw new IOException("Invalid regex for measurement name:" + e.getMessage());
			}
			Set<String> filteredSeries = new HashSet<>();
			for (String measurementName : measurementMap.keySet()) {
				if (p.matcher(measurementName).matches()) {
					filteredSeries.add(measurementName);
				}
			}
			if (filteredSeries.isEmpty()) {
				throw NOT_FOUND_EXCEPTION;
			}
			return filteredSeries;
		}
	}

	/**
	 * List databases
	 * 
	 * @return databases
	 * @throws Exception
	 */
	public Set<String> getDatabases() throws Exception;

	/**
	 * List all measurements for the supplied database
	 * 
	 * @param dbName
	 * @return measurements
	 * @throws Exception
	 */
	public default Set<String> getAllMeasurementsForDb(String dbName) throws Exception {
		if (checkIfExists(dbName)) {
			return getDatabaseMap().get(dbName).keySet();
		} else {
			throw NOT_FOUND_EXCEPTION;
		}
	}

	/**
	 * List all tags for the supplied measurement
	 * 
	 * @param dbName
	 * @param measurementName
	 * @return tags
	 * @throws Exception
	 */
	public default Set<String> getTagKeysForMeasurement(String dbName, String measurementName) throws Exception {
		if (!checkIfExists(dbName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Set<String> measurementsLike = getMeasurementsLike(dbName, measurementName);
		Set<String> results = new HashSet<>();
		for (String m : measurementsLike) {
			results.addAll(getDatabaseMap().get(dbName).get(m).getTagKeys());
		}
		return results;
	}

	public default Set<String> getTagValuesForMeasurement(String dbName, String measurementName, String tagKey)
			throws Exception {
		if (!checkIfExists(dbName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Set<String> measurementsLike = getMeasurementsLike(dbName, measurementName);
		Set<String> results = new HashSet<>();
		for (String m : measurementsLike) {
			Measurement measurement = getDatabaseMap().get(dbName).get(m);
			Collection<String> tagValues = measurement.getTagValues(tagKey);
			if (tagValues != null) {
				results.addAll(tagValues);
			}
		}
		return results;
	}

	/**
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @return tags for the supplied parameters
	 * @throws Exception
	 */
	public default List<List<Tag>> getTagsForMeasurement(String dbName, String measurementName, String valueFieldName)
			throws Exception {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Set<String> measurementsLike = getMeasurementsLike(dbName, measurementName);
		List<List<Tag>> results = new ArrayList<>();
		for (String m : measurementsLike) {
			results.addAll(getDatabaseMap().get(dbName).get(m).getTagsForMeasurement());
		}
		return results;
	}

	/**
	 * Delete all data in this instance
	 * 
	 * @throws Exception
	 */
	public void deleteAllData() throws Exception;

	/**
	 * Check if database exists
	 * 
	 * @param dbName
	 * @return true if db exists
	 * @throws IOException
	 */
	public boolean checkIfExists(String dbName) throws IOException;

	/**
	 * Check if measurement exists
	 * 
	 * @param dbName
	 * @param measurement
	 * @return true if measurement and db exists
	 * @throws IOException
	 */
	public default boolean checkIfExists(String dbName, String measurement) throws IOException {
		if (checkIfExists(dbName)) {
			return getDatabaseMap().get(dbName).containsKey(measurement);
		} else {
			return false;
		}
	}

	/**
	 * Drop database, all data for this database will be deleted
	 * 
	 * @param dbName
	 * @throws Exception
	 */
	public void dropDatabase(String dbName) throws Exception;

	/**
	 * Drop measurement, all data for this measurement will be deleted
	 * 
	 * @param dbName
	 * @param measurementName
	 * @throws Exception
	 */
	public void dropMeasurement(String dbName, String measurementName) throws Exception;

	/**
	 * Get all fields for a measurement
	 * 
	 * @param dbName
	 * @param measurementNameRegex
	 * @return
	 * @throws Exception
	 */
	public default Set<String> getFieldsForMeasurement(String dbName, String measurementNameRegex) throws Exception {
		if (!checkIfExists(dbName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Set<String> filterdMeasurements = new HashSet<>();
		findMeasurementsLike(dbName, measurementNameRegex, filterdMeasurements);
		Set<String> superSet = new HashSet<>();
		if (filterdMeasurements.isEmpty()) {
			throw NOT_FOUND_EXCEPTION;
		}
		for (String name : filterdMeasurements) {
			superSet.addAll(getDatabaseMap().get(dbName).get(name).getFieldsForMeasurement());
		}
		return superSet;
	}

	public default void findMeasurementsLike(String dbName, String measurementNameRegex,
			Set<String> filterdMeasurements) {
		Pattern p = Pattern.compile(measurementNameRegex);
		Set<String> measurementNames = getDatabaseMap().get(dbName).keySet();
		for (String name : measurementNames) {
			if (p.matcher(name).matches()) {
				filterdMeasurements.add(name);
			}
		}
	}

	// retention policy update methods
	/**
	 * Update retention policy for a specific time series
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param tags
	 * @param retentionHours
	 * @param preSorted
	 * @throws IOException
	 */
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, String valueFieldName,
			List<Tag> tags, int retentionHours, boolean preSorted) throws IOException;

	/**
	 * Update retention policy for measurement
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param retentionHours
	 * @throws ItemNotFoundException
	 * @throws IOException
	 */
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, int retentionHours)
			throws ItemNotFoundException, IOException;

	/**
	 * Update default retention policy for a database
	 * 
	 * @param dbName
	 * @param retentionHours
	 * @throws ItemNotFoundException
	 */
	public default void updateDefaultTimeSeriesRetentionPolicy(String dbName, int retentionHours)
			throws ItemNotFoundException {
		DBMetadata dbMetadata = getDbMetadataMap().get(dbName);
		if (dbMetadata == null) {
			throw NOT_FOUND_EXCEPTION;
		}
		synchronized (dbMetadata) {
			dbMetadata.setRetentionHours(retentionHours);
		}
	}

	/**
	 * Update retention policy for a database
	 * 
	 * @param dbName
	 * @param retentionHours
	 * @throws IOException
	 */
	public void updateTimeSeriesRetentionPolicy(String dbName, int retentionHours) throws IOException;

	/**
	 * Gets the database, creates it if it doesn't already exist
	 * 
	 * @param dbName
	 * @return databaseMap
	 * @throws IOException
	 */
	public Map<String, Measurement> getOrCreateDatabase(String dbName) throws IOException;

	/**
	 * Gets the database, creates it with supplied rention policy if it doesn't
	 * already exist
	 * 
	 * @param dbName
	 * @param retentionPolicy
	 * @return measurementMap
	 * @throws IOException
	 */
	public Map<String, Measurement> getOrCreateDatabase(String dbName, int retentionPolicy) throws IOException;

	/**
	 * Gets the measurement, creates it if it doesn't already exist
	 * 
	 * @param dbName
	 * @param measurementName
	 * @return timeseriesMap
	 * @throws IOException
	 */
	public Measurement getOrCreateMeasurement(String dbName, String measurementName) throws IOException;

	/**
	 * Gets the Timeseries, creates it if it doesn't already exist
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param tags
	 * @param timeBucketSize
	 * @param fp
	 * @param preSorted
	 * @return
	 * @throws IOException
	 */
	public TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, String valueFieldName,
			List<Tag> tags, int timeBucketSize, boolean fp, boolean preSorted) throws IOException;

	/**
	 * Check if a measurement field is floating point
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @return true if measurement field is floating point
	 * @throws RejectException
	 * @throws IOException
	 */
	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName)
			throws RejectException, IOException;

	/**
	 * Returns raw readers to be used by the SQL engine for predicate filtering
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws Exception
	 */
	public default LinkedHashMap<Reader, Boolean> queryReaders(String dbName, String measurementName,
			String valueFieldName, long startTime, long endTime) throws Exception {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		LinkedHashMap<Reader, Boolean> readers = new LinkedHashMap<>();
		getDatabaseMap().get(dbName).get(measurementName).queryReaders(valueFieldName, startTime, endTime, readers);
		return readers;
	}

	public default LinkedHashMap<Reader, List<Tag>> queryReadersWithMap(String dbName, String measurementName,
			String valueFieldName, long startTime, long endTime) throws Exception {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		LinkedHashMap<Reader, List<Tag>> readers = new LinkedHashMap<>();
		getDatabaseMap().get(dbName).get(measurementName).queryReadersWithMap(valueFieldName, startTime, endTime,
				readers);
		return readers;
	}

	/**
	 * Check if timeseries exists
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param tags
	 * @return
	 * @throws Exception
	 */
	public default boolean checkTimeSeriesExists(String dbName, String measurementName, String valueFieldName,
			List<Tag> tags) throws Exception {
		if (!checkIfExists(dbName, measurementName)) {
			return false;
		}
		// check and create timeseries
		TimeSeries timeSeries = getDatabaseMap().get(dbName).get(measurementName).getSeriesField(tags)
				.get(valueFieldName);
		return timeSeries != null;
	}

	/**
	 * Get timeseries object
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param tags
	 * @return
	 * @throws IOException
	 */
	public default TimeSeries getTimeSeries(String dbName, String measurementName, String valueFieldName,
			List<Tag> tags) throws IOException {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		// get timeseries
		Measurement measurement = getDatabaseMap().get(dbName).get(measurementName);
		TimeSeries timeSeries = measurement.getSeriesField(tags).get(valueFieldName);
		return timeSeries;
	}

	/**
	 * Get metadata map
	 * 
	 * @return metadata map
	 */
	public Map<String, DBMetadata> getDbMetadataMap();

	public Map<String, Map<String, Measurement>> getMeasurementMap();

	// public default Set<String> getSeriesIdsWhereTags(String dbName, String
	// measurementName, List<String> tags)
	// throws ItemNotFoundException, Exception {
	// if (!checkIfExists(dbName, measurementName)) {
	// throw NOT_FOUND_EXCEPTION;
	// }
	// return
	// getDatabaseMap().get(dbName).get(measurementName).getSeriesIdsWhereTags(tags);
	// }

	public default Set<String> getTagFilteredRowKeys(String dbName, String measurementName, TagFilter tagFilter)
			throws IOException {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		getLogger().log(Level.FINER, "Get tag filtered rowkeys for db" + dbName + " measurement:" + measurementName
				+ " filter:" + tagFilter);
		Measurement measurement = getDatabaseMap().get(dbName).get(measurementName);
		Set<ByteString> tagFilteredRowKeys = measurement.getTagFilteredRowKeys(tagFilter);
		return TagIndex.byteToStringSet(tagFilteredRowKeys, new HashSet<>());
	}

	public Map<String, Map<String, Measurement>> getDatabaseMap();

	public static void validateDataPoint(String dbName, String measurementName, String valueFieldName, List<Tag> tags,
			TimeUnit unit) throws RejectException {
		if (dbName == null || measurementName == null || valueFieldName == null || tags == null || unit == null) {
			throw INVALID_DATAPOINT_EXCEPTION;
		}
	}

	public int getDefaultTimebucketSize();

	public Counter getCounter();

	public default void writeDataPoint(Point dp, boolean preSorted) throws IOException {
		writeDataPoint(dp.getDbName(), dp.getMeasurementName(), dp.getValueFieldName(),
				new ArrayList<>(dp.getTagsList()), dp.getTimestamp(), dp.getValue(), dp.getFp(), preSorted);
	}

	public Logger getLogger();

}
