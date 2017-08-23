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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import com.srotya.sidewinder.core.aggregators.AggregationFunction;
import com.srotya.sidewinder.core.filters.AnyFilter;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.predicates.Predicate;
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
	public static final String DEFAULT_COMPRESSION_CLASS = "com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter";
	public static final String COMPRESSION_CLASS = "compression.class";
	public static final int DEFAULT_TIME_BUCKET_CONSTANT = 4096;
	public static final String DEFAULT_BUCKET_SIZE = "default.bucket.size";
	public static final String RETENTION_HOURS = "default.series.retention.hours";
	public static final int DEFAULT_RETENTION_HOURS = (int) Math
			.ceil((((double) DEFAULT_TIME_BUCKET_CONSTANT) * 24 / 60) / 60);
	public static final String PERSISTENCE_DISK = "persistence.disk";
	public static final String ARCHIVER_CLASS = "archiver.class";
	public static final String GC_DELAY = "gc.delay";
	public static final String GC_FREQUENCY = "gc.frequency";
	public static final String DEFAULT_GC_FREQUENCY = "500000";
	public static final String DEFAULT_GC_DELAY = "60000";

	/**
	 * @param conf
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

	/**
	 * Write datapoint to the storage engine
	 * 
	 * @param dp
	 * @throws IOException
	 */
	public void writeDataPoint(DataPoint dp) throws IOException;

	/**
	 * Query timeseries from the storage engine given the supplied attributes.
	 * This function doesn't allow use of {@link AggregationFunction}.
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param startTime
	 * @param endTime
	 * @param tags
	 * @param valuePredicate
	 * @return timeSeriesResultMap
	 * @throws ItemNotFoundException
	 * @throws IOException
	 */
	public default Set<SeriesQueryOutput> queryDataPoints(String dbName, String measurementName, String valueFieldName,
			long startTime, long endTime, List<String> tags, Predicate valuePredicate)
			throws ItemNotFoundException, IOException {
		return queryDataPoints(dbName, measurementName, valueFieldName, startTime, endTime, tags,
				new AnyFilter<List<String>>(), valuePredicate, null);
	}

	/**
	 * Query timeseries from the storage engine given the supplied attributes.
	 * This function does allow use of {@link AggregationFunction}.
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param startTime
	 * @param endTime
	 * @param tagList
	 * @param tagFilter
	 * @param valuePredicate
	 * @param aggregationFunction
	 * @return timeSeriesResultMap
	 * @throws ItemNotFoundException
	 * @throws IOException
	 */
	public Set<SeriesQueryOutput> queryDataPoints(String dbName, String measurementName, String valueFieldName,
			long startTime, long endTime, List<String> tagList, Filter<List<String>> tagFilter,
			Predicate valuePredicate, AggregationFunction aggregationFunction)
			throws ItemNotFoundException, IOException;

	/**
	 * List measurements containing the supplied keyword
	 * 
	 * @param dbName
	 * @param partialMeasurementName
	 * @return measurements
	 * @throws Exception
	 */
	public Set<String> getMeasurementsLike(String dbName, String partialMeasurementName) throws Exception;

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
	public Set<String> getAllMeasurementsForDb(String dbName) throws Exception;

	/**
	 * List all tags for the supplied measurement
	 * 
	 * @param dbname
	 * @param measurementName
	 * @return tags
	 * @throws Exception
	 */
	public Set<String> getTagsForMeasurement(String dbname, String measurementName) throws Exception;

	/**
	 * @param dbname
	 * @param measurementName
	 * @param valueFieldName
	 * @return
	 * @throws Exception
	 */
	public List<List<String>> getTagsForMeasurement(String dbname, String measurementName, String valueFieldName)
			throws Exception;

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
	 * @throws Exception
	 */
	public boolean checkIfExists(String dbName) throws Exception;

	/**
	 * Check if measurement exists
	 * 
	 * @param dbName
	 * @param measurement
	 * @return true if measurement and db exists
	 * @throws Exception
	 */
	public boolean checkIfExists(String dbName, String measurement) throws Exception;

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
	 * @param measurementName
	 * @return
	 * @throws Exception
	 */
	public Set<String> getFieldsForMeasurement(String dbName, String measurementName) throws Exception;

	// retention policy update methods
	/**
	 * Update retention policy for a specific time series
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param tags
	 * @param retentionHours
	 * @throws IOException
	 */
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int retentionHours) throws IOException;

	/**
	 * Update retention policy for measurement
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param retentionHours
	 * @throws ItemNotFoundException
	 */
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, int retentionHours) throws ItemNotFoundException;

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
	public Map<String, Measurement> getOrCreateDatabase(String dbName, int retentionPolicy)
			throws IOException;

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
	 * @return timeseries object
	 * @throws IOException
	 */
	public TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int timeBucketSize, boolean fp) throws IOException;

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
	 * @param fieldName
	 * @param key
	 * @param value
	 * @return readers
	 * @throws Exception
	 */
	public Map<Reader, Boolean> queryReaders(String dbName, String measurementName, String fieldName, long key,
			long value) throws Exception;

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
	public boolean checkTimeSeriesExists(String dbName, String measurementName, String valueFieldName,
			List<String> tags) throws Exception;

	/**
	 * Get timeseries object
	 * 
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param tags
	 * @return
	 * @throws Exception
	 */
	public TimeSeries getTimeSeries(String dbName, String measurementName, String valueFieldName, List<String> tags)
			throws Exception;

	/**
	 * Get metadata map
	 * 
	 * @return metadata map
	 */
	public Map<String, DBMetadata> getDbMetadataMap();
	
	public Map<String, Map<String, Measurement>> getMeasurementMap();

	Set<String> getSeriesIdsWhereTags(String dbName, String measurementName, List<String> rawTags) throws ItemNotFoundException, Exception;

	Set<String> getTagFilteredRowKeys(String dbName, String measurementName, String valueFieldName,
			Filter<List<String>> tagFilterTree, List<String> tags) throws ItemNotFoundException, Exception;

}
