/**
 * Copyright 2016 Ambud Sharma
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
import java.util.SortedMap;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.gorilla.TimeSeries;

/**
 * @author ambudsharma
 */
public interface StorageEngine {

	/**
	 * @param conf
	 * @throws IOException
	 */
	public void configure(Map<String, String> conf) throws IOException;

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
	 * @param dbName
	 * @param measurementName
	 * @param valueFieldName
	 * @param startTime
	 * @param endTime
	 * @param tags
	 * @param valuePredicate
	 * @return
	 * @throws ItemNotFoundException
	 */
	public Map<String, List<DataPoint>> queryDataPoints(String dbName, String measurementName, String valueFieldName, long startTime,
			long endTime, List<String> tags, Predicate valuePredicate) throws ItemNotFoundException;

	public Set<String> getMeasurementsLike(String dbName, String partialMeasurementName) throws IOException;

	public Set<String> getDatabases() throws Exception;

	public Set<String> getAllMeasurementsForDb(String dbName) throws Exception;
	
	public Set<String> getTagsForMeasurement(String dbname, String measurementName) throws Exception;

	public void deleteAllData() throws Exception;

	public boolean checkIfExists(String dbName) throws Exception;

	public boolean checkIfExists(String dbName, String measurement) throws Exception;

	public void dropDatabase(String dbName) throws Exception;

	public void dropMeasurement(String dbName, String measurementName) throws Exception;

	public Set<String> getFieldsForMeasurement(String dbName, String measurementName) throws Exception;

	// retention policy update methods
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int retentionHours);

	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, int retentionHours);

	public void updateDefaultTimeSeriesRetentionPolicy(String dbName, int retentionHours);

	public void updateTimeSeriesRetentionPolicy(String dbName, int retentionHours);

	public Map<String, SortedMap<String, TimeSeries>> getOrCreateDatabase(String dbName);
	
	public Map<String, SortedMap<String, TimeSeries>> getOrCreateDatabase(String dbName, int retentionPolicy);

	public Map<String, TimeSeries> getOrCreateMeasurement(String dbName, String measurementName);

	public TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, String valueFieldName, List<String> tags,
			int timeBucketSize, boolean fp);

	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName) throws RejectException;

}
