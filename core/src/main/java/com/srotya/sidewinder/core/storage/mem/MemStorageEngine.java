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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.aggregators.AggregationFunction;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.Archiver;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.SeriesQueryOutput;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.TimeSeriesBucket;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.mem.archival.NoneArchiver;

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

	private static final Logger logger = Logger.getLogger(MemStorageEngine.class.getName());
	private static ItemNotFoundException NOT_FOUND_EXCEPTION = new ItemNotFoundException("Item not found");
	private static RejectException FP_MISMATCH_EXCEPTION = new RejectException("Floating point mismatch");
	private static RejectException INVALID_DATAPOINT_EXCEPTION = new RejectException(
			"Datapoint is missing required values");
	private Map<String, Map<String, Measurement>> databaseMap;
	private AtomicInteger counter = new AtomicInteger(0);
	private Map<String, DBMetadata> dbMetadataMap;
	private int defaultRetentionHours;
	private int defaultTimebucketSize;
	private Archiver archiver;
	private Map<String, String> conf;
	private ScheduledExecutorService bgTaskPool;

	@Override
	public void configure(Map<String, String> conf, ScheduledExecutorService bgTaskPool) throws IOException {
		this.conf = conf;
		this.bgTaskPool = bgTaskPool;
		this.defaultRetentionHours = Integer
				.parseInt(conf.getOrDefault(RETENTION_HOURS, String.valueOf(DEFAULT_RETENTION_HOURS)));
		logger.info("Setting default timeseries retention hours policy to:" + defaultRetentionHours);
		databaseMap = new ConcurrentHashMap<>();
		dbMetadataMap = new ConcurrentHashMap<>();
		try {
			archiver = (Archiver) Class.forName(conf.getOrDefault("archiver.class", NoneArchiver.class.getName()))
					.newInstance();
			archiver.init(conf);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Failed to instantiate archiver", e);
		}
		this.defaultTimebucketSize = Integer
				.parseInt(conf.getOrDefault(DEFAULT_BUCKET_SIZE, String.valueOf(DEFAULT_TIME_BUCKET_CONSTANT)));
		conf.put(PERSISTENCE_DISK, "false");
		if (bgTaskPool != null) {
			bgTaskPool.scheduleAtFixedRate(() -> {
				// for (Entry<String, Map<String, Map<String, TimeSeries>>>
				// measurementMap : databaseMap.entrySet()) {
				// String db = measurementMap.getKey();
				// for (Entry<String, Map<String, TimeSeries>> measurementEntry
				// : measurementMap.getValue()
				// .entrySet()) {
				// String measurement = measurementEntry.getKey();
				// for (Entry<String, TimeSeries> entry :
				// measurementEntry.getValue().entrySet()) {
				// List<TimeSeriesBucket> buckets;
				// try {
				// buckets = entry.getValue().collectGarbage();
				// } catch (IOException e1) {
				// continue;
				// }
				// if (archiver != null) {
				// for (TimeSeriesBucket bucket : buckets) {
				// try {
				// archiver.archive(
				// new TimeSeriesArchivalObject(db, measurement, entry.getKey(),
				// bucket));
				// } catch (ArchiveException e) {
				// logger.log(Level.WARNING,
				// "Failed to archive time series bucket:" + entry.getKey(), e);
				// }
				// }
				// }
				// }
				// }
				// }
			}, Integer.parseInt(conf.getOrDefault(GC_FREQUENCY, DEFAULT_GC_FREQUENCY)),
					Integer.parseInt(conf.getOrDefault(GC_DELAY, DEFAULT_GC_DELAY)), TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int retentionHours) throws IOException {
		TimeSeries series = getOrCreateTimeSeries(dbName, measurementName, valueFieldName, tags, defaultTimebucketSize,
				true);
		series.setRetentionHours(retentionHours);
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, int retentionHours)
			throws ItemNotFoundException {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Measurement measurement = databaseMap.get(dbName).get(measurementName);
		for (TimeSeries series : measurement.getTimeSeriesMap().values()) {
			series.setRetentionHours(retentionHours);
		}
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, int retentionHours) {
		DBMetadata metadata = dbMetadataMap.get(dbName);
		synchronized (metadata) {
			metadata.setRetentionHours(retentionHours);
			Map<String, Measurement> measurementMap = databaseMap.get(dbName);
			if (measurementMap != null) {
				for (Measurement sortedMap : measurementMap.values()) {
					for (TimeSeries timeSeries : sortedMap.getTimeSeries()) {
						timeSeries.setRetentionHours(retentionHours);
					}
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
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			Measurement measurement = measurementMap.get(measurementName);
			if (measurement != null) {
				measurement.queryReaders(valueFieldName, startTime, endTime, readers);
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
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			Measurement measurement = measurementMap.get(measurementName);
			if (measurement != null) {
				try {
					measurement.queryDataPoints(valueFieldName, startTime, endTime, tagList, tagFilter, valuePredicate,
							aggregationFunction, resultMap);
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Error running query on measurement", e);
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
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
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
				dp.getTags(), defaultTimebucketSize, dp.isFp());
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

	@Override
	public Map<String, Measurement> getOrCreateDatabase(String dbName) {
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			synchronized (databaseMap) {
				if ((measurementMap = databaseMap.get(dbName)) == null) {
					measurementMap = new ConcurrentHashMap<>();
					databaseMap.put(dbName, measurementMap);
					DBMetadata metadata = new DBMetadata();
					metadata.setRetentionHours(defaultRetentionHours);
					dbMetadataMap.put(dbName, metadata);
					logger.info("Created new database:" + dbName + "\t with retention period:" + defaultRetentionHours
							+ " hours");
				}
			}
		}
		return measurementMap;
	}

	@Override
	public Map<String, Measurement> getOrCreateDatabase(String dbName, int retentionPolicy) {
		Map<String, Measurement> map = getOrCreateDatabase(dbName);
		updateTimeSeriesRetentionPolicy(dbName, retentionPolicy);
		return map;
	}

	@Override
	public Measurement getOrCreateMeasurement(String dbName, String measurementName) throws IOException {
		Map<String, Measurement> measurementMap = getOrCreateDatabase(dbName);
		return getOrCreateMeasurement(measurementMap, dbName, measurementName);
	}

	protected Measurement getOrCreateMeasurement(Map<String, Measurement> measurementMap, String dbName, String measurementName) throws IOException {
		Measurement measurement = measurementMap.get(measurementName);
		if (measurement == null) {
			synchronized (measurementMap) {
				if ((measurement = measurementMap.get(measurementName)) == null) {
					// TODO create measurement
					measurement = new MemoryMeasurement();
					measurement.configure(conf, measurementName, "", "", dbMetadataMap.get(dbName), bgTaskPool);
					measurementMap.put(measurementName, measurement);
					logger.info("Created new measurement:" + measurementName);
				}
			}
		}
		return measurement;
	}

	@Override
	public boolean checkTimeSeriesExists(String dbName, String measurementName, String valueFieldName,
			List<String> tags) throws Exception {
		if (!checkIfExists(dbName, measurementName)) {
			return false;
		}
		// check and create timeseries
		TimeSeries timeSeries = databaseMap.get(dbName).get(measurementName).getTimeSeries(valueFieldName, tags);
		return timeSeries != null;
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int timeBucketSize, boolean fp) throws IOException {
		// check and create database map
		Map<String, Measurement> dbMap = getOrCreateDatabase(dbName);

		// check and create measurement map
		Measurement measurement = getOrCreateMeasurement(dbMap, dbName, measurementName);

		// check and create timeseries
		return measurement.getOrCreateTimeSeries(valueFieldName, tags, timeBucketSize, fp, conf);
	}

	@Override
	public TimeSeries getTimeSeries(String dbName, String measurementName, String valueFieldName, List<String> tags)
			throws IOException {
		// check and create database map
		Map<String, Measurement> dbMap = getOrCreateDatabase(dbName);

		// check and create measurement map
		Measurement measurementMap = getOrCreateMeasurement(dbMap, dbName, measurementName);

		// check and create timeseries
		return measurementMap.getTimeSeries(valueFieldName, tags);
	}

	@Override
	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName)
			throws IOException {
		Map<String, Measurement> dbMap = getOrCreateDatabase(dbName);

		// check and create measurement map
		Measurement measurement = getOrCreateMeasurement(dbMap, dbName, measurementName);
		for (Entry<String, TimeSeries> entry : measurement.getTimeSeriesMap().entrySet()) {
			if (entry.getKey().startsWith(valueFieldName)) {
				return entry.getValue().isFp();
			}
		}
		throw NOT_FOUND_EXCEPTION;
	}

	@Override
	public Set<String> getSeriesIdsWhereTags(String dbName, String measurementName, List<String> tags) throws IOException {
		if(!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		return databaseMap.get(dbName).get(measurementName).getSeriesIdsWhereTags(tags);
	}

	@Override
	public Set<String> getTagFilteredRowKeys(String dbName, String measurementName, String valueFieldName,
			Filter<List<String>> tagFilterTree, List<String> rawTags) throws IOException {
		if(!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		return databaseMap.get(dbName).get(measurementName).getTagFilteredRowKeys(valueFieldName, tagFilterTree, rawTags);
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
	public boolean checkIfExists(String dbName) {
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
	protected Measurement getMeasurement(String dbName, String measurementName) {
		return databaseMap.get(dbName).get(measurementName);
	}

	@Override
	public void connect() throws IOException {
	}

	@Override
	public void disconnect() throws IOException {
	}

	@Override
	public boolean checkIfExists(String dbName, String measurement) {
		if (checkIfExists(dbName)) {
			return databaseMap.get(dbName).containsKey(measurement);
		} else {
			return false;
		}
	}

	@Override
	public Set<String> getTagsForMeasurement(String dbName, String measurementName) throws Exception {
		if(!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		return databaseMap.get(dbName).get(measurementName).getTags();
	}

	@Override
	public List<List<String>> getTagsForMeasurement(String dbName, String measurementName, String valueFieldName)
			throws Exception {
		if(!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		return databaseMap.get(dbName).get(measurementName).getTagsForMeasurement(valueFieldName);
	}

	@Override
	public Set<String> getFieldsForMeasurement(String dbName, String measurementName) throws Exception {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		return databaseMap.get(dbName).get(measurementName).getFieldsForMeasurement();
	}

	public Map<String, DBMetadata> getDbMetadataMap() {
		return dbMetadataMap;
	}

	@Override
	public Map<String, Map<String, Measurement>> getMeasurementMap() {
		return databaseMap;
	}
}
