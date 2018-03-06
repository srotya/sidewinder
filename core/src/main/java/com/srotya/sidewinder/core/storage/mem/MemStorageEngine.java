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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.Archiver;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.SeriesFieldMap;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.archival.NoneArchiver;
import com.srotya.sidewinder.core.storage.compression.Writer;

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
 * {@link Writer} is uses compressed in-memory representation of the actual
 * data. Periodic checks against size ensure that Sidewinder server doesn't run
 * out of memory. <br>
 * <br>
 * 
 * @author ambud
 */
public class MemStorageEngine implements StorageEngine {

	private static final Logger logger = Logger.getLogger(MemStorageEngine.class.getName());
	private Map<String, Map<String, Measurement>> databaseMap;
	private Map<String, DBMetadata> dbMetadataMap;
	private int defaultRetentionHours;
	private int defaultTimebucketSize;
	private Archiver archiver;
	private Map<String, String> conf;
	private ScheduledExecutorService bgTaskPool;

	// monitoring metrics
	private Counter metricsDbCounter;
	private Counter metricsMeasurementCounter;
	private Counter metricsWriteCounter;

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
				for (Entry<String, Map<String, Measurement>> measurementMap : databaseMap.entrySet()) {
					for (Entry<String, Measurement> measurementEntry : measurementMap.getValue().entrySet()) {
						Measurement value = measurementEntry.getValue();
						try {
							value.collectGarbage(archiver);
						} catch (IOException e) {
							logger.log(Level.SEVERE,
									"Failed collect garbage for measurement:" + value.getMeasurementName(), e);
						}
					}
				}
			}, Integer.parseInt(conf.getOrDefault(GC_FREQUENCY, DEFAULT_GC_FREQUENCY)),
					Integer.parseInt(conf.getOrDefault(GC_DELAY, DEFAULT_GC_DELAY)), TimeUnit.SECONDS);
			if (Boolean.parseBoolean(conf.getOrDefault(StorageEngine.COMPACTION_ENABLED, "false"))) {
				logger.info("Compaction is enabled");
				bgTaskPool.scheduleAtFixedRate(() -> {
					for (Entry<String, Map<String, Measurement>> measurementMap : databaseMap.entrySet()) {
						for (Entry<String, Measurement> measurementEntry : measurementMap.getValue().entrySet()) {
							Measurement value = measurementEntry.getValue();
							try {
								value.compact();
							} catch (Exception e) {
								logger.log(Level.SEVERE,
										"Failed compaction for measurement:" + value.getMeasurementName(), e);
							}
						}
					}
				}, Integer.parseInt(conf.getOrDefault(COMPACTION_FREQUENCY, DEFAULT_COMPACTION_FREQUENCY)),
						Integer.parseInt(conf.getOrDefault(COMPACTION_DELAY, DEFAULT_COMPACTION_DELAY)),
						TimeUnit.SECONDS);
			} else {
				logger.warning("Compaction is disabled");
			}
		}
		enableMetricsService();
	}

	public void enableMetricsService() {
		MetricsRegistryService reg = MetricsRegistryService.getInstance(this, bgTaskPool);
		MetricRegistry metaops = reg.getInstance("metaops");
		metricsDbCounter = metaops.counter("dbcreate");
		metricsMeasurementCounter = metaops.counter("measurementcreate");
		metricsWriteCounter = reg.getInstance("ops").counter("writecounter");
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
			throws IOException {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Measurement measurement = databaseMap.get(dbName).get(measurementName);
		for (TimeSeries series : measurement.getTimeSeries()) {
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
					metricsDbCounter.inc();
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

	protected Measurement getOrCreateMeasurement(Map<String, Measurement> measurementMap, String dbName,
			String measurementName) throws IOException {
		Measurement measurement = measurementMap.get(measurementName);
		if (measurement == null) {
			synchronized (measurementMap) {
				if ((measurement = measurementMap.get(measurementName)) == null) {
					measurement = new MemoryMeasurement();
					measurement.configure(conf, this, dbName, measurementName, "", "", dbMetadataMap.get(dbName),
							bgTaskPool);
					measurementMap.put(measurementName, measurement);
					logger.info("Created new measurement:" + measurementName);
					metricsMeasurementCounter.inc();
				}
			}
		}
		return measurement;
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
	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName)
			throws IOException {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Map<String, Measurement> dbMap = getOrCreateDatabase(dbName);
		// check and create measurement map
		Measurement measurement = getOrCreateMeasurement(dbMap, dbName, measurementName);
		for (String entry : measurement.getSeriesKeys()) {
			SeriesFieldMap seriesFromKey = measurement.getSeriesFromKey(entry);
			if (seriesFromKey.get(valueFieldName) != null) {
				return seriesFromKey.get(valueFieldName).isFp();
			}
		}
		throw NOT_FOUND_EXCEPTION;
	}

	@Override
	public Set<String> getDatabases() throws Exception {
		return databaseMap.keySet();
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
		metricsDbCounter.dec();
	}

	@Override
	public void dropMeasurement(String dbName, String measurementName) throws Exception {
		databaseMap.get(dbName).remove(measurementName);
		metricsMeasurementCounter.dec();
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

	public Map<String, DBMetadata> getDbMetadataMap() {
		return dbMetadataMap;
	}

	@Override
	public Map<String, Map<String, Measurement>> getMeasurementMap() {
		return databaseMap;
	}

	@Override
	public Map<String, Map<String, Measurement>> getDatabaseMap() {
		return databaseMap;
	}

	@Override
	public int getDefaultTimebucketSize() {
		return defaultTimebucketSize;
	}

	@Override
	public Counter getCounter() {
		return metricsWriteCounter;
	}

	@Override
	public Logger getLogger() {
		return logger;
	}
}
