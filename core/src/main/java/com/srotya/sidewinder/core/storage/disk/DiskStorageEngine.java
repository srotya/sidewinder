/**
 * Copyright Ambud Sharma
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
package com.srotya.sidewinder.core.storage.disk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.archival.Archiver;
import com.srotya.sidewinder.core.storage.archival.NoneArchiver;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * Disk based timeseries {@link StorageEngine}
 * 
 * @author ambud
 */
public class DiskStorageEngine implements StorageEngine {

	private static final String TMP_SIDEWINDER_INDEX = "/tmp/sidewinder/index";
	private static final String TMP_SIDEWINDER_DATA = "/tmp/sidewinder/data";
	private static final String INDEX_DIR = "index.dir";
	private static final String DATA_DIRS = "data.dir";
	private static final Logger logger = Logger.getLogger(DiskStorageEngine.class.getName());
	private Map<String, Map<String, Measurement>> databaseMap;
	private Map<String, DBMetadata> dbMetadataMap;
	private int defaultRetentionHours;
	private int defaultTimebucketSize;
	private Archiver archiver;
	private Map<String, String> conf;
	private String[] dataDirs;
	private String baseIndexDirectory;
	private ScheduledExecutorService bgTaskPool;
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
		dataDirs = MiscUtils.splitAndNormalizeString(conf.getOrDefault(DATA_DIRS, TMP_SIDEWINDER_DATA));
		baseIndexDirectory = conf.getOrDefault(INDEX_DIR, TMP_SIDEWINDER_INDEX);
		for (String dataDir : dataDirs) {
			new File(dataDir).mkdirs();
		}
		new File(baseIndexDirectory).mkdirs();
		databaseMap = new ConcurrentHashMap<>();
		dbMetadataMap = new ConcurrentHashMap<>();

		setCodecsForCompression(conf);

		try {
			archiver = (Archiver) Class.forName(conf.getOrDefault(ARCHIVER_CLASS, NoneArchiver.class.getName()))
					.newInstance();
			archiver.init(conf);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Failed to instantiate archiver", e);
		}
		this.defaultTimebucketSize = Integer
				.parseInt(conf.getOrDefault(DEFAULT_BUCKET_SIZE, String.valueOf(DEFAULT_TIME_BUCKET_CONSTANT)));
		logger.info("Configuring default time bucket:" + getDefaultTimebucketSize());
		enableMetricsService();
		if (bgTaskPool != null) {
			if (Boolean.parseBoolean(conf.getOrDefault(GC_ENABLED, "true"))) {
				bgTaskPool.scheduleAtFixedRate(() -> {
					for (Entry<String, Map<String, Measurement>> measurementMap : databaseMap.entrySet()) {
						for (Entry<String, Measurement> measurementEntry : measurementMap.getValue().entrySet()) {
							Measurement value = measurementEntry.getValue();
							try {
								value.collectGarbage(archiver);
							} catch (Exception e) {
								logger.log(Level.SEVERE,
										"Failed collect garbage for measurement:" + value.getMeasurementName(), e);
							}
							logger.log(Level.FINE, "Completed Measuremenet GC:" + measurementEntry.getKey());
						}
						logger.log(Level.FINE, "Completed DB GC:" + measurementMap.getKey());
					}
				}, Integer.parseInt(conf.getOrDefault(GC_FREQUENCY, DEFAULT_GC_FREQUENCY)),
						Integer.parseInt(conf.getOrDefault(GC_DELAY, DEFAULT_GC_DELAY)), TimeUnit.SECONDS);
			} else {
				logger.info("WARNING: GC has been disabled, data retention policies will not be honored");
			}
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
		loadDatabases();
	}

	public void enableMetricsService() {
		MetricsRegistryService reg = MetricsRegistryService.getInstance(this, bgTaskPool);
		MetricRegistry metaops = reg.getInstance("metaops");
		metricsDbCounter = metaops.counter("db-create");
		metricsMeasurementCounter = metaops.counter("measurement-create");
		metricsWriteCounter = reg.getInstance("ops").counter("write-counter");
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, int retentionHours) {
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		synchronized (dbMetadataMap) {
			if (measurementMap != null) {
				Measurement m = measurementMap.get(measurementName);
				if (m != null) {
					m.setRetentionHours(retentionHours);
				}
			}
		}
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, int retentionHours) throws IOException {
		DBMetadata metadata = dbMetadataMap.get(dbName);
		synchronized (dbMetadataMap) {
			metadata.setRetentionHours(retentionHours);
			saveDBMetadata(dbName, metadata);
			Map<String, Measurement> measurementMap = databaseMap.get(dbName);
			if (measurementMap != null) {
				for (Measurement m : measurementMap.values()) {
					m.setRetentionHours(retentionHours);
				}
			}
		}
	}

	@Override
	public Map<String, Measurement> getOrCreateDatabase(String dbName) throws IOException {
		return getOrCreateDatabase(dbName, defaultRetentionHours, conf);
	}
	
	@Override
	public Map<String, Measurement> getOrCreateDatabase(String dbName, int retention) throws IOException {
		return getOrCreateDatabase(dbName, retention, conf);
	}

	private void saveDBMetadata(String dbName, DBMetadata metadata) throws IOException {
		String dbDirectory = dbDirectoryPath(dbName);
		String line = new Gson().toJson(metadata);
		writeLineToFile(line, dbDirectory + "/.md");
	}

	protected void createDatabaseDirectory(String dbName) {
		String dbDirectory = dbDirectoryPath(dbName);
		File file = new File(dbDirectory);
		file.mkdirs();
	}

	protected void loadDatabases() throws IOException {
		for (String dataDir : dataDirs) {
			File mdDir = new File(dataDir);
			if (!mdDir.exists()) {
				return;
			}
			File[] dbs = mdDir.listFiles();
			for (File db : dbs) {
				if (!db.isDirectory()) {
					continue;
				}
				Map<String, Measurement> measurementMap = new ConcurrentHashMap<>();
				String dbName = db.getName();
				databaseMap.put(dbName, measurementMap);
				DBMetadata metadata = readMetadata(dbName);
				dbMetadataMap.put(dbName, metadata);
				logger.info("Loading database:" + dbName);
				loadMeasurements(dbName, measurementMap, metadata);
			}
		}
	}

	protected DBMetadata readMetadata(String dbName) throws IOException {
		String path = dbDirectoryPath(dbName) + "/.md";
		File file = new File(path);
		if (!file.exists()) {
			return new DBMetadata(defaultRetentionHours, DiskMalloc.getBufIncrement(conf),
					DiskMalloc.getFileIncrement(conf));
		}
		List<String> lines = MiscUtils.readAllLines(file);
		StringBuilder builder = new StringBuilder();
		for (String line : lines) {
			builder.append(line);
		}
		logger.info("JSON for metadata:" + builder.toString());
		DBMetadata metadata = new Gson().fromJson(builder.toString(), DBMetadata.class);
		if (metadata == null) {
			throw new IOException("Invalid metadata for:" + dbName);
		}
		return metadata;
	}

	@Override
	public Map<String, Measurement> getOrCreateDatabase(String dbName, int retentionHours, Map<String, String> conf) throws IOException {
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			synchronized (databaseMap) {
				if ((measurementMap = databaseMap.get(dbName)) == null) {
					measurementMap = new ConcurrentHashMap<>();
					databaseMap.put(dbName, measurementMap);
					createDatabaseDirectory(dbName);
					DBMetadata metadata = new DBMetadata();
					metadata.setRetentionHours(retentionHours);
					metadata.setBufIncrementSize(DiskMalloc.getBufIncrement(conf));
					metadata.setFileIncrementSize(DiskMalloc.getFileIncrement(conf));
					dbMetadataMap.put(dbName, metadata);
					saveDBMetadata(dbName, metadata);
					logger.info("Created new database:" + dbName + "\t with retention period:" + retentionHours
							+ " hours (" + (retentionHours * 3600 / defaultTimebucketSize) + " buckets)");
					metricsDbCounter.inc();
				}
			}
		}
		return measurementMap;
	}

	@Override
	public Measurement getOrCreateMeasurement(String dbName, String measurementName) throws IOException {
		Map<String, Measurement> measurementMap = getOrCreateDatabase(dbName);
		return getOrCreateMeasurement(measurementMap, measurementName, dbName);
	}

	protected Measurement getOrCreateMeasurement(Map<String, Measurement> measurementMap, String measurementName,
			String dbName) throws IOException {
		Measurement measurement = measurementMap.get(measurementName);
		if (measurement == null) {
			synchronized (databaseMap) {
				if ((measurement = measurementMap.get(measurementName)) == null) {
					measurement = new PersistentMeasurement();
					measurement.configure(conf, this, getDefaultTimebucketSize(), dbName, measurementName,
							dbIndexPath(dbName), dbDirectoryPath(dbName), dbMetadataMap.get(dbName), bgTaskPool);
					measurementMap.put(measurementName, measurement);
					logger.info("Created new measurement:" + measurementName);
					metricsMeasurementCounter.inc();
				}
			}
		}
		return measurement;
	}

	public String getDataDir(String dbName) {
		return dataDirs[dbName.hashCode() % dataDirs.length];
	}

	public String dbDirectoryPath(String dbName) {
		return getDataDir(dbName) + "/" + dbName;
	}

	public String dbIndexPath(String dbName) {
		return getIndexDir(dbName) + "/" + dbName;
	}

	public String getIndexDir(String dbName) {
		return baseIndexDirectory;
	}

	protected void loadMeasurements(String dbName, Map<String, Measurement> measurementMap, DBMetadata metadata)
			throws IOException {
		File file = new File(dbDirectoryPath(dbName));
		if (!file.exists() || file.listFiles() == null) {
			return;
		}
		List<Future<?>> futures = new ArrayList<>();
		for (File measurementMdFile : file.listFiles()) {
			if (!measurementMdFile.isDirectory()) {
				continue;
			}
			String measurementName = measurementMdFile.getName();
			Measurement measurement = new PersistentMeasurement();
			measurementMap.put(measurementName, measurement);
			logger.info("Loading measurements:" + measurementName);
			futures.add(bgTaskPool.submit(() -> {
				try {
					measurement.configure(conf, this, getDefaultTimebucketSize(), dbName, measurementName,
							dbIndexPath(dbName), dbDirectoryPath(dbName), metadata, bgTaskPool);
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Error recovering measurement:" + measurementName, e);
				}
			}));
		}
		for (Future<?> future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				logger.log(Level.SEVERE, "Error future get recovering measurement, db:" + dbName, e);
			}
		}
	}

	public static void writeLineToFile(String line, String filePath) throws IOException {
		File pth = new File(filePath);
		PrintWriter pr = new PrintWriter(new FileOutputStream(pth, false));
		pr.println(line);
		pr.close();
	}

	public static void appendLineToFile(String line, PrintWriter pr) throws IOException {
		pr.println(line);
		pr.flush();
	}

	@Override
	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName)
			throws IOException {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Map<String, Measurement> dbMap = getOrCreateDatabase(dbName);
		// check and create measurement map
		Measurement measurement = getOrCreateMeasurement(dbMap, measurementName, dbName);
		return measurement.isFieldFp(valueFieldName);
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
	public boolean checkIfExists(String dbName) throws IOException {
		return databaseMap.containsKey(dbName);
	}

	@Override
	public void dropDatabase(String dbName) throws Exception {
		synchronized (databaseMap) {
			Map<String, Measurement> remove = databaseMap.remove(dbName);
			for (Measurement measurement : remove.values()) {
				measurement.close();
			}
			boolean result = MiscUtils.delete(new File(dbDirectoryPath(dbName)));
			if (!result) {
				throw new Exception("Database(" + dbName + ") deletion(data) failed due file deletion issues");
			}
			result = MiscUtils.delete(new File(dbIndexPath(dbName)));
			if (!result) {
				throw new Exception("Database(" + dbName + ") deletion(index) failed due file deletion issues");
			}
			metricsDbCounter.dec();
			logger.info("Database(" + dbName + ") deleted");
		}
	}

	@Override
	public void dropMeasurement(String dbName, String measurementName) throws Exception {
		Map<String, Measurement> map = databaseMap.get(dbName);
		synchronized (databaseMap) {
			map.remove(measurementName);
			metricsMeasurementCounter.dec();
		}
	}

	@Override
	public void startup() throws IOException {
	}

	@Override
	public void shutdown() throws IOException {
		if (databaseMap != null) {
			for (Entry<String, Map<String, Measurement>> measurementMap : databaseMap.entrySet()) {
				for (Measurement m : measurementMap.getValue().values()) {
					m.close();
				}
			}
			System.gc();
		}
	}

	@Override
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

	@Override
	public Map<String, String> getConf() {
		return conf;
	}
}
