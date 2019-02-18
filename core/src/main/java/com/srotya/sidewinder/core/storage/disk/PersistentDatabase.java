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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Database;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.utils.MiscUtils;

public class PersistentDatabase implements Database {

	private static final Logger logger = Logger.getLogger(PersistentDatabase.class.getName());
	private String dbName;
	private Map<String, Measurement> measurementMap;
	private DBMetadata metadata;
	private DiskStorageEngine engine;

	public PersistentDatabase(String dbName, DiskStorageEngine engine) {
		this.dbName = dbName;
		this.engine = engine;
		this.metadata = new DBMetadata();
		this.measurementMap = new ConcurrentHashMap<>();
	}

	@Override
	public void init(int retentionHours, Map<String, String> conf) throws IOException {
		createDatabaseDirectory(dbName);
		metadata.setTimeBucketSize(
				Integer.parseInt(conf.getOrDefault(BUCKET_SIZE, String.valueOf(engine.getDefaultTimebucketSize()))));
		metadata.setRetentionHours(retentionHours);
		metadata.setBufIncrementSize(DiskMalloc.getBufIncrement(conf));
		metadata.setFileIncrementSize(DiskMalloc.getFileIncrement(conf));
		saveDBMetadata(dbName, metadata);
		logger.info("Created new database:" + dbName + "\t with retention period:" + retentionHours + " hours ("
				+ (retentionHours * 3600 / metadata.getTimeBucketSize()) + " buckets)");
	}

	@Override
	public void load() throws IOException {
		measurementMap = new ConcurrentHashMap<>();
		metadata = readMetadata(dbName);
		loadMeasurements(dbName, measurementMap, metadata);
	}

	protected DBMetadata readMetadata(String dbName) throws IOException {
		String path = dbDirectoryPath(dbName) + "/.md";
		File file = new File(path);
		if (!file.exists()) {
			return new DBMetadata(engine.getDefaultRetentionHours(), DiskMalloc.getBufIncrement(engine.getConf()),
					DiskMalloc.getFileIncrement(engine.getConf()), engine.getDefaultTimebucketSize());
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

	private void saveDBMetadata(String dbName, DBMetadata metadata) throws IOException {
		String dbDirectory = dbDirectoryPath(dbName);
		String line = new Gson().toJson(metadata);
		DiskStorageEngine.writeLineToFile(line, dbDirectory + "/.md");
	}

	protected void createDatabaseDirectory(String dbName) {
		String dbDirectory = dbDirectoryPath(dbName);
		File file = new File(dbDirectory);
		file.mkdirs();
	}

	protected String dbDirectoryPath(String dbName) {
		return engine.getDataDir(dbName) + "/" + dbName;
	}

	protected String dbIndexPath(String dbName) {
		return engine.getIndexDir(dbName) + "/" + dbName;
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
			futures.add(engine.getBgTaskPool().submit(() -> {
				try {
					measurement.configure(engine.getConf(), engine, metadata.getTimeBucketSize(), dbName,
							measurementName, dbIndexPath(dbName), dbDirectoryPath(dbName), metadata,
							engine.getBgTaskPool());
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

	@Override
	public Measurement getOrCreateMeasurement(String measurementName) throws IOException {
		Measurement measurement = measurementMap.get(measurementName);
		if (measurement == null) {
			synchronized (measurementMap) {
				if ((measurement = measurementMap.get(measurementName)) == null) {
					measurement = new PersistentMeasurement();
					measurement.configure(engine.getConf(), engine, metadata.getTimeBucketSize(), dbName,
							measurementName, dbIndexPath(dbName), dbDirectoryPath(dbName), metadata,
							engine.getBgTaskPool());
					measurementMap.put(measurementName, measurement);
					logger.info("Created new measurement:" + measurementName);
					// metricsMeasurementCounter.inc();
				}
			}
		}
		return measurement;
	}

	@Override
	public void dropMeasurement(String measurementName) throws IOException {
		synchronized (measurementMap) {
			Measurement measurement = measurementMap.remove(measurementName);
			measurement.close();
		}
	}

	@Override
	public void dropDatabase() throws Exception {
		for (Measurement measurement : measurementMap.values()) {
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
	}

	@Override
	public Map<String, Measurement> getMeasurementMap() {
		return measurementMap;
	}

	@Override
	public Collection<Measurement> getMeasurements() {
		return measurementMap.values();
	}

	@Override
	public Measurement getMeasurement(String measurementName) {
		return measurementMap.get(measurementName);
	}

	@Override
	public DBMetadata getDbMetadata() {
		return metadata;
	}

	public void updateRetentionPOlicy(String measurementName, int retentionHours) {
		synchronized (measurementMap) {
			if (measurementMap != null) {
				Measurement m = measurementMap.get(measurementName);
				if (m != null) {
					m.setRetentionHours(retentionHours);
				}
			}
		}
	}

	@Override
	public Set<String> keySet() {
		return measurementMap.keySet();
	}

	@Override
	public boolean containsMeasurement(String measurement) {
		return measurementMap.containsKey(measurement);
	}

	@Override
	public void updateRetention(int retentionHours) {
		// TODO Auto-generated method stub
	}

	@Override
	public int size() {
		return measurementMap.size();
	}

}