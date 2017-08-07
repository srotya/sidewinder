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
package com.srotya.sidewinder.core.storage.disk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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

import com.google.gson.Gson;
import com.srotya.sidewinder.core.aggregators.AggregationFunction;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.Archiver;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.SeriesQueryOutput;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.mem.TimeSeries;
import com.srotya.sidewinder.core.storage.mem.archival.NoneArchiver;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * Disk based timeseries {@link StorageEngine}
 * 
 * @author ambud
 */
public class DiskStorageEngine implements StorageEngine {

	private static final String INDEX_DIR = "index.dir";
	private static final String DATA_DIRS = "data.dir";
	private static final Logger logger = Logger.getLogger(DiskStorageEngine.class.getName());
	private Map<String, Map<String, Measurement>> databaseMap;
	private AtomicInteger counter = new AtomicInteger(0);
	private Map<String, DBMetadata> dbMetadataMap;
	private int defaultRetentionHours;
	private int defaultTimebucketSize;
	private Archiver archiver;
	private String compressionFQCN;
	private Map<String, String> conf;
	private String[] dataDirs;
	private String baseIndexDirectory;
	private ScheduledExecutorService bgTaskPool;

	@Override
	public void configure(Map<String, String> conf, ScheduledExecutorService bgTaskPool) throws IOException {
		this.conf = conf;
		this.bgTaskPool = bgTaskPool;
		this.defaultRetentionHours = Integer
				.parseInt(conf.getOrDefault(RETENTION_HOURS, String.valueOf(DEFAULT_RETENTION_HOURS)));
		logger.info("Setting default timeseries retention hours policy to:" + defaultRetentionHours);

		this.conf.put(StorageEngine.PERSISTENCE_DISK, "true");
		dataDirs = MiscUtils.splitAndNormalizeString(conf.getOrDefault(DATA_DIRS, "/tmp/sidewinder/metadata"));
		baseIndexDirectory = conf.getOrDefault(INDEX_DIR, "/tmp/sidewinder/index");
		for (String dataDir : dataDirs) {
			new File(dataDir).mkdirs();
		}
		new File(baseIndexDirectory).mkdirs();
		databaseMap = new ConcurrentHashMap<>();
		dbMetadataMap = new ConcurrentHashMap<>();

		try {
			archiver = (Archiver) Class.forName(conf.getOrDefault(ARCHIVER_CLASS, NoneArchiver.class.getName()))
					.newInstance();
			archiver.init(conf);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Failed to instantiate archiver", e);
		}
		compressionFQCN = conf.getOrDefault(COMPRESSION_CLASS, DEFAULT_COMPRESSION_CLASS);

		this.defaultTimebucketSize = Integer
				.parseInt(conf.getOrDefault(DEFAULT_BUCKET_SIZE, String.valueOf(DEFAULT_TIME_BUCKET_CONSTANT)));
		bgTaskPool.scheduleAtFixedRate(() -> {
			for (Entry<String, Map<String, Measurement>> measurementMap : databaseMap.entrySet()) {
				for (Entry<String, Measurement> measurementEntry : measurementMap.getValue().entrySet()) {
					Measurement value = measurementEntry.getValue();
					try {
						value.garbageCollector();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}, Integer.parseInt(conf.getOrDefault(GC_FREQUENCY, DEFAULT_GC_FREQUENCY)),
				Integer.parseInt(conf.getOrDefault(GC_DELAY, DEFAULT_GC_DELAY)), TimeUnit.MILLISECONDS);

		loadDatabases();
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int retentionHours) throws IOException {
		TimeSeries series = getOrCreateTimeSeries(dbName, measurementName, valueFieldName, tags, defaultTimebucketSize,
				true);
		series.setRetentionHours(retentionHours);
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, int retentionHours) {
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			Measurement seriesMap = measurementMap.get(measurementName);
			if (seriesMap != null) {
				for (TimeSeries series : seriesMap.getTimeSeries()) {
					series.setRetentionHours(retentionHours);
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
				TagIndex memTagIndex = measurement.getTagIndex();// getOrCreateTagIndex(dbName,
																	// measurementName);
				for (String entry : measurement.getTimeSeriesMap().keySet()) {
					TimeSeries series = measurement.get(entry);
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
			Predicate valuePredicate, AggregationFunction aggregationFunction) throws IOException {
		Set<SeriesQueryOutput> resultMap = new HashSet<>();
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			Measurement seriesMap = measurementMap.get(measurementName);
			if (seriesMap != null) {
				TagIndex memTagIndex = getOrCreateTagIndex(dbName, measurementName);
				Set<String> rowKeys = null;
				if (tagList == null || tagList.size() == 0) {
					rowKeys = seriesMap.getTimeSeriesMap().keySet();
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
					points = value.queryDataPoints(keys[0], seriesTags, startTime, endTime, valuePredicate);
					if (aggregationFunction != null) {
						points = aggregationFunction.aggregate(points);
					}
					if (points == null) {
						points = new ArrayList<>();
					}
					if (points.size() > 0) {
						SeriesQueryOutput seriesQueryOutput = new SeriesQueryOutput(measurementName, keys[0],
								seriesTags);
						seriesQueryOutput.setDataPoints(points);
						resultMap.add(seriesQueryOutput);
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
	public Map<String, Measurement> getOrCreateDatabase(String dbName) throws IOException {
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			synchronized (databaseMap) {
				if ((measurementMap = databaseMap.get(dbName)) == null) {
					measurementMap = new ConcurrentHashMap<>();
					databaseMap.put(dbName, measurementMap);
					createDatabaseDirectory(dbName);
					DBMetadata metadata = new DBMetadata();
					metadata.setRetentionHours(defaultRetentionHours);
					dbMetadataMap.put(dbName, metadata);
					saveDBMetadata(dbName, metadata);
					logger.info("Created new database:" + dbName + "\t with retention period:" + defaultRetentionHours
							+ " hours");
				}
			}
		}
		return measurementMap;
	}

	private void saveDBMetadata(String dbName, DBMetadata metadata) throws IOException {
		String dbDirectory = dbMetadataDirectoryPath(dbName);
		String line = new Gson().toJson(metadata);
		writeLineToFile(line, dbDirectory + ".md");
	}

	protected void createDatabaseDirectory(String dbName) {
		String dbDirectory = dbMetadataDirectoryPath(dbName);
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
		String path = dbMetadataDirectoryPath(dbName) + ".md";
		File file = new File(path);
		if (!file.exists()) {
			return new DBMetadata(defaultRetentionHours);
		}
		List<String> lines = MiscUtils.readAllLines(file);
		StringBuilder builder = new StringBuilder();
		for (String line : lines) {
			builder.append(line);
		}
		DBMetadata metadata = new Gson().fromJson(builder.toString(), DBMetadata.class);
		return metadata;
	}

	@Override
	public Map<String, Measurement> getOrCreateDatabase(String dbName, int retentionPolicy) throws IOException {
		Map<String, Measurement> map = getOrCreateDatabase(dbName);
		updateTimeSeriesRetentionPolicy(dbName, retentionPolicy);
		return map;
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
			synchronized (measurementMap) {
				if ((measurement = measurementMap.get(measurementName)) == null) {
					measurement = new PersistentMeasurement();
					measurementMap.put(measurementName, measurement);
					logger.info("Created new measurement:" + measurementName);
				}
			}
		}
		return measurement;
	}

	public String getDataDir(String dbName) {
		return dataDirs[dbName.hashCode() % dataDirs.length];
	}

	public String dbMetadataDirectoryPath(String dbName) {
		return getDataDir(dbName) + "/" + dbName;
	}

	protected void loadMeasurements(String dbName, Map<String, Measurement> measurementMap, DBMetadata metadata)
			throws IOException {
		File file = new File(dbMetadataDirectoryPath(dbName));
		if (!file.exists() || file.listFiles() == null) {
			return;
		}
		for (File measurementMdFile : file.listFiles()) {
			String measurementName = measurementMdFile.getName();
			Measurement measurement = new PersistentMeasurement();
			measurement.configure(conf, compressionFQCN, baseIndexDirectory, getDataDir(dbName), dbName,
					measurementName, metadata, bgTaskPool);
			measurementMap.put(measurementName, measurement);
			logger.info("Loading measurements:" + measurementName);
			measurement.loadTimeseriesFromMeasurementMetadata();
		}
	}

	@Override
	public TimeSeries getTimeSeries(String dbName, String measurementName, String valueFieldName, List<String> tags)
			throws IOException {
		Collections.sort(tags);

		String rowKey = constructRowKey(dbName, measurementName, valueFieldName, tags);

		// check and create database map
		Map<String, Measurement> dbMap = getOrCreateDatabase(dbName);

		// check and create measurement map
		Measurement measurementMap = getOrCreateMeasurement(dbMap, measurementName, dbName);

		// check and create timeseries
		TimeSeries timeSeries = measurementMap.get(rowKey);
		return timeSeries;
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int timeBucketSize, boolean fp) throws IOException {
		Collections.sort(tags);

		String rowKey = constructRowKey(dbName, measurementName, valueFieldName, tags);

		// check and create database map
		Map<String, Measurement> dbMap = getOrCreateDatabase(dbName);

		// check and create measurement map
		Measurement measurement = getOrCreateMeasurement(dbMap, measurementName, dbName);

		// check and create timeseries
		TimeSeries timeSeries = measurement.get(rowKey);
		if (timeSeries == null) {
			synchronized (measurement) {
				if ((timeSeries = measurement.get(rowKey)) == null) {
					timeSeries = measurement.getOrCreateTimeSeries(rowKey, timeBucketSize, fp, conf);
					logger.fine("Created new timeseries:" + timeSeries + " for measurement:" + measurementName + "\t"
							+ rowKey + "\t" + dbMetadataMap.get(dbName));
				}
			}
		}
		return timeSeries;
	}

	public static void writeLineToFile(String line, String filePath) throws IOException {
		File pth = new File(filePath);
		PrintWriter pr = new PrintWriter(new FileOutputStream(pth, false));
		pr.println(line);
		pr.close();
	}

	public static void appendLineToFile(String line, String filePath) throws IOException {
		File pth = new File(filePath);
		PrintWriter pr = new PrintWriter(new FileOutputStream(pth, true));
		pr.println(line);
		pr.close();
	}

	@Override
	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName)
			throws IOException {
		Map<String, Measurement> dbMap = getOrCreateDatabase(dbName);
		// check and create measurement map
		Measurement measurementMap = getOrCreateMeasurement(dbMap, measurementName, dbName);
		for (Entry<String, TimeSeries> entry : measurementMap.getTimeSeriesMap().entrySet()) {
			if (entry.getKey().startsWith(valueFieldName)) {
				return entry.getValue().isFp();
			}
		}
		throw NOT_FOUND_EXCEPTION;
	}

	public Set<String> getSeriesIdsWhereTags(String dbName, String measurementName, List<String> tags)
			throws IOException {
		Set<String> series = new HashSet<>();
		DiskTagIndex memTagLookupTable = (DiskTagIndex) getOrCreateTagIndex(dbName, measurementName);
		for (String tag : tags) {
			Set<String> keys = memTagLookupTable.searchRowKeysForTag(tag);
			if (keys != null) {
				series.addAll(keys);
			}
		}
		return series;
	}

	public Set<String> getTagFilteredRowKeys(String dbName, String measurementName, String valueFieldName,
			Filter<List<String>> tagFilterTree, List<String> rawTags) throws IOException {
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
				seriesTags = decodeStringToTags(getOrCreateTagIndex(dbName, measurementName), keys[1]);
			} else {
				seriesTags = new ArrayList<>();
			}
			if (!tagFilterTree.isRetain(seriesTags)) {
				iterator.remove();
			}
		}
		return filteredSeries;
	}

	@Override
	public TagIndex getOrCreateTagIndex(String dbName, String measurementName) throws IOException {
		Map<String, Measurement> lookupMap = databaseMap.get(dbName);
		return lookupMap.get(measurementName).getTagIndex();
	}

	@Override
	public boolean checkTimeSeriesExists(String dbName, String measurementName, String valueFieldName,
			List<String> tags) throws Exception {
		Collections.sort(tags);

		if (!checkIfExists(dbName)) {
			return false;
		}
		// check and create database map
		Map<String, Measurement> dbMap = getOrCreateDatabase(dbName);

		if (!checkIfExists(dbName, measurementName)) {
			return false;
		}
		// check and create measurement map
		Measurement measurementMap = getOrCreateMeasurement(dbMap, dbName, measurementName);

		String rowKey = constructRowKey(dbName, measurementName, valueFieldName, tags);
		// check and create timeseries
		TimeSeries timeSeries = measurementMap.get(rowKey);

		return timeSeries == null;
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
		synchronized (databaseMap) {
			databaseMap.remove(dbName);
			MiscUtils.delete(new File(dbMetadataDirectoryPath(dbName)));
		}
	}

	@Override
	public void dropMeasurement(String dbName, String measurementName) throws Exception {
		Map<String, Measurement> map = databaseMap.get(dbName);
		synchronized (map) {
			map.remove(measurementName);
		}
	}

	@Override
	public List<List<String>> getTagsForMeasurement(String dbName, String measurementName, String valueFieldName)
			throws Exception {
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			throw new ItemNotFoundException("Database " + dbName + " not found");
		}
		TagIndex tagLookupTable = measurementMap.get(measurementName).getTagIndex();
		if (tagLookupTable == null) {
			throw new ItemNotFoundException("Measurement " + measurementName + " not found");
		}
		Measurement map = databaseMap.get(dbName).get(measurementMap);
		Set<String> keySet = new HashSet<>();
		for (Entry<String, TimeSeries> entry : map.getTimeSeriesMap().entrySet()) {
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
			List<String> tags = decodeStringToTags(tagLookupTable, keys[1]);
			tagList.add(tags);
		}
		return tagList;
	}

	/**
	 * Function for unit testing
	 * 
	 * @param dbName
	 * @param measurementName
	 * @return
	 */
	protected Map<String, TimeSeries> getSeriesMap(String dbName, String measurementName) {
		return databaseMap.get(dbName).get(measurementName).getTimeSeriesMap();
	}

	@Override
	public void connect() throws IOException {
	}

	@Override
	public void disconnect() throws IOException {
		for (Entry<String, Map<String, Measurement>> measurementMap : databaseMap.entrySet()) {
			for (Entry<String, Measurement> seriesMap : measurementMap.getValue().entrySet()) {
				for (Entry<String, TimeSeries> series : seriesMap.getValue().getTimeSeriesMap().entrySet()) {
					series.getValue().close();
				}
			}
		}
		System.gc();
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
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			throw new ItemNotFoundException("Database " + dbName + " not found");
		}
		TagIndex memTagLookupTable = measurementMap.get(measurementName).getTagIndex();
		if (memTagLookupTable == null) {
			throw new ItemNotFoundException("Measurement " + measurementName + " not found");
		}
		return memTagLookupTable.getTags();
	}

	@Override
	public Set<String> getFieldsForMeasurement(String dbName, String measurementName) throws Exception {
		if (!checkIfExists(dbName, measurementName)) {
			throw NOT_FOUND_EXCEPTION;
		}
		Map<String, Measurement> measurementMap = databaseMap.get(dbName);
		if (measurementMap == null) {
			throw new ItemNotFoundException("Database " + dbName + " not found");
		}
		Measurement seriesMap = measurementMap.get(measurementName);
		if (seriesMap == null) {
			throw new ItemNotFoundException("Measurement " + measurementName + " not found");
		}
		Set<String> results = new HashSet<>();
		Set<String> keySet = seriesMap.getTimeSeriesMap().keySet();
		for (String key : keySet) {
			String[] splits = key.split(FIELD_TAG_SEPARATOR);
			if (splits.length == 2) {
				results.add(splits[0]);
			}
		}
		return results;
	}

	@Override
	public Map<String, DBMetadata> getDbMetadataMap() {
		return dbMetadataMap;
	}

}
