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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import com.srotya.sidewinder.core.aggregators.AggregationFunction;
import com.srotya.sidewinder.core.filters.AnyFilter;
import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.Archiver;
import com.srotya.sidewinder.core.storage.mem.TimeSeries;
import com.srotya.sidewinder.core.storage.mem.archival.NoneArchiver;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

/**
 * Disk based timeseries {@link StorageEngine}
 * 
 * @author ambud
 */
public class DiskStorageEngine implements StorageEngine {

	public static final String MEM_COMPRESSION_CLASS = "disk.compression.class";
	public static final int DEFAULT_TIME_BUCKET_CONSTANT = 4096;
	public static final String RETENTION_HOURS = "default.series.retention.hours";
	public static final int DEFAULT_RETENTION_HOURS = (int) Math
			.ceil((((double) DEFAULT_TIME_BUCKET_CONSTANT) * 24 * 7 / 60) / 60);
	private static final String FIELD_TAG_SEPARATOR = "#";
	private static final String TAG_SEPARATOR = "_";
	private static final Logger logger = Logger.getLogger(DiskStorageEngine.class.getName());
	private static ItemNotFoundException NOT_FOUND_EXCEPTION = new ItemNotFoundException("Item not found");
	private static RejectException FP_MISMATCH_EXCEPTION = new RejectException("Floating point mismatch");
	private static RejectException INVALID_DATAPOINT_EXCEPTION = new RejectException(
			"Datapoint is missing required values");
	private Map<String, Map<String, SortedMap<String, TimeSeries>>> databaseMap;
	private AtomicInteger counter = new AtomicInteger(0);
	private Map<String, Map<String, DiskTagIndex>> tagLookupTable;
	private Map<String, Integer> databaseRetentionPolicyMap;
	private int defaultRetentionHours;
	private Archiver archiver;
	private String compressionFQCN;
	private Map<String, String> conf;
	private String dataDir;
	private String baseIndexDirectory;

	@Override
	public void configure(Map<String, String> conf, ScheduledExecutorService bgTaskPool) throws IOException {
		this.conf = conf;
		this.defaultRetentionHours = Integer
				.parseInt(conf.getOrDefault(RETENTION_HOURS, String.valueOf(DEFAULT_RETENTION_HOURS)));
		logger.info("Setting default timeseries retention hours policy to:" + defaultRetentionHours);

		this.conf.put(StorageEngine.PERSISTENCE_DISK, "true");
		dataDir = conf.getOrDefault("data.dir", "/tmp/sidewinder/metadata");
		baseIndexDirectory = conf.getOrDefault("index.dir", "/tmp/sidewinder/index");
		new File(dataDir).mkdirs();
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
		compressionFQCN = conf.getOrDefault(MEM_COMPRESSION_CLASS,
				"com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter");

		Executors.newSingleThreadScheduledExecutor(new BackgrounThreadFactory("sidewinder-gc"))
				.scheduleAtFixedRate(() -> {
					for (Entry<String, Map<String, SortedMap<String, TimeSeries>>> measurementMap : databaseMap
							.entrySet()) {
						// String db = measurementMap.getKey();
						for (Entry<String, SortedMap<String, TimeSeries>> measurementEntry : measurementMap.getValue()
								.entrySet()) {
							// String measurement = measurementEntry.getKey();
							for (Entry<String, TimeSeries> entry : measurementEntry.getValue().entrySet()) {
								entry.getValue().collectGarbage();
							}
						}
					}
				}, 500, 60, TimeUnit.SECONDS);

		loadDatabases();
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int retentionHours) throws IOException {
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
				DiskTagIndex memTagIndex = getOrCreateMemTagIndex(dbName, measurementName);
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
	public void start() throws Exception {
		connect();
	}

	@Override
	public void stop() throws Exception {
		disconnect();
	}

	@Override
	public Map<String, List<DataPoint>> queryDataPoints(String dbName, String measurementName, String valueFieldName,
			long startTime, long endTime, List<String> tags, Predicate valuePredicate) throws IOException {
		return queryDataPoints(dbName, measurementName, valueFieldName, startTime, endTime, tags,
				new AnyFilter<List<String>>(), valuePredicate, null);
	}

	@Override
	public Map<String, List<DataPoint>> queryDataPoints(String dbName, String measurementName, String valueFieldName,
			long startTime, long endTime, List<String> tagList, Filter<List<String>> tagFilter,
			Predicate valuePredicate, AggregationFunction aggregationFunction) throws IOException {
		Map<String, List<DataPoint>> resultMap = new HashMap<>();
		Map<String, SortedMap<String, TimeSeries>> measurementMap = databaseMap.get(dbName);
		if (measurementMap != null) {
			SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
			if (seriesMap != null) {
				DiskTagIndex memTagIndex = getOrCreateMemTagIndex(dbName, measurementName);
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
					points = value.queryDataPoints(keys[0], seriesTags, startTime, endTime, valuePredicate);
					if (aggregationFunction != null) {
						points = aggregationFunction.aggregate(points);
					}
					if (points == null) {
						points = new ArrayList<>();
					}
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

	public static String encodeTagsToString(DiskTagIndex tagLookupTable, List<String> tags) throws IOException {
		StringBuilder builder = new StringBuilder(tags.size() * 5);
		for (String tag : tags) {
			builder.append(tagLookupTable.createEntry(tag));
			builder.append(TAG_SEPARATOR);
		}
		return builder.toString();
	}

	public static List<String> decodeStringToTags(DiskTagIndex tagLookupTable, String tagString) {
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
					createDatabaseDirectory(dbName);
					databaseRetentionPolicyMap.put(dbName, defaultRetentionHours);
					logger.info("Created new database:" + dbName + "\t with retention period:" + defaultRetentionHours
							+ " hours");
				}
			}
		}
		return measurementMap;
	}

	protected void createDatabaseDirectory(String dbName) {
		String dbDirectory = dbMetadataDirectoryPath(dbName);
		File file = new File(dbDirectory);
		file.mkdirs();
	}

	protected void loadDatabases() throws IOException {
		File mdDir = new File(dataDir);
		if (!mdDir.exists()) {
			return;
		}
		File[] dbs = mdDir.listFiles();
		for (File db : dbs) {
			Map<String, SortedMap<String, TimeSeries>> measurementMap = new ConcurrentHashMap<>();
			String dbName = db.getName();
			databaseMap.put(dbName, measurementMap);
			databaseRetentionPolicyMap.put(dbName, defaultRetentionHours);
			logger.info("Loading database:" + dbName);
			loadMeasurements(dbName, measurementMap);
		}
	}

	@Override
	public Map<String, SortedMap<String, TimeSeries>> getOrCreateDatabase(String dbName, int retentionPolicy) {
		Map<String, SortedMap<String, TimeSeries>> map = getOrCreateDatabase(dbName);
		updateTimeSeriesRetentionPolicy(dbName, retentionPolicy);
		return map;
	}

	@Override
	public SortedMap<String, TimeSeries> getOrCreateMeasurement(String dbName, String measurementName)
			throws IOException {
		Map<String, SortedMap<String, TimeSeries>> measurementMap = getOrCreateDatabase(dbName);
		return getOrCreateMeasurement(measurementMap, measurementName, dbName);
	}

	protected SortedMap<String, TimeSeries> getOrCreateMeasurement(
			Map<String, SortedMap<String, TimeSeries>> measurementMap, String measurementName, String dbName)
			throws IOException {
		SortedMap<String, TimeSeries> seriesMap = measurementMap.get(measurementName);
		if (seriesMap == null) {
			synchronized (measurementMap) {
				if ((seriesMap = measurementMap.get(measurementName)) == null) {
					seriesMap = new ConcurrentSkipListMap<>();
					createMeasurementDirectory(dbName, measurementName);
					measurementMap.put(measurementName, seriesMap);
					logger.info("Created new measurement:" + measurementName);
				}
			}
		}
		return seriesMap;
	}

	protected void createMeasurementDirectory(String dbName, String measurementName) throws IOException {
		String measurementDirectory = measurementMetadataDirectoryPath(dbName, measurementName);
		new File(measurementDirectory).mkdirs();
	}

	public String dbMetadataDirectoryPath(String dbName) {
		return dataDir + "/" + dbName;
	}

	protected void loadMeasurements(String dbName, Map<String, SortedMap<String, TimeSeries>> measurementMap)
			throws IOException {
		File file = new File(dbMetadataDirectoryPath(dbName));
		if (!file.exists()) {
			return;
		}
		for (File measurementMdFile : file.listFiles()) {
			SortedMap<String, TimeSeries> seriesMap = new ConcurrentSkipListMap<>();
			String measurementName = measurementMdFile.getName();
			measurementMap.put(measurementName, seriesMap);
			logger.info("Loading measurements:" + measurementName);
			loadTimeseriesFromMeasurementMetadata(measurementMetadataFilePath(dbName, measurementName), dbName,
					measurementName, seriesMap);
		}
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int timeBucketSize, boolean fp) throws IOException {
		Collections.sort(tags);

		String rowKey = constructRowKey(dbName, measurementName, valueFieldName, tags);

		// check and create database map
		Map<String, SortedMap<String, TimeSeries>> dbMap = getOrCreateDatabase(dbName);

		// check and create measurement map
		SortedMap<String, TimeSeries> measurementMap = getOrCreateMeasurement(dbMap, measurementName, dbName);

		// check and create timeseries
		TimeSeries timeSeries = measurementMap.get(rowKey);
		if (timeSeries == null) {
			synchronized (measurementMap) {
				if ((timeSeries = measurementMap.get(rowKey)) == null) {
					timeSeries = new PersistentTimeSeries(measurementMetadataDirectoryPath(dbName, measurementName),
							compressionFQCN, seriesId(measurementName, rowKey), databaseRetentionPolicyMap.get(dbName),
							timeBucketSize, fp, conf);
					measurementMap.put(rowKey, timeSeries);
					appendTimeseriesToMeasurementMetadata(measurementMetadataFilePath(dbName, measurementName), rowKey,
							fp, timeBucketSize);
					logger.fine("Created new timeseries:" + timeSeries + " for measurement:" + measurementName + "\t"
							+ rowKey + "\t" + databaseRetentionPolicyMap.get(dbName));
				}
			}
		}
		return timeSeries;
	}

	private String measurementMetadataDirectoryPath(String dbName, String measurementName) {
		return dataDir + "/" + dbName + "/" + measurementName;
	}

	private String measurementMetadataFilePath(String dbName, String measurementName) {
		return dataDir + "/" + dbName + "/" + measurementName + "/.md";
	}

	public static String seriesId(String measurementName, String rowKey) {
		return measurementName + "_" + rowKey;
	}

	protected void loadTimeseriesFromMeasurementMetadata(String measurementFilePath, String dbName,
			String measurementName, SortedMap<String, TimeSeries> measurementMap) throws IOException {
		File file = new File(measurementFilePath);
		if (!file.exists()) {
			return;
		}
		List<String> seriesEntries = Files.readAllLines(file.toPath());
		for (String entry : seriesEntries) {
			String[] split = entry.split("\t");
			logger.fine("Loading Timeseries:" + seriesId(measurementName, split[0]));
			measurementMap.put(split[0],
					new PersistentTimeSeries(measurementMetadataDirectoryPath(dbName, measurementName), compressionFQCN,
							seriesId(measurementName, split[0]), databaseRetentionPolicyMap.get(dbName),
							Integer.parseInt(split[2]), Boolean.parseBoolean(split[1]), conf));
		}
	}

	public static void appendLineToFile(String line, String filePath) throws IOException {
		File pth = new File(filePath);
		PrintWriter pr = new PrintWriter(new FileOutputStream(pth, true));
		pr.println(line);
		pr.close();
	}

	protected void appendTimeseriesToMeasurementMetadata(String measurementFilePath, String rowKey, boolean fp,
			int timeBucketSize) throws IOException {
		appendLineToFile(rowKey + "\t" + fp + "\t" + timeBucketSize, measurementFilePath);

	}

	@Override
	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName)
			throws IOException {
		Map<String, SortedMap<String, TimeSeries>> dbMap = getOrCreateDatabase(dbName);
		// check and create measurement map
		SortedMap<String, TimeSeries> measurementMap = getOrCreateMeasurement(dbMap, measurementName, dbName);
		SortedMap<String, TimeSeries> subMap = measurementMap.subMap(valueFieldName,
				valueFieldName + Character.MAX_VALUE);
		if (!subMap.isEmpty()) {
			return subMap.values().iterator().next().isFp();
		} else {
			throw NOT_FOUND_EXCEPTION;
		}
	}

	protected String constructRowKey(String dbName, String measurementName, String valueFieldName, List<String> tags)
			throws IOException {
		DiskTagIndex memTagLookupTable = getOrCreateMemTagIndex(dbName, measurementName);
		String encodeTagsToString = encodeTagsToString(memTagLookupTable, tags);
		StringBuilder rowKeyBuilder = new StringBuilder(valueFieldName.length() + 1 + encodeTagsToString.length());
		rowKeyBuilder.append(valueFieldName);
		rowKeyBuilder.append(FIELD_TAG_SEPARATOR);
		rowKeyBuilder.append(encodeTagsToString);
		String rowKey = rowKeyBuilder.toString();
		indexRowKey(memTagLookupTable, rowKey, tags);
		return rowKey;
	}

	public Set<String> getSeriesIdsWhereTags(String dbName, String measurementName, List<String> tags)
			throws IOException {
		Set<String> series = new HashSet<>();
		DiskTagIndex memTagLookupTable = getOrCreateMemTagIndex(dbName, measurementName);
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

	protected void indexRowKey(DiskTagIndex memTagLookupTable, String rowKey, List<String> tags) throws IOException {
		for (String tag : tags) {
			memTagLookupTable.index(tag, rowKey);
		}
	}

	protected DiskTagIndex getOrCreateMemTagIndex(String dbName, String measurementName) throws IOException {
		Map<String, DiskTagIndex> lookupMap = tagLookupTable.get(dbName);
		if (lookupMap == null) {
			synchronized (tagLookupTable) {
				if ((lookupMap = tagLookupTable.get(dbName)) == null) {
					lookupMap = new ConcurrentHashMap<>();
					tagLookupTable.put(dbName, lookupMap);
				}
			}
		}

		DiskTagIndex memTagLookupTable = lookupMap.get(measurementName);
		if (memTagLookupTable == null) {
			synchronized (lookupMap) {
				if ((memTagLookupTable = lookupMap.get(measurementName)) == null) {
					memTagLookupTable = new DiskTagIndex(baseIndexDirectory, dbName, measurementName);
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
		synchronized (databaseMap) {
			databaseMap.remove(dbName);
			FileUtils.deleteDirectory(new File(dbMetadataDirectoryPath(dbName)));
		}
	}

	@Override
	public void dropMeasurement(String dbName, String measurementName) throws Exception {
		Map<String, SortedMap<String, TimeSeries>> map = databaseMap.get(dbName);
		synchronized (map) {
			map.remove(measurementName);
			FileUtils.deleteDirectory(new File(measurementMetadataDirectoryPath(dbName, measurementName)));
		}
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
		Map<String, DiskTagIndex> measurementMap = tagLookupTable.get(dbName);
		if (measurementMap == null) {
			throw new ItemNotFoundException("Database " + dbName + " not found");
		}
		DiskTagIndex memTagLookupTable = measurementMap.get(measurementName);
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
