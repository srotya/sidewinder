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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.TimeSeriesBucket;
import com.srotya.sidewinder.core.storage.mem.TimeSeries;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class PersistentMeasurement implements Measurement {

	private static final Logger logger = Logger.getLogger(PersistentMeasurement.class.getName());
	private Map<String, TimeSeries> seriesMap;
	private DiskTagIndex tagIndex;
	private String compressionClass;
	private String measurementName;
	private String dbDataDirectory;
	private DBMetadata metadata;
	private Map<String, String> conf;
	private String dbName;
	private ScheduledExecutorService bgTaskPool;
	private List<RandomAccessFile> list;

	@Override
	public void configure(Map<String, String> conf, String indexDirectory, String dataDirectory,
			String compressionClass, String dbName, String measurementName, DBMetadata metadata,
			ScheduledExecutorService bgTaskPool) throws IOException {
		this.list = new ArrayList<>();
		this.conf = conf;
		this.dbDataDirectory = dataDirectory;
		this.dbName = dbName;
		this.metadata = metadata;
		this.bgTaskPool = bgTaskPool;
		this.seriesMap = new ConcurrentHashMap<>();
		this.tagIndex = new DiskTagIndex(indexDirectory, dbName, measurementName);
		this.compressionClass = compressionClass;
		this.measurementName = measurementName;
		String bucketMapPath = "";
		conf.put("data.dir", bucketMapPath);
		new File(bucketMapPath).mkdirs();
		bucketMapPath += "/.bucket";
		createMeasurementDirectory(dbName, measurementName);
	}
	
	@Override
	public void garbageCollector() throws IOException {
		for (Entry<String, TimeSeries> entry : seriesMap.entrySet()) {
			try {
				List<TimeSeriesBucket> garbage = entry.getValue().collectGarbage();
				for (TimeSeriesBucket timeSeriesBucket : garbage) {
					timeSeriesBucket.delete();
					logger.info("Collecting garbage for bucket:" + entry.getKey());
				}
				logger.info("Collecting garbage for time series:" + entry.getKey());
			} catch (IOException e) {
				logger.log(Level.SEVERE, "Error collecing garbage", e);
			}
		}
	}
	
	@Override
	public TimeSeries getOrCreateTimeSeries(String rowKey, int timeBucketSize, boolean fp, Map<String, String> conf)
			throws IOException {
		PersistentTimeSeries timeSeries = new PersistentTimeSeries(
				measurementDataDirectoryPath(dbName, measurementName), compressionClass,
				seriesId(measurementName, rowKey), metadata, timeBucketSize, fp, conf, bgTaskPool);
		seriesMap.put(rowKey, timeSeries);
		appendTimeseriesToMeasurementMetadata(measurementMetadataFilePath(dbName, measurementName), rowKey, fp,
				timeBucketSize);
		return timeSeries;
	}

	@Override
	public void delete() throws IOException {
		MiscUtils.delete(new File(measurementDataDirectoryPath(dbName, measurementName)));
	}

	@Override
	public void loadTimeseriesFromMeasurementMetadata() throws IOException {
		String measurementFilePath = measurementMetadataFilePath(dbName, measurementName);
		File file = new File(measurementFilePath);
		if (!file.exists()) {
			return;
		}
		List<String> seriesEntries = MiscUtils.readAllLines(file);
		List<Future<?>> futures = new ArrayList<>();
		for (String entry : seriesEntries) {
			String[] split = entry.split("\t");
			futures.add(bgTaskPool.submit(() -> {
				logger.fine("Loading Timeseries:" + seriesId(measurementName, split[0]));
				try {
					seriesMap.put(split[0],
							new PersistentTimeSeries(measurementDataDirectoryPath(dbName, measurementName),
									compressionClass, seriesId(measurementName, split[0]), metadata,
									Integer.parseInt(split[2]), Boolean.parseBoolean(split[1]), conf, bgTaskPool));
					logger.fine("Loaded Timeseries:" + seriesId(measurementName, split[0]));
				} catch (NumberFormatException | IOException e) {
					logger.log(Level.SEVERE, "Failed to load series:" + entry, e);
				}
			}));
		}
		for (Future<?> future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				logger.log(Level.SEVERE, "Failed to load series", e);
			}
		}
	}

	@Override
	public Collection<TimeSeries> getTimeSeries() {
		return seriesMap.values();
	}

	@Override
	public Map<String, TimeSeries> getTimeSeriesMap() {
		return seriesMap;
	}

	@Override
	public TagIndex getTagIndex() {
		return tagIndex;
	}

	@Override
	public TimeSeries get(String entry) {
		return seriesMap.get(entry);
	}

	private String measurementDataDirectoryPath(String dbName, String measurementName) {
		return dbDataDirectory + "/" + dbName + "/" + measurementName;
	}

	private String measurementMetadataFilePath(String dbName, String measurementName) {
		return dbDataDirectory + "/" + dbName + "/" + measurementName + "/.md";
	}

	public static String seriesId(String measurementName, String rowKey) {
		return measurementName + "_" + rowKey;
	}
	
	protected void createMeasurementDirectory(String dbName, String measurementName) throws IOException {
		String measurementDirectory = measurementDataDirectoryPath(dbName, measurementName);
		new File(measurementDirectory).mkdirs();
	}

	protected void appendTimeseriesToMeasurementMetadata(String measurementFilePath, String rowKey, boolean fp,
			int timeBucketSize) throws IOException {
		DiskStorageEngine.appendLineToFile(rowKey + "\t" + fp + "\t" + timeBucketSize, measurementFilePath);
	}

}
