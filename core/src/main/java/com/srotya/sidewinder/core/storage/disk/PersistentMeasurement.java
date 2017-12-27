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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.BufferObject;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.malloc.DiskMalloc;
import com.srotya.sidewinder.core.storage.malloc.Malloc;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class PersistentMeasurement implements Measurement {

	private static final Logger logger = Logger.getLogger(PersistentMeasurement.class.getName());
	private ReentrantLock lock = new ReentrantLock(false);
	private Map<String, Integer> seriesMap;
	private List<TimeSeries> seriesList;
	private TagIndex tagIndex;
	private String compressionCodec;
	private String compactionCodec;
	private String dataDirectory;
	private DBMetadata metadata;
	private Map<String, String> conf;
	private String dbName;
	private PrintWriter prMetadata;
	private String indexDirectory;
	// metrics
	private boolean enableMetricsCapture;
	private Counter metricsTimeSeriesCounter;
	private boolean useQueryPool;
	private String measurementName;
	private Malloc malloc;

	@Override
	public void configure(Map<String, String> conf, StorageEngine engine, String dbName, String measurementName,
			String indexDirectory, String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool)
			throws IOException {
		this.dbName = dbName;
		this.measurementName = measurementName;
		enableMetricsMonitoring(engine, bgTaskPool);
		this.conf = conf;
		this.useQueryPool = Boolean.parseBoolean(conf.getOrDefault(USE_QUERY_POOL, "true"));
		if (useQueryPool) {
			logger.fine("Query Pool enabled, datapoint queries will be parallelized");
		}
		this.dataDirectory = dataDirectory + "/" + measurementName;
		this.indexDirectory = indexDirectory + "/" + measurementName;
		createMeasurementDirectory();
		if (metadata == null) {
			throw new IOException("Metadata can't be null");
		}

		this.metadata = metadata;
		// this.seriesMap = (Map<String, Integer>)
		// DBMaker.memoryDirectDB().make().hashMap(measurementName).create();
		this.seriesMap = new ConcurrentHashMap<>(100_000);
		this.seriesList = new ArrayList<>(100_000);
		this.compressionCodec = conf.getOrDefault(StorageEngine.COMPRESSION_CODEC,
				StorageEngine.DEFAULT_COMPRESSION_CODEC);
		this.compactionCodec = conf.getOrDefault(StorageEngine.COMPACTION_CODEC,
				StorageEngine.DEFAULT_COMPACTION_CODEC);
		this.measurementName = measurementName;
		this.prMetadata = new PrintWriter(new FileOutputStream(new File(getMetadataPath()), true));
		this.tagIndex = new MappedSetTagIndex(this.indexDirectory, measurementName);
		malloc = new DiskMalloc();
		malloc.configure(conf, dataDirectory, measurementName, engine, bgTaskPool);
		loadTimeseriesFromMeasurements();
	}

	private void enableMetricsMonitoring(StorageEngine engine, ScheduledExecutorService bgTaskPool) {
		if (engine == null || bgTaskPool == null) {
			enableMetricsCapture = false;
			logger.warning("Metrics capture is disabled");
			return;
		}
		logger.info("Metrics capture is enabled");
		MetricsRegistryService reg = MetricsRegistryService.getInstance(engine, bgTaskPool);
		MetricRegistry metaops = reg.getInstance("metaops");
		metricsTimeSeriesCounter = metaops.counter("timeseries-counter");
		enableMetricsCapture = true;
	}

	private String getMetadataPath() {
		return dataDirectory + "/.md";
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException {
		Collections.sort(tags);
		String seriesId = constructSeriesId(valueFieldName, tags, tagIndex);
		TimeSeries timeSeries = getSeriesFromKey(seriesId);
		if (timeSeries == null) {
			lock.lock();
			if ((timeSeries = getSeriesFromKey(seriesId)) == null) {
				Measurement.indexRowKey(tagIndex, seriesId, tags);
				timeSeries = new TimeSeries(this, compressionCodec, compactionCodec, seriesId, timeBucketSize, metadata,
						fp, conf);
				int index = seriesList.size();
				seriesList.add(timeSeries);
				seriesMap.put(seriesId, index);
				appendTimeseriesToMeasurementMetadata(seriesId, fp, timeBucketSize);
				logger.fine("Created new timeseries:" + timeSeries + " for measurement:" + measurementName + "\t"
						+ seriesId + "\t" + metadata.getRetentionHours() + "\t" + seriesMap.size());
				if (enableMetricsCapture) {
					metricsTimeSeriesCounter.inc();
				}
			}
			lock.unlock();
		}
		return timeSeries;
	}

	@Override
	public Collection<TimeSeries> getTimeSeries() {
		return seriesList;
	}

	@Override
	public TagIndex getTagIndex() {
		return tagIndex;
	}

	protected void createMeasurementDirectory() throws IOException {
		new File(dataDirectory).mkdirs();
		new File(indexDirectory).mkdirs();
	}

	protected void appendTimeseriesToMeasurementMetadata(String seriesId, boolean fp, int timeBucketSize)
			throws IOException {
		DiskStorageEngine.appendLineToFile(seriesId + "\t" + fp + "\t" + timeBucketSize, prMetadata);
	}

	private void loadSeriesEntries(List<String> seriesEntries) {
		for (String entry : seriesEntries) {
			String[] split = entry.split("\t");
			String seriesId = split[0];
			logger.fine("Loading Timeseries:" + seriesId);
			try {
				String timeBucketSize = split[2];
				String isFp = split[1];
				TimeSeries timeSeries = new TimeSeries(this, compressionCodec, compactionCodec, seriesId,
						Integer.parseInt(timeBucketSize), metadata, Boolean.parseBoolean(isFp), conf);
				int index = seriesList.size();
				seriesList.add(timeSeries);
				seriesMap.put(seriesId, index);
				logger.fine("Intialized Timeseries:" + seriesId);
			} catch (NumberFormatException | IOException e) {
				logger.log(Level.SEVERE, "Failed to load series:" + entry, e);
			}
		}
	}

	@Override
	public void loadTimeseriesFromMeasurements() throws IOException {
		logger.info("Loading measurement:" + measurementName);
		String mdFilePath = getMetadataPath();
		File file = new File(mdFilePath);
		if (!file.exists()) {
			logger.warning("Metadata file missing for measurement:" + measurementName);
			return;
		}

		List<String> seriesEntries = MiscUtils.readAllLines(file);
		loadSeriesEntries(seriesEntries);

		Map<String, List<Entry<String, BufferObject>>> seriesBuffers = malloc.seriesBufferMap();
		for (String series : seriesMap.keySet()) {
			Integer seriesId = seriesMap.get(series);
			TimeSeries ts = seriesList.get(seriesId);
			List<Entry<String, BufferObject>> list = seriesBuffers.get(series);
			if (list != null) {
				try {
					ts.loadBucketMap(list);
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Failed to load bucket map for:" + series + ":" + measurementName, e);
				}
			}
		}
	}

	@Override
	public String getMeasurementName() {
		return measurementName;
	}

	@Override
	public Collection<String> getTags() throws IOException {
		return tagIndex.getTags();
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public void close() throws IOException {
		tagIndex.close();
		prMetadata.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "PersistentMeasurement [seriesMap=" + seriesMap + ", measurementName=" + measurementName + "]";
	}

	@Override
	public SortedMap<String, List<Writer>> createNewBucketMap(String seriesId) {
		return new ConcurrentSkipListMap<>();
	}

	@Override
	public ReentrantLock getLock() {
		return lock;
	}

	@Override
	public boolean useQueryPool() {
		return useQueryPool;
	}

	@Override
	public Set<String> getSeriesKeys() {
		return seriesMap.keySet();
	}

	@Override
	public TimeSeries getSeriesFromKey(String key) {
		Integer index = seriesMap.get(key);
		if (index == null) {
			return null;
		} else {
			return seriesList.get(index);
		}
	}

	@Override
	public String getDbName() {
		return dbName;
	}

	@Override
	public Malloc getMalloc() {
		return malloc;
	}
}
