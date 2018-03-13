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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.BufferObject;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Malloc;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.SeriesFieldMap;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class PersistentMeasurement implements Measurement {

	private static final String MD_SEPARATOR = "/";
	private static final Logger logger = Logger.getLogger(PersistentMeasurement.class.getName());
	private ReentrantLock lock = new ReentrantLock(false);
	private ReentrantLock mallocLock = new ReentrantLock(false);
	private Map<String, Integer> seriesMap;
	private List<SeriesFieldMap> seriesList;
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
	private boolean compactOnStart;

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
		this.compactOnStart = Boolean.parseBoolean(
				conf.getOrDefault(StorageEngine.COMPACTION_ON_START, StorageEngine.DEFAULT_COMPACTION_ON_START));
		this.measurementName = measurementName;
		this.prMetadata = new PrintWriter(new FileOutputStream(new File(getMetadataPath()), true));
		// this.tagIndex = new MappedSetTagIndex(this.indexDirectory, measurementName,
		// true, this);
		this.tagIndex = new MappedBitmapTagIndex(this.indexDirectory, measurementName, this);
		malloc = new DiskMalloc();
		malloc.configure(conf, dataDirectory, measurementName, engine, bgTaskPool, mallocLock);
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
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<Tag> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException {
		Collections.sort(tags, TAG_COMPARATOR);
		String seriesId = constructSeriesId(tags, tagIndex);
		int index = 0;
		SeriesFieldMap seriesFieldMap = getSeriesFromKey(seriesId);
		if (seriesFieldMap == null) {
			lock.lock();
			try {
				if ((seriesFieldMap = getSeriesFromKey(seriesId)) == null) {
					index = seriesList.size();
					Measurement.indexRowKey(tagIndex, index, tags);
					seriesFieldMap = new SeriesFieldMap(seriesId);
					seriesList.add(seriesFieldMap);
					seriesMap.put(seriesId, index);
					if (enableMetricsCapture) {
						metricsTimeSeriesCounter.inc();
					}
					logger.fine("Created new series:" + seriesId + "\t");
				} else {
					index = seriesMap.get(seriesId);
				}
			} finally {
				lock.unlock();
			}
		} else {
			index = seriesMap.get(seriesId);
		}

		TimeSeries series = seriesFieldMap.get(valueFieldName);
		if (series == null) {
			lock.lock();
			try {
				if ((series = seriesFieldMap.get(valueFieldName)) == null) {
					String seriesId2 = seriesId + SERIESID_SEPARATOR + valueFieldName;
					series = new TimeSeries(this, compressionCodec, compactionCodec, seriesId2, timeBucketSize,
							metadata, fp, conf);
					seriesFieldMap.addSeries(valueFieldName, series);
					appendTimeseriesToMeasurementMetadata(seriesId2, fp, timeBucketSize, index);
					logger.fine("Created new timeseries:" + seriesFieldMap + " for measurement:" + measurementName
							+ "\t" + seriesId + "\t" + metadata.getRetentionHours() + "\t" + seriesList.size());
				}
			} finally {
				lock.unlock();
			}
		}

		return series;
	}

	@Override
	public TagIndex getTagIndex() {
		return tagIndex;
	}

	protected void createMeasurementDirectory() throws IOException {
		new File(dataDirectory).mkdirs();
		new File(indexDirectory).mkdirs();
	}

	protected void appendTimeseriesToMeasurementMetadata(String seriesId, boolean fp, int timeBucketSize, int idx)
			throws IOException {
		String line = seriesId + MD_SEPARATOR + fp + MD_SEPARATOR + timeBucketSize + MD_SEPARATOR
				+ Integer.toHexString(idx);
		DiskStorageEngine.appendLineToFile(line, prMetadata);
	}

	private void loadSeriesEntries(List<String> seriesEntries) {
		Collections.sort(seriesEntries, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return Integer.compare(Integer.parseInt(o1.split(MD_SEPARATOR)[3], 16),
						Integer.parseInt(o2.split(MD_SEPARATOR)[3], 16));
			}
		});
		SortedSet<String> set = new TreeSet<>();
		seriesEntries.stream().forEach(e -> set.add(e.split(MD_SEPARATOR)[3]));
		try {
			for (String entry : seriesEntries) {
				loadEntry(entry);
			}
		} catch (Exception e) {
			System.out.println(set.size() + "  " + Integer.parseInt(set.last(), 16));
			int i = 0;
			for (String s : set) {
				System.out.println(i++ + " s:" + Integer.parseInt(s, 16));
			}
			throw e;
		}
	}

	private void loadEntry(String entry) {
		String[] split = entry.split(MD_SEPARATOR);
		String seriesId = split[0];
		logger.fine("Loading Timeseries:" + seriesId);
		try {
			String timeBucketSize = split[2];
			String isFp = split[1];
			TimeSeries timeSeries = new TimeSeries(this, compressionCodec, compactionCodec, seriesId,
					Integer.parseInt(timeBucketSize), metadata, Boolean.parseBoolean(isFp), conf);
			String[] split2 = seriesId.split(SERIESID_SEPARATOR);

			String valueField = split2[1];
			seriesId = split2[0];

			Integer seriesIdx = seriesMap.get(seriesId);
			SeriesFieldMap m = null;
			if (seriesIdx == null) {
				seriesIdx = Integer.parseInt(split[3], 16);
				m = new SeriesFieldMap(seriesId);
				seriesMap.put(seriesId, seriesIdx);
				seriesList.add(seriesIdx, m);
			} else {
				m = seriesList.get(seriesIdx);
			}
			m.addSeries(valueField, timeSeries);
			logger.fine("Intialized Timeseries:" + seriesId);
		} catch (NumberFormatException | IOException e) {
			logger.log(Level.SEVERE, "Failed to load series:" + entry, e);
		}
	}

	@Override
	public void loadTimeseriesFromMeasurements() throws IOException {
		String mdFilePath = getMetadataPath();
		File file = new File(mdFilePath);
		if (!file.exists()) {
			logger.warning("Metadata file missing for measurement:" + measurementName);
			return;
		} else {
			logger.fine("Metadata file exists:" + file.getAbsolutePath());
		}

		List<String> seriesEntries = MiscUtils.readAllLines(file);
		try {
			loadSeriesEntries(seriesEntries);
		} catch (Exception e) {
			throw new IOException(e);
		}

		Map<String, List<Entry<String, BufferObject>>> seriesBuffers = malloc.seriesBufferMap();
		for (Entry<String, List<Entry<String, BufferObject>>> entry : seriesBuffers.entrySet()) {
			String[] split = entry.getKey().split(SERIESID_SEPARATOR);
			Integer seriesId = seriesMap.get(split[0]);
			SeriesFieldMap ts = seriesList.get(seriesId);
			List<Entry<String, BufferObject>> list = entry.getValue();
			if (list != null) {
				try {
					ts.get(split[1]).loadBucketMap(list);
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Failed to load bucket map for:" + entry.getKey() + ":" + measurementName,
							e);
				}
			}
		}
		if (compactOnStart) {
			compact();
		}
		logger.info("Loaded measurement:" + measurementName);
	}

	@Override
	public String getMeasurementName() {
		return measurementName;
	}

	@Override
	public Collection<String> getTagKeys() throws IOException {
		return tagIndex.getTagKeys();
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public void close() throws IOException {
		malloc.close();
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
		return new HashSet<>(seriesMap.keySet());
	}

	@Override
	public SeriesFieldMap getSeriesFromKey(String key) {
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

	@Override
	public Collection<SeriesFieldMap> getSeriesList() {
		return seriesList;
	}

	public List<SeriesFieldMap> getSeriesListAsList() {
		return seriesList;
	}
}
