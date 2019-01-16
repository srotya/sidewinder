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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.BufferObject;
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.ByteString.ByteStringCache;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Malloc;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class PersistentMeasurement implements Measurement {

	private static final int SERIES_MD_IDX = 1;
	private static final String MD_SEPARATOR = "~";
	private static final Logger logger = Logger.getLogger(PersistentMeasurement.class.getName());
	private ReentrantLock lock = new ReentrantLock(false);
	private ReentrantLock mallocLock = new ReentrantLock(false);
	private Map<ByteString, Integer> seriesMap;
	private SortedMap<String, Boolean> fieldTypeMap;
	private List<Series> seriesList;
	private TagIndex tagIndex;
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
	private int timeBucketSize;
	private AtomicInteger retentionBuckets;
	private PrintWriter prFieldMetadata;
	private ByteStringCache fieldCache;
	private Counter metricsCompactionCounter;
	private Counter metricsCleanupBufferCounter;

	@Override
	public void configure(Map<String, String> conf, StorageEngine engine, int defaultTimeBucketSize, String dbName,
			String measurementName, String indexDirectory, String dataDirectory, DBMetadata metadata,
			ScheduledExecutorService bgTaskPool) throws IOException {
		this.timeBucketSize = defaultTimeBucketSize;
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
		this.fieldCache = ByteStringCache.instance();
		this.seriesMap = new ConcurrentHashMap<>(100_000);
		this.fieldTypeMap = new ConcurrentSkipListMap<>();
		this.seriesList = new ArrayList<>(100_000);
		this.compactOnStart = Boolean.parseBoolean(
				conf.getOrDefault(StorageEngine.COMPACTION_ON_START, StorageEngine.DEFAULT_COMPACTION_ON_START));
		this.measurementName = measurementName;
		this.prMetadata = new PrintWriter(new FileOutputStream(new File(getMetadataPath()), true));
		this.prFieldMetadata = new PrintWriter(new FileOutputStream(new File(getFieldMetadataPath()), true));

		this.retentionBuckets = new AtomicInteger(0);
		setRetentionHours(metadata.getRetentionHours());

		this.tagIndex = new MappedBitmapTagIndex();
		this.tagIndex.configure(getConf(), indexDirectory, this);
		malloc = new DiskMalloc();
		malloc.configure(conf, dataDirectory, measurementName, engine, bgTaskPool, mallocLock);
		loadTimeseriesInMeasurements();
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
		metricsCompactionCounter = metaops.counter("compaction-counter");
		metricsCleanupBufferCounter = metaops.counter("cleanbuf-counter");
		enableMetricsCapture = true;
	}

	private String getMetadataPath() {
		return dataDirectory + "/.md";
	}

	private String getFieldMetadataPath() {
		return dataDirectory + "/.field";
	}

	@Override
	public ByteStringCache getFieldCache() {
		return fieldCache;
	}

	@Override
	public Map<ByteString, Integer> getSeriesMap() {
		return seriesMap;
	}

	@Override
	public TagIndex getTagIndex() {
		return tagIndex;
	}

	protected void createMeasurementDirectory() throws IOException {
		new File(dataDirectory).mkdirs();
		new File(indexDirectory).mkdirs();
	}

	@Override
	public synchronized void appendTimeseriesToMeasurementMetadata(ByteString fieldId, int idx) throws IOException {
		String line = fieldId.toString() + MD_SEPARATOR + Integer.toHexString(idx);
		DiskStorageEngine.appendLineToFile(line, prMetadata);
	}

	@Override
	public synchronized void appendFieldMetadata(String valueFieldName, boolean fp) throws IOException {
		DiskStorageEngine.appendLineToFile(valueFieldName + MD_SEPARATOR + fp, prFieldMetadata);
	}

	private void loadSeriesEntries(List<String> seriesEntries) {
		Collections.sort(seriesEntries, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				try {
					return Integer.compare(Integer.parseInt(o1.split(MD_SEPARATOR)[SERIES_MD_IDX], 16),
							Integer.parseInt(o2.split(MD_SEPARATOR)[SERIES_MD_IDX], 16));
				} catch (Exception e) {
					throw new RuntimeException("Bad entry:\n" + o1 + "\n" + o2 + " ");
				}
			}
		});
		SortedSet<String> set = new TreeSet<>();
		seriesEntries.stream().forEach(e -> set.add(e.split(MD_SEPARATOR)[SERIES_MD_IDX]));
		try {
			for (String entry : seriesEntries) {
				loadEntry(entry);
			}
		} catch (Exception e) {
			logger.severe(set.size() + "  " + Integer.parseInt(set.last(), 16));
			int i = 0;
			for (String s : set) {
				logger.severe(i++ + " s:" + Integer.parseInt(s, 16));
			}
			throw e;
		}
	}

	private void loadEntry(String entry) {
		String[] split = entry.split(MD_SEPARATOR);
		String fieldId = split[0];
		logger.fine("Loading Timeseries:" + fieldId);
		try {
			String[] split2 = fieldId.split(SERIESID_SEPARATOR);
			String seriesId = split2[0];
			ByteString key = new ByteString(seriesId);

			Integer seriesIdx = seriesMap.get(key);
			Series series = null;
			if (seriesIdx == null) {
				seriesIdx = Integer.parseInt(split[SERIES_MD_IDX], 16);
				series = new Series(key, seriesIdx);
				seriesMap.put(key, seriesIdx);
				seriesList.add(seriesIdx, series);
			} else {
				series = seriesList.get(seriesIdx);
			}
			if (enableMetricsCapture) {
				metricsTimeSeriesCounter.inc();
			}
			logger.fine("Intialized Timeseries:" + seriesId);
		} catch (NumberFormatException e) {
			logger.log(Level.SEVERE, "Failed to load series:" + entry, e);
		}
	}

	@Override
	public void loadTimeseriesInMeasurements() throws IOException {
		String fieldFilePath = getFieldMetadataPath();
		File file = new File(fieldFilePath);
		if (!file.exists()) {
			logger.warning("Field file missing for measurement:" + measurementName);
			return;
		} else {
			logger.fine("Field file exists:" + file.getAbsolutePath());
		}
		List<String> fieldEntries = MiscUtils.readAllLines(file);
		loadFieldList(fieldEntries);

		String mdFilePath = getMetadataPath();
		file = new File(mdFilePath);
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
			e.printStackTrace();
			throw new IOException(e);
		}

		ByteStringCache localCache = ByteStringCache.instance();
		Map<ByteString, List<Entry<Integer, BufferObject>>> seriesBuffers = malloc.seriesBufferMap();
		for (Entry<ByteString, List<Entry<Integer, BufferObject>>> entry : seriesBuffers.entrySet()) {
			ByteString[] split = entry.getKey().split(SERIESID_SEPARATOR);
			ByteString seriesId = localCache.get(split[0]);
			Integer seriesIndex = seriesMap.get(seriesId);
			Series series = seriesList.get(seriesIndex);

			if (series.getSeriesId() != seriesId) {
				seriesMap.put(seriesId, seriesIndex);
				series.setSeriesId(seriesId);
			}

			List<Entry<Integer, BufferObject>> list = entry.getValue();
			if (list != null) {
				try {
					String fieldName = split[1].toString();
					series.loadBuffers(this, fieldName, list, this.getConf());
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

	private void loadFieldList(List<String> fieldEntries) {
		for (String field : fieldEntries) {
			String[] split = field.split(MD_SEPARATOR);
			fieldTypeMap.put(split[0], Boolean.parseBoolean(split[1]));
		}
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
	public ReentrantLock getLock() {
		return lock;
	}

	@Override
	public boolean useQueryPool() {
		return useQueryPool;
	}

	@Override
	public Set<ByteString> getSeriesKeys() {
		Set<ByteString> hashSet = new HashSet<>();
		for (ByteString str : seriesMap.keySet()) {
			hashSet.add(str);
		}
		return hashSet;
	}

	@Override
	public String getDbName() {
		return dbName;
	}

	@Override
	public Malloc getMalloc() {
		return malloc;
	}

	public DBMetadata getMetadata() {
		return metadata;
	}

	@Override
	public List<Series> getSeriesList() {
		return seriesList;
	}

	public List<Series> getSeriesListAsList() {
		return seriesList;
	}

	@Override
	public int getTimeBucketSize() {
		return timeBucketSize;
	}

	@Override
	public Map<String, String> getConf() {
		return conf;
	}

	@Override
	public boolean isEnableMetricsCapture() {
		return enableMetricsCapture;
	}

	@Override
	public Counter getMetricsTimeSeriesCounter() {
		return metricsTimeSeriesCounter;
	}

	@Override
	public AtomicInteger getRetentionBuckets() {
		return retentionBuckets;
	}

	@Override
	public SortedMap<String, Boolean> getFieldTypeMap() {
		return fieldTypeMap;
	}

	@Override
	public Counter getMetricsCleanupBufferCounter() {
		return metricsCleanupBufferCounter;
	}

	@Override
	public Counter getMetricsCompactionCounter() {
		return metricsCompactionCounter;
	}

}
