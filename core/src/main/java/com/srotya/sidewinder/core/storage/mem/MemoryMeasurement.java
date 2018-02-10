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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Malloc;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.SeriesFieldMap;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.compression.Writer;

/**
 * @author ambud
 */
public class MemoryMeasurement implements Measurement {

	private static Logger logger = Logger.getLogger(MemoryMeasurement.class.getName());
	private ReentrantLock lock = new ReentrantLock(false);
	private String measurementName;
	private DBMetadata metadata;
	private Map<String, SeriesFieldMap> seriesMap;
	private MemTagIndex tagIndex;
	private String compressionCodec;
	private String compactionCodec;
	private boolean useQueryPool;
	private String dbName;
	private Malloc malloc;

	@Override
	public void configure(Map<String, String> conf, StorageEngine engine, String dbName, String measurementName,
			String baseIndexDirectory, String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool)
			throws IOException {
		this.dbName = dbName;
		this.measurementName = measurementName;
		this.metadata = metadata;
		this.tagIndex = new MemTagIndex(MetricsRegistryService.getInstance(engine, bgTaskPool).getInstance("request"));
		this.seriesMap = new ConcurrentHashMap<>();
		this.compressionCodec = conf.getOrDefault(StorageEngine.COMPRESSION_CODEC,
				StorageEngine.DEFAULT_COMPRESSION_CODEC);
		this.compactionCodec = conf.getOrDefault(StorageEngine.COMPACTION_CODEC,
				StorageEngine.DEFAULT_COMPACTION_CODEC);
		this.useQueryPool = Boolean.parseBoolean(conf.getOrDefault(USE_QUERY_POOL, "true"));
		malloc = new MemMalloc();
		malloc.configure(conf, dataDirectory, measurementName, engine, bgTaskPool, lock);
	}

	@Override
	public TagIndex getTagIndex() {
		return tagIndex;
	}

	@Override
	public void loadTimeseriesFromMeasurements() throws IOException {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException {
		Collections.sort(tags);
		String seriesId = constructSeriesId(tags, tagIndex);
		SeriesFieldMap seriesFieldMap = getSeriesFromKey(seriesId);
		if (seriesFieldMap == null) {
			lock.lock();
			if ((seriesFieldMap = getSeriesFromKey(seriesId)) == null) {
				Measurement.indexRowKey(tagIndex, seriesId, tags);
				seriesFieldMap = new SeriesFieldMap();
				seriesMap.put(seriesId, seriesFieldMap);
			}
			lock.unlock();
		}
		TimeSeries series = seriesFieldMap.get(valueFieldName);
		if (series == null) {
			lock.lock();
			if ((series = seriesFieldMap.get(valueFieldName)) == null) {
				String seriesId2 = seriesId + SERIESID_SEPARATOR + valueFieldName;
				series = new TimeSeries(this, compressionCodec, compactionCodec, seriesId2, timeBucketSize, metadata,
						fp, conf);
				seriesFieldMap.addSeries(valueFieldName, series);
				logger.fine("Created new timeseries:" + seriesFieldMap + " for measurement:" + measurementName + "\t"
						+ seriesId + "\t" + metadata.getRetentionHours() + "\t" + seriesMap.size());
			}
			lock.unlock();
		}

		return series;
	}

	@Override
	public String getMeasurementName() {
		return measurementName;
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MemoryMeasurement [measurementName=" + measurementName + ", metadata=" + metadata + ", seriesMap="
				+ seriesMap + ", tagIndex=" + tagIndex + ", compressionClass=" + compressionCodec + "]";
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
	public SeriesFieldMap getSeriesFromKey(String key) {
		return seriesMap.get(key);
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
		return seriesMap.values();
	}
}
