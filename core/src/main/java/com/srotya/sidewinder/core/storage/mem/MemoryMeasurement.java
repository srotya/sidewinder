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
package com.srotya.sidewinder.core.storage.mem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.ByteString.ByteStringCache;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Malloc;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.compression.Writer;

/**
 * @author ambud
 */
public class MemoryMeasurement implements Measurement {

	private static Logger logger = Logger.getLogger(MemoryMeasurement.class.getName());
	private ReentrantLock lock = new ReentrantLock(false);
	private String measurementName;
	private DBMetadata metadata;
	private Map<ByteString, Integer> seriesMap;
	private List<Series> seriesList;
	private MemTagIndex tagIndex;
	private boolean useQueryPool;
	private String dbName;
	private Malloc malloc;
	private int timeBucketSize;
	private Map<String, String> conf;
	private boolean enableMetricsCapture;
	private Counter metricsTimeSeriesCounter;
	private AtomicInteger retentionBuckets;
	private SortedMap<String, Boolean> fieldTypeMap;
	private ByteStringCache fieldCache;
	private Counter metricsCleanupBufferCounter;
	private Counter metricsCompactionCounter;

	@Override
	public void configure(Map<String, String> conf, StorageEngine engine, int defaultTimeBucketSize, String dbName,
			String measurementName, String baseIndexDirectory, String dataDirectory, DBMetadata metadata,
			ScheduledExecutorService bgTaskPool) throws IOException {
		MetricsRegistryService reg = null;
		if (engine == null || bgTaskPool == null) {
			enableMetricsCapture = false;
		} else {
			reg = MetricsRegistryService.getInstance(engine, bgTaskPool);
			MetricRegistry metaops = reg.getInstance("metaops");
			metricsTimeSeriesCounter = metaops.counter("timeseries-counter");
			metricsCompactionCounter = metaops.counter("compaction-counter");
			metricsCleanupBufferCounter = metaops.counter("cleanbuf-counter");
			enableMetricsCapture = true;
		}
		this.conf = conf;
		this.timeBucketSize = defaultTimeBucketSize;
		this.dbName = dbName;
		this.measurementName = measurementName;
		this.fieldCache = ByteStringCache.instance();
		this.metadata = metadata;
		this.seriesList = new ArrayList<>(10_000);
		this.tagIndex = new MemTagIndex();
		tagIndex.configure(getConf(), null, this);
		this.seriesMap = new ConcurrentHashMap<>();
		this.fieldTypeMap = new ConcurrentSkipListMap<>();
		this.retentionBuckets = new AtomicInteger(0);
		setRetentionHours(metadata.getRetentionHours());
		this.useQueryPool = Boolean.parseBoolean(conf.getOrDefault(USE_QUERY_POOL, "true"));
		this.malloc = new MemMalloc();
		this.malloc.configure(conf, dataDirectory, measurementName, engine, bgTaskPool, lock);
	}

	@Override
	public ByteStringCache getFieldCache() {
		return fieldCache;
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
				+ seriesMap + ", tagIndex=" + tagIndex + "]";
	}

	@Override
	public SortedMap<Integer, List<Writer>> createNewBucketMap(ByteString seriesId) {
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
	public Set<ByteString> getSeriesKeys() {
		Set<ByteString> set = new HashSet<>();
		for (ByteString str : seriesMap.keySet()) {
			set.add(str);
		}
		return set;
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
	public List<Series> getSeriesList() {
		return seriesList;
	}

	@Override
	public int getTimeBucketSize() {
		return timeBucketSize;
	}

	@Override
	public DBMetadata getMetadata() {
		return metadata;
	}

	@Override
	public Map<String, String> getConf() {
		return conf;
	}

	@Override
	public Map<ByteString, Integer> getSeriesMap() {
		return seriesMap;
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
