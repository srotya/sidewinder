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
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import com.srotya.sidewinder.core.storage.BufferObject;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Measurement;
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
	private Map<String, TimeSeries> seriesMap;
	private MemTagIndex tagIndex;
	private List<ByteBuffer> bufTracker;
	private String compressionCodec;
	private String compactionCodec;
	private boolean useQueryPool;

	@Override
	public void configure(Map<String, String> conf, StorageEngine engine, String measurementName,
			String baseIndexDirectory, String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool)
			throws IOException {
		this.measurementName = measurementName;
		this.metadata = metadata;
		this.tagIndex = new MemTagIndex(MetricsRegistryService.getInstance(engine, bgTaskPool).getInstance("request"));
		this.seriesMap = new ConcurrentHashMap<>();
		this.bufTracker = new ArrayList<>();
		this.compressionCodec = conf.getOrDefault(StorageEngine.COMPRESSION_CODEC,
				StorageEngine.DEFAULT_COMPRESSION_CODEC);
		this.compactionCodec = conf.getOrDefault(StorageEngine.COMPACTION_CODEC,
				StorageEngine.DEFAULT_COMPACTION_CODEC);
		this.useQueryPool = Boolean.parseBoolean(conf.getOrDefault(USE_QUERY_POOL, "true"));
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
	public void loadTimeseriesFromMeasurements() throws IOException {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public BufferObject createNewBuffer(String seriesId, String tsBucket) throws IOException {
		return createNewBuffer(seriesId, tsBucket, 1024);
	}

	@Override
	public BufferObject createNewBuffer(String seriesId, String tsBucket, int newSize) throws IOException {
		ByteBuffer allocateDirect = ByteBuffer.allocateDirect(newSize);
		synchronized (bufTracker) {
			bufTracker.add(allocateDirect);
		}
		return new BufferObject(seriesId + "\t" + tsBucket, allocateDirect);
	}

	public List<ByteBuffer> getBufTracker() {
		return bufTracker;
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException {
		Collections.sort(tags);
		String rowKey = constructSeriesId(valueFieldName, tags, tagIndex);
		TimeSeries timeSeries = getTimeSeries(rowKey);
		if (timeSeries == null) {
			synchronized (this) {
				if ((timeSeries = getTimeSeries(rowKey)) == null) {
					Measurement.indexRowKey(tagIndex, rowKey, tags);
					timeSeries = new TimeSeries(this, compressionCodec, compactionCodec, rowKey, timeBucketSize,
							metadata, fp, conf);
					seriesMap.put(rowKey, timeSeries);
					logger.fine("Created new timeseries:" + timeSeries + " for measurement:" + measurementName + "\t"
							+ rowKey + "\t" + metadata);
				}
			}
		}
		return timeSeries;
	}

	private TimeSeries getTimeSeries(String rowKey) {
		return getTimeSeriesMap().get(rowKey);
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
				+ seriesMap + ", tagIndex=" + tagIndex + ", bufTracker=" + bufTracker + ", compressionClass="
				+ compressionCodec + "]";
	}

	@Override
	public SortedMap<String, List<Writer>> createNewBucketMap(String seriesId) {
		return new ConcurrentSkipListMap<>();
	}

	@Override
	public void cleanupBufferIds(Set<String> cleanupList) {
		// nothing much to do since this is an in-memory implementation
	}

	@Override
	public ReentrantLock getLock() {
		return lock;
	}

	@Override
	public boolean useQueryPool() {
		return useQueryPool;
	}
}
