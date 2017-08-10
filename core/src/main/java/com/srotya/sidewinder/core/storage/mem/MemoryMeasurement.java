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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.TimeSeries;

/**
 * @author ambud
 */
public class MemoryMeasurement implements Measurement {

	private static Logger logger = Logger.getLogger(MemoryMeasurement.class.getName());
	private String measurementName;
	private DBMetadata metadata;
	private Map<String, TimeSeries> seriesMap;
	private MemTagIndex tagIndex;
	private List<ByteBuffer> bufTracker;
	private String compressionClass;
	private ScheduledExecutorService bgTaskPool;

	@Override
	public void configure(Map<String, String> conf, String measurementName, String baseIndexDirectory,
			String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool) throws IOException {
		this.measurementName = measurementName;
		this.metadata = metadata;
		this.bgTaskPool = bgTaskPool;
		this.tagIndex = new MemTagIndex();
		this.seriesMap = new ConcurrentHashMap<>();
		this.bufTracker = new ArrayList<>();
		this.compressionClass = conf.getOrDefault(StorageEngine.COMPRESSION_CLASS,
				StorageEngine.DEFAULT_COMPRESSION_CLASS);
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
	public void delete() throws IOException {
	}

	@Override
	public ByteBuffer createNewBuffer() throws IOException {
		ByteBuffer allocateDirect = ByteBuffer.allocateDirect(1024);
		synchronized (bufTracker) {
			bufTracker.add(allocateDirect);
		}
		return allocateDirect;
	}

	@Override
	public List<ByteBuffer> getBufTracker() {
		return bufTracker;
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException {
		Collections.sort(tags);
		String rowKey = constructRowKey(valueFieldName, tags, tagIndex);
		TimeSeries timeSeries = getTimeSeries(rowKey);
		if (timeSeries == null) {
			synchronized (this) {
				if ((timeSeries = getTimeSeries(rowKey)) == null) {
					timeSeries = new TimeSeries(this, compressionClass, rowKey, timeBucketSize, metadata, fp, conf,
							bgTaskPool);
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

}
