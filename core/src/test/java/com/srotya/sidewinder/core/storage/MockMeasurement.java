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
package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * @author ambud
 */
public class MockMeasurement implements Measurement {

	private int bufferRenewCounter = 0;
	private List<ByteBuffer> list;
	private int bufSize;

	public MockMeasurement(int bufSize) {
		this.bufSize = bufSize;
		list = new ArrayList<>();
	}

	@Override
	public Collection<TimeSeries> getTimeSeries() {
		return null;
	}

	@Override
	public Map<String, TimeSeries> getTimeSeriesMap() {
		return null;
	}

	@Override
	public TagIndex getTagIndex() {
		return null;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void garbageCollector() throws IOException {
	}

	@Override
	public ByteBuffer createNewBuffer(String seriesId) throws IOException {
		bufferRenewCounter++;
		ByteBuffer allocate = ByteBuffer.allocate(bufSize);
		list.add(allocate);
		return allocate;
	}

	public int getBufferRenewCounter() {
		return bufferRenewCounter;
	}

	public List<ByteBuffer> getBufTracker() {
		return list;
	}

	@Override
	public void loadTimeseriesFromMeasurements() throws IOException {
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize,
			boolean fp, Map<String, String> conf) throws IOException {
		return null;
	}

	@Override
	public void configure(Map<String, String> conf, String measurementName, String baseIndexDirectory,
			String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool) throws IOException {
	}

	@Override
	public String getMeasurementName() {
		return null;
	}

	@Override
	public Logger getLogger() {
		return null;
	}



}