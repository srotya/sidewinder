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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.malloc.Malloc;
import com.srotya.sidewinder.core.storage.malloc.MemMalloc;

/**
 * @author ambud
 */
public class MockMeasurement implements Measurement {

	private ReentrantLock lock = new ReentrantLock();
	private Malloc memMalloc;

	public MockMeasurement(int bufSize) {
		memMalloc = new MemMalloc();
		Map<String, String> conf = new HashMap<>();
		conf.put("buffer.size", String.valueOf(bufSize));
		memMalloc.configure(conf, null, null, null, null);
	}

	@Override
	public Collection<TimeSeries> getTimeSeries() {
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
	public void collectGarbage(Archiver archiver) throws IOException {
	}

	@Override
	public void loadTimeseriesFromMeasurements() throws IOException {
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException {
		return null;
	}

	@Override
	public void configure(Map<String, String> conf, StorageEngine engine, String dbName, String measurementName,
			String baseIndexDirectory, String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool)
			throws IOException {
	}

	@Override
	public String getMeasurementName() {
		return null;
	}

	@Override
	public Logger getLogger() {
		return null;
	}

	@Override
	public SortedMap<String, List<Writer>> createNewBucketMap(String seriesId) {
		return new ConcurrentSkipListMap<>();
	}

	public void cleanupBufferIds(Set<String> cleanupList) {
	}

	@Override
	public ReentrantLock getLock() {
		return lock;
	}

	@Override
	public boolean useQueryPool() {
		return false;
	}

	@Override
	public Set<String> getSeriesKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TimeSeries getSeriesFromKey(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDbName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Malloc getMalloc() {
		return memMalloc;
	}

}