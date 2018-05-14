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
package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.ByteString.ByteStringCache;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.mem.MemMalloc;

/**
 * @author ambud
 */
public class MockMeasurement implements Measurement {

	private ReentrantLock lock = new ReentrantLock();
	private MemMalloc memMalloc;
	private List<String> list;
	private int rentionBuckets;
	private int timebucket;
	private SortedMap<String, Boolean> typeMap;
	private ByteStringCache cache;

	public MockMeasurement(int bufSize, int rentionBuckets) {
		this.rentionBuckets = rentionBuckets;
		list = new ArrayList<>();
		memMalloc = new MemMalloc(list);
		Map<String, String> conf = new HashMap<>();
		conf.put("malloc.buf.increment", String.valueOf(bufSize));
		memMalloc.configure(conf, null, null, null, null, null);
		typeMap = new ConcurrentSkipListMap<>();
		cache = ByteStringCache.instance();
	}

	public MemMalloc getAllocator() {
		return memMalloc;
	}

	@Override
	public TagIndex getTagIndex() {
		return null;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void loadTimeseriesFromMeasurements() throws IOException {
	}

	@Override
	public void configure(Map<String, String> conf, StorageEngine engine, int defaultTimeBucketSize, String dbName,
			String measurementName, String baseIndexDirectory, String dataDirectory, DBMetadata metadata,
			ScheduledExecutorService bgTaskPool) throws IOException {
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
	public SortedMap<Integer, List<Writer>> createNewBucketMap(ByteString seriesId) {
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
	public Set<ByteString> getSeriesKeys() {
		return null;
	}

	@Override
	public String getDbName() {
		return null;
	}

	@Override
	public Malloc getMalloc() {
		return memMalloc;
	}

	@Override
	public Series getSeriesFromKey(ByteString key) {
		return null;
	}

	@Override
	public List<Series> getSeriesList() {
		return null;
	}

	@Override
	public Series getOrCreateSeries(List<Tag> tags, boolean preSorted) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getTimeBucketSize() {
		return timebucket;
	}
	
	public void setTimebucket(int timebucket) {
		this.timebucket = timebucket;
	}

	@Override
	public DBMetadata getMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ByteString, Integer> getSeriesMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isEnableMetricsCapture() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Counter getMetricsTimeSeriesCounter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AtomicInteger getRetentionBuckets() {
		return new AtomicInteger(rentionBuckets);
	}

	@Override
	public SortedMap<String, Boolean> getFieldTypeMap() {
		return typeMap;
	}

	@Override
	public ByteStringCache getFieldCache() {
		return cache;
	}

	@Override
	public Counter getMetricsCompactionCounter() {
		return null;
	}

	@Override
	public Counter getMetricsCleanupBufferCounter() {
		return null;
	}

}