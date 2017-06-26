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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.TimeSeriesBucket;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * A timeseries is defined as a subset of a measurement for a specific set of
 * tags. Measurement is defined as a category and is an abstract to group
 * metrics about a given topic under a the same label. E.g of a measurement is
 * CPU, Memory whereas a {@link TimeSeries} would be cpu measurement on a
 * specific host.<br>
 * <br>
 * Internally a {@link TimeSeries} contains a {@link SortedMap} of buckets that
 * bundle datapoints under temporally sorted partitions that makes storage,
 * retrieval and evictions efficient. This class provides the abstractions
 * around that, therefore partitioning / bucketing interval can be controlled on
 * a per {@link TimeSeries} basis rather than keep it a constant.<br>
 * <br>
 * 
 * @author ambud
 */
public class TimeSeries {

	private SortedMap<String, TimeSeriesBucket> bucketMap;
	private boolean fp;
	private AtomicInteger retentionBuckets;
	private static final Logger logger = Logger.getLogger(TimeSeries.class.getName());
	private String seriesId;
	private int timeBucketSize;
	private String compressionFQCN;
	private Map<String, String> conf;

	protected TimeSeries() {
	}

	/**
	 * @param seriesId
	 *            used for logger name
	 * @param retentionHours
	 *            duration of data that will be stored in this time series
	 * @param timeBucketSize
	 *            size of each time bucket (partition)
	 * @param fp
	 */
	public TimeSeries(String compressionFQCN, String seriesId, int retentionHours, int timeBucketSize, boolean fp,
			Map<String, String> conf) {
		this.compressionFQCN = compressionFQCN;
		this.seriesId = seriesId;
		this.timeBucketSize = timeBucketSize;
		this.conf = conf;
		retentionBuckets = new AtomicInteger(0);
		setRetentionHours(retentionHours);
		this.fp = fp;
		bucketMap = new ConcurrentSkipListMap<>();
	}

	/**
	 * Extract {@link DataPoint}s for the supplied time range and value
	 * predicate.
	 * 
	 * Each {@link DataPoint} has the appendFieldValue and appendTags set in it.
	 * 
	 * @param appendFieldValueName
	 *            fieldname to append to each datapoint
	 * @param appendTags
	 *            tags to append to each datapoint
	 * @param startTime
	 *            time range beginning
	 * @param endTime
	 *            time range end
	 * @param valuePredicate
	 *            pushed down filter for values
	 * @return list of datapoints
	 * @throws IOException 
	 */
	public List<DataPoint> queryDataPoints(String appendFieldValueName, List<String> appendTags, long startTime,
			long endTime, Predicate valuePredicate) throws IOException {
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		List<DataPoint> points = new ArrayList<>();
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		int tsStartBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, timeBucketSize) - timeBucketSize;
		String startTsBucket = Integer.toHexString(tsStartBucket);
		int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, timeBucketSize);
		String endTsBucket = Integer.toHexString(tsEndBucket);
		SortedMap<String, TimeSeriesBucket> series = bucketMap.subMap(startTsBucket, endTsBucket + Character.MAX_VALUE);
		for (TimeSeriesBucket timeSeries : series.values()) {
			seriesToDataPoints(appendFieldValueName, appendTags, points, timeSeries, timeRangePredicate, valuePredicate,
					fp);
		}
		return points;
	}

	/**
	 * Extract list of readers for the supplied time range and value predicate.
	 * 
	 * Each {@link DataPoint} has the appendFieldValue and appendTags set in it.
	 * 
	 * @param appendFieldValueName
	 *            fieldname to append to each datapoint
	 * @param appendTags
	 *            tags to append to each datapoint
	 * @param startTime
	 *            time range beginning
	 * @param endTime
	 *            time range end
	 * @param valuePredicate
	 *            pushed down filter for values
	 * @return list of readers
	 * @throws IOException 
	 */
	public List<Reader> queryReader(String appendFieldValueName, List<String> appendTags, long startTime, long endTime,
			Predicate valuePredicate) throws IOException {
		List<Reader> readers = new ArrayList<>();
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		int tsStartBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, timeBucketSize) - timeBucketSize;
		String startTsBucket = Integer.toHexString(tsStartBucket);
		int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, timeBucketSize);
		String endTsBucket = Integer.toHexString(tsEndBucket);
		SortedMap<String, TimeSeriesBucket> series = bucketMap.subMap(startTsBucket, endTsBucket + Character.MAX_VALUE);
		for (TimeSeriesBucket timeSeries : series.values()) {
			readers.add(timeSeries.getReader(timeRangePredicate, valuePredicate, fp, appendFieldValueName, appendTags));
		}
		return readers;
	}

	/**
	 * Add data point with non floating point value
	 * 
	 * @param unit
	 *            of time for the supplied timestamp
	 * @param timestamp
	 *            of this data point
	 * @param value
	 *            of this data point
	 * @throws RejectException
	 */
	public void addDataPoint(TimeUnit unit, long timestamp, long value) throws IOException {
		TimeSeriesBucket timeseriesBucket = getOrCreateSeriesBucket(unit, timestamp);
		timeseriesBucket.addDataPoint(timestamp, value);
	}

	/**
	 * @param unit
	 * @param timestamp
	 * @return
	 * @throws IOException
	 */
	public TimeSeriesBucket getOrCreateSeriesBucket(TimeUnit unit, long timestamp) throws IOException {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, timeBucketSize);
		String tsBucket = Integer.toHexString(bucket);
		TimeSeriesBucket timeseriesBucket = bucketMap.get(tsBucket);
		if (timeseriesBucket == null) {
			synchronized (bucketMap) {
				if ((timeseriesBucket = bucketMap.get(tsBucket)) == null) {
					timeseriesBucket = new TimeSeriesBucket(seriesId + tsBucket, compressionFQCN, timestamp, false,
							conf);
					bucketMap.put(tsBucket, timeseriesBucket);
				}
			}
		}
		return timeseriesBucket;
	}

	/**
	 * @param unit
	 * @param timestamp
	 * @return
	 * @throws IOException
	 */
	public TimeSeriesBucket getSeriesBucket(TimeUnit unit, long timestamp) throws IOException {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, timeBucketSize);
		String tsBucket = Integer.toHexString(bucket);
		return bucketMap.get(tsBucket);
	}

	public SortedMap<String, TimeSeriesBucket> getSeriesBuckets(TimeUnit unit, long timestamp) throws IOException {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, timeBucketSize);
		String tsBucket = Integer.toHexString(bucket);
		return bucketMap.tailMap(tsBucket);
	}

	/**
	 * Add data point with floating point value
	 * 
	 * @param unit
	 *            of time for the supplied timestamp
	 * @param timestamp
	 *            of this data point
	 * @param value
	 *            of this data point
	 * @throws RejectException
	 */
	public void addDataPoint(TimeUnit unit, long timestamp, double value) throws IOException {
		TimeSeriesBucket timeseriesBucket = getOrCreateSeriesBucket(unit, timestamp);
		timeseriesBucket.addDataPoint(timestamp, value);
	}

	/**
	 * Converts timeseries to a list of datapoints appended to the supplied list
	 * object. Datapoints are filtered by the supplied predicates before they
	 * are returned. These predicates are pushed down to the reader for
	 * efficiency and performance as it prevents unnecessary object creation.
	 * 
	 * @param appendFieldValueName
	 * @param appendTags
	 * 
	 * @param points
	 *            list data points are appended to
	 * @param timeSeries
	 *            to extract the data points from
	 * @param timePredicate
	 *            time range filter
	 * @param valuePredicate
	 *            value filter
	 * @return the points argument
	 * @throws IOException 
	 */
	public static List<DataPoint> seriesToDataPoints(String appendFieldValueName, List<String> appendTags,
			List<DataPoint> points, TimeSeriesBucket timeSeries, Predicate timePredicate, Predicate valuePredicate,
			boolean isFp) throws IOException {
		Reader reader = timeSeries.getReader(timePredicate, valuePredicate, isFp, appendFieldValueName, appendTags);
		DataPoint point = null;
		while (true) {
			try {
				point = reader.readPair();
				if (point != null) {
					points.add(point);
				}
			} catch (IOException e) {
				if (e instanceof RejectException) {
				} else {
					e.printStackTrace();
				}
				break;
			}
		}
		if (reader.getCounter() != reader.getPairCount() || points.size() < reader.getCounter()) {
			// System.err.println("SDP:" + points.size() + "/" +
			// reader.getCounter() + "/" + reader.getPairCount());
		}
		return points;
	}

	/**
	 * Cleans stale series
	 * @throws IOException 
	 */
	public List<TimeSeriesBucket> collectGarbage() throws IOException {
		List<TimeSeriesBucket> gcedBuckets = new ArrayList<>();
		while (bucketMap.size() > retentionBuckets.get()) {
			int oldSize = bucketMap.size();
			String key = bucketMap.firstKey();
			TimeSeriesBucket bucket = bucketMap.remove(key);
			bucket.close();
			gcedBuckets.add(bucket);
			logger.log(Level.INFO, "GC, removing bucket:" + key + ": as it passed retention period of:"
					+ retentionBuckets.get() + ":old size:" + oldSize + ":newsize:" + bucketMap.size() + ":");
		}
		return gcedBuckets;
	}

	/**
	 * Update retention hours for this TimeSeries
	 * 
	 * @param retentionHours
	 */
	public void setRetentionHours(int retentionHours) {
		if (retentionHours < 1) {
			retentionHours = 2;
		}
		this.retentionBuckets.set((int) ((long) retentionHours * 3600) / timeBucketSize);
	}

	/**
	 * @return the timeBucketSize
	 */
	public int getTimeBucketSize() {
		return timeBucketSize;
	}

	/**
	 * @return number of {@link TimeSeriesBucket}s to retain for this
	 *         {@link TimeSeries}
	 */
	public int getRetentionBuckets() {
		return retentionBuckets.get();
	}

	/**
	 * @return the bucketMap
	 */
	public SortedMap<String, TimeSeriesBucket> getBucketMap() {
		return bucketMap;
	}

	/**
	 * @return the seriesId
	 */
	public String getSeriesId() {
		return seriesId;
	}

	/**
	 * @param seriesId
	 *            the seriesId to set
	 */
	public void setSeriesId(String seriesId) {
		this.seriesId = seriesId;
	}

	/**
	 * @return the fp
	 */
	public boolean isFp() {
		return fp;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TimeSeries [bucketMap=" + bucketMap + ", fp=" + fp + ", retentionBuckets=" + retentionBuckets
				+ ", logger=" + logger + ", seriesId=" + seriesId + ", timeBucketSize=" + timeBucketSize + "]";
	}

	public void close() throws IOException {
		if (bucketMap == null) {
			return;
		}
		for (Entry<String, TimeSeriesBucket> entry : bucketMap.entrySet()) {
			entry.getValue().close();
		}
	}

}
