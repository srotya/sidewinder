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
package com.srotya.sidewinder.core.storage.gorilla;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.RejectException;
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
	private Logger logger;
	private String seriesId;
	private int timeBucketSize;

	public TimeSeries(String seriesId, int retentionHours, int timeBucketSize, boolean fp) {
		this.seriesId = seriesId;
		this.timeBucketSize = timeBucketSize;
		logger = Logger.getLogger(seriesId);
		logger.fine("Created timeseries with bucket" + timeBucketSize);
		retentionBuckets = new AtomicInteger(0);
		setRetentionHours(retentionHours);
		this.fp = fp;
		bucketMap = new ConcurrentSkipListMap<>();
	}

	public List<DataPoint> queryDataPoints(String appendFieldValueName, List<String> appendTags, long startTime,
			long endTime, Predicate valuePredicate) {
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
		if (series == null || series.isEmpty()) {
			TimeSeriesBucket timeSeries = bucketMap.get(startTsBucket);
			if (timeSeries != null) {
				seriesToDataPoints(appendFieldValueName, appendTags, points, timeSeries, timeRangePredicate,
						valuePredicate, fp);
			}
		} else {
			for (TimeSeriesBucket timeSeries : series.values()) {
				seriesToDataPoints(appendFieldValueName, appendTags, points, timeSeries, timeRangePredicate,
						valuePredicate, fp);
			}
		}
		return points;
	}

	public List<Reader> queryReader(String appendFieldValueName, List<String> appendTags, long startTime, long endTime,
			Predicate valuePredicate) {
		List<Reader> points = new ArrayList<>();
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		int tsStartBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, timeBucketSize) - timeBucketSize;
		String startTsBucket = Integer.toHexString(tsStartBucket);
		int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, timeBucketSize);
		String endTsBucket = Integer.toHexString(tsEndBucket);
		SortedMap<String, TimeSeriesBucket> series = bucketMap.subMap(startTsBucket, endTsBucket + Character.MAX_VALUE);
		if (series == null || series.isEmpty()) {
			TimeSeriesBucket timeSeries = bucketMap.get(startTsBucket);
			if (timeSeries != null) {
				points.add(
						timeSeries.getReader(timeRangePredicate, valuePredicate, fp, appendFieldValueName, appendTags));
			}
		} else {
			for (TimeSeriesBucket timeSeries : series.values()) {
				points.add(
						timeSeries.getReader(timeRangePredicate, valuePredicate, fp, appendFieldValueName, appendTags));
			}
		}
		return points;
	}

	public void addDataPoint(TimeUnit unit, long timestamp, long value) throws RejectException {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, timeBucketSize);
		String tsBucket = Integer.toHexString(bucket);
		TimeSeriesBucket timeseriesBucket = bucketMap.get(tsBucket);
		if (timeseriesBucket == null) {
			timeseriesBucket = new TimeSeriesBucket(timeBucketSize, timestamp);
			bucketMap.put(tsBucket, timeseriesBucket);
		}
		timeseriesBucket.addDataPoint(timestamp, value);
	}

	public void addDataPoint(TimeUnit unit, long timestamp, double value) throws RejectException {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, timeBucketSize);
		String tsBucket = Integer.toHexString(bucket);
		TimeSeriesBucket timeseriesBucket = bucketMap.get(tsBucket);
		if (timeseriesBucket == null) {
			timeseriesBucket = new TimeSeriesBucket(timeBucketSize, timestamp);
			bucketMap.put(tsBucket, timeseriesBucket);
		}
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
	 */
	public static List<DataPoint> seriesToDataPoints(String appendFieldValueName, List<String> appendTags,
			List<DataPoint> points, TimeSeriesBucket timeSeries, Predicate timePredicate, Predicate valuePredicate,
			boolean isFp) {
		Reader reader = timeSeries.getReader(timePredicate, valuePredicate, isFp, appendFieldValueName, appendTags);
		DataPoint point = null;
		while (true) {
			try {
				point = reader.readPair();
				if (point != null) {
					points.add(point);
				}
			} catch (IOException e) {
				break;
			}
		}
		return points;
	}

	/**
	 * Cleans stale series
	 */
	public void collectGarbage() {
		while (bucketMap.size() > retentionBuckets.get()) {
			int oldSize = bucketMap.size();
			String key = bucketMap.firstKey();
			bucketMap.remove(key);
			logger.info("GC, removing bucket:" + key + ": as it passed retention period of:" + retentionBuckets.get()
					+ ":old size:" + oldSize + ":newsize:" + bucketMap.size() + ":");
		}
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

}
