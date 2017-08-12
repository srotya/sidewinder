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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.RollOverException;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * Persistent version of {@link TimeSeries}. Persistence is provided via keeping
 * series buckets in a text file whenever a new series is added. Series buckets
 * are added not that frequently therefore using a text format is fairly
 * reasonable. This system is prone to instant million file appends at the mark
 * of series bucket time boundary i.e. all series will have bucket changes at
 * the same time however depending on the frequency of writes, this may not be
 * an issue.
 * 
 * @author ambud
 */
public class TimeSeries {

	private static final Logger logger = Logger.getLogger(TimeSeries.class.getName());
	private SortedMap<String, List<TimeSeriesBucket>> bucketMap;
	private boolean fp;
	private AtomicInteger retentionBuckets;
	private String seriesId;
	private String compressionFQCN;
	private Map<String, String> conf;
	private Measurement measurement;
	private int timeBucketSize;

	/**
	 * @param seriesId
	 *            used for logger name
	 * @param metadata
	 *            duration of data that will be stored in this time series
	 * @param fp
	 * @param bgTaskPool
	 * @throws IOException
	 */
	public TimeSeries(Measurement measurement, String compressionFQCN, String seriesId, int timeBucketSize,
			DBMetadata metadata, boolean fp, Map<String, String> conf, ScheduledExecutorService bgTaskPool)
			throws IOException {
		this.measurement = measurement;
		this.compressionFQCN = compressionFQCN;
		this.seriesId = seriesId;
		this.timeBucketSize = timeBucketSize;
		this.conf = new HashMap<>(conf);
		retentionBuckets = new AtomicInteger(0);
		setRetentionHours(metadata.getRetentionHours());
		this.fp = fp;
		bucketMap = new ConcurrentSkipListMap<>();
	}

	public TimeSeriesBucket getOrCreateSeriesBucket(TimeUnit unit, long timestamp, boolean createNew)
			throws IOException {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, timeBucketSize);
		String tsBucket = Integer.toHexString(bucket);
		List<TimeSeriesBucket> list = bucketMap.get(tsBucket);
		if (list == null) {
			synchronized (bucketMap) {
				if (list == null) {
					list = new ArrayList<>();
					createNew = true;
					bucketMap.put(tsBucket, list);
				}
			}
		}
		if (createNew) {
			synchronized (bucketMap) {
				ByteBuffer buf = measurement.createNewBuffer(seriesId);
//				writeStringToBuffer(seriesId, buf);
				writeStringToBuffer(tsBucket, buf);
				buf.putLong(timestamp);
				buf = buf.slice();
				TimeSeriesBucket bucketEntry = new TimeSeriesBucket(compressionFQCN, timestamp, conf, buf, true);
				list.add(bucketEntry);
			}
		}
		return list.get(list.size() - 1);
	}
	
	/**
	 * Function to check and recover existing bucket map, if one exists.
	 * 
	 * @param list2
	 * @throws IOException
	 */
	public void loadBucketMap(List<ByteBuffer> bufList) throws IOException {
		logger.info("Scanning buffer for:" + seriesId);
		for (ByteBuffer entry : bufList) {
			ByteBuffer duplicate = entry.duplicate();
			duplicate.rewind();
//			String series = getStringFromBuffer(duplicate);
//			if (!series.equalsIgnoreCase(seriesId)) {
//				continue;
//			}
			String tsBucket = getStringFromBuffer(duplicate);
			List<TimeSeriesBucket> list = bucketMap.get(tsBucket);
			if (list == null) {
				list = new ArrayList<>();
				bucketMap.put(tsBucket, list);
			}
			long bucketTimestamp = duplicate.getLong();
			ByteBuffer slice = duplicate.slice();
			list.add(new TimeSeriesBucket(compressionFQCN, bucketTimestamp, conf, slice, false));
			logger.info("Loading bucketmap:" + seriesId + "\t" + tsBucket);
		}
	}

	public static String getStringFromBuffer(ByteBuffer buf) {
		short length = buf.getShort();
		byte[] dst = new byte[length];
		buf.get(dst);
		return new String(dst);
	}

	public static void writeStringToBuffer(String str, ByteBuffer buf) {
		buf.putShort((short) str.length());
		buf.put(str.getBytes());
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
		List<Reader> readers = new ArrayList<>();
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		int tsStartBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, timeBucketSize) - timeBucketSize;
		String startTsBucket = Integer.toHexString(tsStartBucket);
		int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, timeBucketSize);
		String endTsBucket = Integer.toHexString(tsEndBucket);
		SortedMap<String, List<TimeSeriesBucket>> series = bucketMap.subMap(startTsBucket,
				endTsBucket + Character.MAX_VALUE);
		for (List<TimeSeriesBucket> timeSeries : series.values()) {
			for (TimeSeriesBucket bucketEntry : timeSeries) {
				readers.add(bucketEntry.getReader(timeRangePredicate, valuePredicate, fp, appendFieldValueName,
						appendTags));
			}
		}
		List<DataPoint> points = new ArrayList<>();
		for (Reader reader : readers) {
			readerToDataPoints(points, reader);
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
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		List<Reader> readers = new ArrayList<>();
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		int tsStartBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, timeBucketSize) - timeBucketSize;
		String startTsBucket = Integer.toHexString(tsStartBucket);
		int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, timeBucketSize);
		String endTsBucket = Integer.toHexString(tsEndBucket);
		SortedMap<String, List<TimeSeriesBucket>> series = bucketMap.subMap(startTsBucket,
				endTsBucket + Character.MAX_VALUE);
		for (List<TimeSeriesBucket> timeSeries : series.values()) {
			for (TimeSeriesBucket bucketEntry : timeSeries) {
				readers.add(bucketEntry.getReader(timeRangePredicate, valuePredicate, fp, appendFieldValueName,
						appendTags));
			}
		}
		return readers;
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
	 * @throws IOException
	 */
	public void addDataPoint(TimeUnit unit, long timestamp, double value) throws IOException {
		TimeSeriesBucket timeseriesBucket = getOrCreateSeriesBucket(unit, timestamp, false);
		try {
			timeseriesBucket.addDataPoint(timestamp, value);
		} catch (RollOverException e) {
			timeseriesBucket = getOrCreateSeriesBucket(unit, timestamp, true);
			timeseriesBucket.addDataPoint(timestamp, value);
		}
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
	 * @throws IOException
	 */
	public void addDataPoint(TimeUnit unit, long timestamp, long value) throws IOException {
		TimeSeriesBucket timeseriesBucket = getOrCreateSeriesBucket(unit, timestamp, false);
		try {
			timeseriesBucket.addDataPoint(timestamp, value);
		} catch (RollOverException e) {
			timeseriesBucket = getOrCreateSeriesBucket(unit, timestamp, true);
			timeseriesBucket.addDataPoint(timestamp, value);
		}
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

	public static List<DataPoint> readerToDataPoints(List<DataPoint> points, Reader reader) throws IOException {
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
	 * 
	 * @throws IOException
	 */
	public List<TimeSeriesBucket> collectGarbage() throws IOException {
		List<TimeSeriesBucket> gcedBuckets = new ArrayList<>();
		while (bucketMap.size() > retentionBuckets.get()) {
			int oldSize = bucketMap.size();
			String key = bucketMap.firstKey();
			List<TimeSeriesBucket> buckets = bucketMap.remove(key);
			for (TimeSeriesBucket bucket : buckets) {
				bucket.close();
				gcedBuckets.add(bucket);
				logger.log(Level.INFO, "GC, removing bucket:" + key + ": as it passed retention period of:"
						+ retentionBuckets.get() + ":old size:" + oldSize + ":newsize:" + bucketMap.size() + ":");
			}
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
		SortedMap<String, TimeSeriesBucket> map = new TreeMap<>();
		for (Entry<String, List<TimeSeriesBucket>> entry : bucketMap.entrySet()) {
			List<TimeSeriesBucket> value = entry.getValue();
			for (int i = 0; i < value.size(); i++) {
				TimeSeriesBucket bucketEntry = value.get(i);
				map.put(entry.getKey() + i, bucketEntry);
			}
		}
		return map;
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
		return "PersistentTimeSeries [bucketMap=" + bucketMap + ", fp=" + fp + ", retentionBuckets=" + retentionBuckets
				+ ", logger=" + logger + ", seriesId=" + seriesId + ", timeBucketSize=" + timeBucketSize + "]";
	}

	public void close() throws IOException {
		// TODO close series
	}

	public int getTimeBucketSize() {
		return timeBucketSize;
	}

}
