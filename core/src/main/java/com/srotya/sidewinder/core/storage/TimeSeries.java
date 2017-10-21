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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.RollOverException;
import com.srotya.sidewinder.core.storage.compression.Writer;
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
	private SortedMap<String, List<Writer>> bucketMap;
	private boolean fp;
	private AtomicInteger retentionBuckets;
	private String seriesId;
	private String compressionFQCN;
	private Map<String, String> conf;
	private Measurement measurement;
	private int timeBucketSize;
	private int bucketCount;

	/**
	 * @param measurement
	 * @param compressionFQCN
	 * @param seriesId
	 * @param timeBucketSize
	 * @param metadata
	 * @param fp
	 * @param conf
	 * @throws IOException
	 */
	public TimeSeries(Measurement measurement, String compressionFQCN, String seriesId, int timeBucketSize,
			DBMetadata metadata, boolean fp, Map<String, String> conf) throws IOException {
		this.measurement = measurement;
		this.compressionFQCN = compressionFQCN;
		this.seriesId = seriesId;
		this.timeBucketSize = timeBucketSize;
		this.conf = new HashMap<>(conf);
		retentionBuckets = new AtomicInteger(0);
		setRetentionHours(metadata.getRetentionHours());
		this.fp = fp;
		bucketMap = measurement.createNewBucketMap(seriesId);
	}

	public Writer getOrCreateSeriesBucket(TimeUnit unit, long timestamp) throws IOException {
		String tsBucket = getTimeBucket(unit, timestamp, timeBucketSize);
		List<Writer> list = bucketMap.get(tsBucket);
		if (list == null) {
			synchronized (bucketMap) {
				if ((list = bucketMap.get(tsBucket)) == null) {
					list = new ArrayList<>();
					createNewTimeSeriesBucket(timestamp, tsBucket, list);
					bucketMap.put(tsBucket, list);
				}
			}
		}

		synchronized (list) {
			Writer ans = list.get(list.size() - 1);
			if (ans.isFull()) {
				if ((ans = list.get(list.size() - 1)).isFull()) {
					logger.fine("Requesting new time series:" + seriesId + ",measurement:"
							+ measurement.getMeasurementName() + "\t" + bucketCount);
					ans = createNewTimeSeriesBucket(timestamp, tsBucket, list);
				}
			}
			try {
				// int idx = list.indexOf(ans);
				// if (idx != (list.size() - 1)) {
				// System.out.println("\n\nThread safety error\t" + idx + "\t" + list.size() +
				// "\n\n");
				// }
				return ans;
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Create new:" + "\tList:" + list + "\tbucket:" + tsBucket + "\t" + bucketMap,
						e);
				throw e;
			}
		}
	}

	public static String getTimeBucket(TimeUnit unit, long timestamp, int timeBucketSize) {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, timeBucketSize);
		String tsBucket = Integer.toHexString(bucket);
		return tsBucket;
	}

	private Writer createNewTimeSeriesBucket(long timestamp, String tsBucket, List<Writer> list) throws IOException {
		Entry<String, ByteBuffer> bufPair = measurement.createNewBuffer(seriesId, tsBucket);
		// writeStringToBuffer(seriesId, buf);
		Writer writer;
		writer = getWriterInstance();
		writer.configure(conf, bufPair.getValue(), true);
		writer.setHeaderTimestamp(timestamp);
		list.add(writer);
		bucketCount++;
		return writer;
	}

	private Writer getWriterInstance() {
		try {
			Writer writer = (Writer) Class.forName(compressionFQCN).newInstance();
			return writer;
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Function to check and recover existing bucket map, if one exists.
	 * 
	 * @param bufferEntries
	 * @throws IOException
	 */
	public void loadBucketMap(List<Entry<String, ByteBuffer>> bufferEntries) throws IOException {
		Map<String, String> cacheConf = new HashMap<>(conf);
		logger.fine("Scanning buffer for:" + seriesId);
		for (Entry<String, ByteBuffer> entry : bufferEntries) {
			ByteBuffer duplicate = entry.getValue();
			duplicate.rewind();
			// String series = getStringFromBuffer(duplicate);
			// if (!series.equalsIgnoreCase(seriesId)) {
			// continue;
			// }
			String tsBucket = entry.getKey();
			List<Writer> list = bucketMap.get(tsBucket);
			if (list == null) {
				list = new ArrayList<>();
				bucketMap.put(tsBucket, list);
			}
			// long bucketTimestamp = duplicate.getLong();
			ByteBuffer slice = duplicate.slice();
			Writer writer = getWriterInstance();
			writer.configure(cacheConf, slice, false);
			list.add(writer);
			logger.fine("Loading bucketmap:" + seriesId + "\t" + tsBucket);
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
	 * Extract {@link DataPoint}s for the supplied time range and value predicate.
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
		if (startTime == 0) {
			startTsBucket = bucketMap.firstKey();
		}
		int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, timeBucketSize);
		String endTsBucket = Integer.toHexString(tsEndBucket);
		if (endTime == Long.MAX_VALUE) {
			endTsBucket = bucketMap.lastKey();
		}
		SortedMap<String, List<Writer>> series = bucketMap.subMap(startTsBucket, endTsBucket + Character.MAX_VALUE);
		for (List<Writer> writers : series.values()) {
			for (Writer writer : writers) {
				readers.add(
						getReader(writer, timeRangePredicate, valuePredicate, fp, appendFieldValueName, appendTags));
			}
		}
		List<DataPoint> points = new ArrayList<>();
		for (Reader reader : readers) {
			readerToDataPoints(points, reader);
		}
		return points;
	}

	/**
	 * Get {@link Reader} with time and value filter predicates pushed-down to it.
	 * Along with {@link DataPoint} enrichments pushed to it.
	 * 
	 * @param timePredicate
	 * @param valuePredicate
	 * @param isFp
	 * @param appendFieldValueName
	 * @param appendTags
	 * @return point in time instance of reader
	 * @throws IOException
	 */
	public static Reader getReader(Writer writer, Predicate timePredicate, Predicate valuePredicate, boolean isFp,
			String appendFieldValueName, List<String> appendTags) throws IOException {
		Reader reader = writer.getReader();
		reader.setTimePredicate(timePredicate);
		reader.setValuePredicate(valuePredicate);
		reader.setFieldName(appendFieldValueName);
		reader.setIsFP(isFp);
		reader.setTags(appendTags);
		return reader;
	}

	/**
	 * Get {@link Reader} with time and value filter predicates pushed-down to it.
	 * 
	 * @param timePredicate
	 * @param valuePredicate
	 * @return point in time instance of reader
	 * @throws IOException
	 */
	public static Reader getReader(Writer writer, Predicate timePredicate, Predicate valuePredicate)
			throws IOException {
		Reader reader = writer.getReader();
		reader.setTimePredicate(timePredicate);
		reader.setValuePredicate(valuePredicate);
		return reader;
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
		SortedMap<String, List<Writer>> series = bucketMap.subMap(startTsBucket, endTsBucket + Character.MAX_VALUE);
		for (List<Writer> writers : series.values()) {
			for (Writer writer : writers) {
				readers.add(
						getReader(writer, timeRangePredicate, valuePredicate, fp, appendFieldValueName, appendTags));
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
		Writer writer = getOrCreateSeriesBucket(unit, timestamp);
		try {
			writer.addValue(timestamp, value);
		} catch (RollOverException e) {
			addDataPoint(unit, timestamp, value);
		} catch (NullPointerException e) {
			logger.log(Level.SEVERE, "\n\nNPE occurred for add datapoint operation\n\n", e);
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
		Writer timeseriesBucket = getOrCreateSeriesBucket(unit, timestamp);
		try {
			timeseriesBucket.addValue(timestamp, value);
		} catch (RollOverException e) {
			addDataPoint(unit, timestamp, value);
		} catch (NullPointerException e) {
			logger.log(Level.SEVERE, "\n\nNPE occurred for add datapoint operation\n\n", e);
		}
	}

	public void addDataPoints(TimeUnit unit, List<DataPoint> dps) throws IOException {
		Map<Writer, List<DataPoint>> dpMap = new HashMap<>();
		for (DataPoint dp : dps) {
			Writer writer = getOrCreateSeriesBucket(unit, dp.getTimestamp());
			List<DataPoint> dpx;
			if (!dpMap.containsKey(writer)) {
				dpMap.put(writer, dpx = new ArrayList<>());
			} else {
				dpx = dpMap.get(writer);
			}
			dpx.add(dp);
		}
		for (Entry<Writer, List<DataPoint>> entry : dpMap.entrySet()) {
			entry.getKey().write(entry.getValue());
		}
	}

	/**
	 * Converts timeseries to a list of datapoints appended to the supplied list
	 * object. Datapoints are filtered by the supplied predicates before they are
	 * returned. These predicates are pushed down to the reader for efficiency and
	 * performance as it prevents unnecessary object creation.
	 * 
	 * @param appendFieldValueName
	 * @param appendTags
	 * 
	 * @param points
	 *            list data points are appended to
	 * @param writer
	 *            to extract the data points from
	 * @param timePredicate
	 *            time range filter
	 * @param valuePredicate
	 *            value filter
	 * @return the points argument
	 * @throws IOException
	 */
	public static List<DataPoint> seriesToDataPoints(String appendFieldValueName, List<String> appendTags,
			List<DataPoint> points, Writer writer, Predicate timePredicate, Predicate valuePredicate, boolean isFp)
			throws IOException {
		Reader reader = getReader(writer, timePredicate, valuePredicate, isFp, appendFieldValueName, appendTags);
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
	public List<Writer> collectGarbage() throws IOException {
		List<Writer> gcedBuckets = new ArrayList<>();
		while (bucketMap.size() > retentionBuckets.get()) {
			int oldSize = bucketMap.size();
			String key = bucketMap.firstKey();
			List<Writer> buckets = bucketMap.remove(key);
			for (Writer bucket : buckets) {
				// TODO close
				// bucket.close();
				gcedBuckets.add(bucket);
				logger.log(Level.INFO,
						"GC," + measurement.getMeasurementName() + ":" + seriesId + " removing bucket:" + key
								+ ": as it passed retention period of:" + retentionBuckets.get() + ":old size:"
								+ oldSize + ":newsize:" + bucketMap.size() + ":");
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
	 * @return number of buckets to retain for this {@link TimeSeries}
	 */
	public int getRetentionBuckets() {
		return retentionBuckets.get();
	}

	/**
	 * @return the bucketMap
	 */
	public SortedMap<String, Writer> getBucketMap() {
		SortedMap<String, Writer> map = new TreeMap<>();
		for (Entry<String, List<Writer>> entry : bucketMap.entrySet()) {
			List<Writer> value = entry.getValue();
			for (int i = 0; i < value.size(); i++) {
				Writer bucketEntry = value.get(i);
				map.put(entry.getKey() + i, bucketEntry);
			}
		}
		return map;
	}

	public SortedMap<String, List<Writer>> getBucketRawMap() {
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
		return "PersistentTimeSeries [bucketMap=" + bucketMap + ", fp=" + fp + ", retentionBuckets=" + retentionBuckets
				+ ", logger=" + logger + ", seriesId=" + seriesId + ", timeBucketSize=" + timeBucketSize + "]";
	}

	public void close() throws IOException {
		// TODO close series
	}

	public int getTimeBucketSize() {
		return timeBucketSize;
	}

	/**
	 * FOR UNIT TESTING ONLY
	 * 
	 * @return
	 */
	public int getBucketCount() {
		return bucketCount;
	}

}
