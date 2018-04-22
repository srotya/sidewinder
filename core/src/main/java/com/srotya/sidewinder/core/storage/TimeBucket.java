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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.compression.CompressionFactory;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.RollOverException;
import com.srotya.sidewinder.core.storage.compression.TimeWriter;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * Persistent version of {@link TimeBucket}. Persistence is provided via keeping
 * series buckets in a text file whenever a new series is added. Series buckets
 * are added not that frequently therefore using a text format is fairly
 * reasonable. This system is prone to instant million file appends at the mark
 * of series bucket time boundary i.e. all series will have bucket changes at
 * the same time however depending on the frequency of writes, this may not be
 * an issue.
 * 
 * @author ambud
 */
public class TimeBucket {

	private static final int COMPACTION_THRESHOLD = 2;
	private static final int START_OFFSET = 2;
	private static final Logger logger = Logger.getLogger(TimeBucket.class.getName());
	private SortedMap<Integer, List<TimeWriter>> bucketMap;
	private ByteString fieldId;
	private Measurement measurement;
	private int timeBucketSize;
	private Map<Integer, List<TimeWriter>> compactionCandidateSet;
	public static boolean compactionEnabled;
	public static double compactionRatio;
	public static Class<TimeWriter> compressionClass;
	public static Class<TimeWriter> compactionClass;
	private Timer timerGetCreateSeriesBuckets;
	private Timer timerCreateTimeWriter;
	private Timer timerCompaction;

	/**
	 * @param measurement
	 * @param fieldId
	 * @param timeBucketSize
	 * @param fp
	 * @param conf
	 * @throws IOException
	 */
	public TimeBucket(Measurement measurement, ByteString fieldId, int timeBucketSize, Map<String, String> conf)
			throws IOException {
		this.measurement = measurement;
		this.fieldId = fieldId;
		this.timeBucketSize = timeBucketSize;
		this.bucketMap = new ConcurrentSkipListMap<>();
		this.compactionCandidateSet = new HashMap<>();
		logger.fine(() -> "Time bucket size:" + timeBucketSize + " " + measurement.getRetentionBuckets().get());
		checkAndEnableMethodProfiling();
	}

	private void checkAndEnableMethodProfiling() {
		if (StorageEngine.ENABLE_METHOD_METRICS && MetricsRegistryService.getInstance() != null) {
			logger.finest(() -> "Enabling method metrics for:" + fieldId);
			MetricRegistry methodMetrics = MetricsRegistryService.getInstance().getInstance("method-metrics");
			timerGetCreateSeriesBuckets = methodMetrics.timer("getCreateSeriesBucket");
			timerCreateTimeWriter = methodMetrics.timer("createNewTimeWriter");
			timerCompaction = methodMetrics.timer("compact");
		}
	}

	public TimeWriter getOrCreateSeriesBucket(int tsBucket, long timestamp) throws IOException {
		Context ctx = null;
		if (StorageEngine.ENABLE_METHOD_METRICS) {
			ctx = timerGetCreateSeriesBuckets.time();
		}
		List<TimeWriter> list = bucketMap.get(tsBucket);
		if (list == null) {
			// potential opportunity to load bucket information from some other
			// non-memory
			// location
			list = Collections.synchronizedList(new ArrayList<>());
			createNewTimeWriter(tsBucket, timestamp, list);
			bucketMap.put(tsBucket, list);
			logger.fine(() -> "Creating new time series bucket:" + fieldId + ",measurement:"
					+ measurement.getMeasurementName());
		}

		TimeWriter ans = list.get(list.size() - 1);
		if (ans.isFull()) {
			final TimeWriter ansTmp = ans;
			logger.fine(
					() -> "Requesting new writer for:" + fieldId + ",measurement:" + measurement.getMeasurementName()
							+ " bucketcount:" + bucketMap.size() + " pos:" + ansTmp.getPosition());
			ans = createNewTimeWriter(tsBucket, timestamp, list);
			// if there are more than 2 buffers in the list then it is a
			// candidate for
			// compaction else not because 2 or less buffers means there
			// is at least 1
			// writable buffer which can't be compacted
			// #COMPACTHRESHOLD
			if (compactionEnabled && list.size() > COMPACTION_THRESHOLD) {//
				// add older bucket to compaction queue
				final List<TimeWriter> listTmp = list;
				logger.fine(() -> "Adding bucket to compaction set:" + listTmp.size());
				compactionCandidateSet.put(tsBucket, list);
			}
		}
		if (StorageEngine.ENABLE_METHOD_METRICS) {
			ctx.stop();
		}
		return ans;
	}

	private TimeWriter createNewTimeWriter(int tsBucket, long timestamp, List<TimeWriter> list) throws IOException {
		Context ctx = null;
		if (StorageEngine.ENABLE_METHOD_METRICS) {
			ctx = timerCreateTimeWriter.time();
		}
		BufferObject bufPair = measurement.getMalloc().createNewBuffer(fieldId, tsBucket);
		bufPair.getBuf().put((byte) CompressionFactory.getIdByTimeClass(compressionClass));
		bufPair.getBuf().put((byte) list.size());
		TimeWriter writer;
		writer = getTimeWriterInstance(compressionClass);
		writer.setBufferId(bufPair.getBufferId());
		// first byte is used to store compression codec type
		writer.configure(bufPair.getBuf(), true, START_OFFSET);
		writer.setHeaderTimestamp(timestamp);
		list.add(writer);
		logger.fine(() -> "Created new writer for:" + tsBucket + " buckectInfo:" + bufPair.getBufferId());
		if (StorageEngine.ENABLE_METHOD_METRICS) {
			ctx.stop();
		}
		return writer;
	}

	private TimeWriter getTimeWriterInstance(Class<TimeWriter> compressionClass) {
		try {
			TimeWriter writer = compressionClass.newInstance();
			return writer;
		} catch (InstantiationException | IllegalAccessException e) {
			// should never happen unless the constructors are hidden
			throw new RuntimeException(e);
		}
	}

	/**
	 * Function to check and recover existing bucket map, if one exists.
	 * 
	 * @param bufferEntries
	 * @throws IOException
	 */
	public void loadBucketMap(List<Entry<Integer, BufferObject>> bufferEntries) throws IOException {
		logger.fine(() -> "Scanning buffer for:" + fieldId);
		for (Entry<Integer, BufferObject> entry : bufferEntries) {
			ByteBuffer duplicate = entry.getValue().getBuf();
			duplicate.rewind();
			// String series = getStringFromBuffer(duplicate);
			// if (!series.equalsIgnoreCase(seriesId)) {
			// continue;
			// }
			int tsBucket = entry.getKey();
			List<TimeWriter> list = bucketMap.get(tsBucket);
			if (list == null) {
				list = Collections.synchronizedList(new ArrayList<>());
				bucketMap.put(tsBucket, list);
			}
			ByteBuffer slice = duplicate.slice();
			int codecId = (int) slice.get();
			// int listIndex = (int) slice.get();
			Class<TimeWriter> classById = CompressionFactory.getTimeClassById(codecId);
			TimeWriter writer = getTimeWriterInstance(classById);
			if (entry.getValue().getBufferId() == null) {
				throw new IOException("Buffer id can't be read:" + measurement.getDbName() + ":"
						+ measurement.getMeasurementName() + " series:" + getFieldId());
			}
			logger.fine(() -> "Loading bucketmap:" + fieldId + "\t" + tsBucket + " bufferid:"
					+ entry.getValue().getBufferId());
			writer.setBufferId(entry.getValue().getBufferId());
			writer.configure(slice, false, START_OFFSET);
			list.add(writer);
			logger.fine(() -> "Loaded bucketmap:" + fieldId + "\t" + tsBucket + " bufferid:"
					+ entry.getValue().getBufferId());
		}
		sortBucketMap();
	}

	private void sortBucketMap() throws IOException {
		for (Entry<Integer, List<TimeWriter>> entry : bucketMap.entrySet()) {
			Collections.sort(entry.getValue(), new Comparator<TimeWriter>() {

				@Override
				public int compare(TimeWriter o1, TimeWriter o2) {
					return Integer.compare((int) o1.getRawBytes().get(1), (int) o2.getRawBytes().get(1));
				}
			});
			for (int i = 0; i < entry.getValue().size() - 1; i++) {
				TimeWriter writer = entry.getValue().get(i);
				writer.makeReadOnly();
			}
			// #COMPACTHRESHOLD
			if (entry.getValue().size() > COMPACTION_THRESHOLD) {
				compactionCandidateSet.put(entry.getKey(), entry.getValue());
			}
		}
	}

	private SortedMap<Integer, List<TimeWriter>> correctTimeRangeScan(long startTime, long endTime) {
		int tsStartBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, timeBucketSize) - timeBucketSize;
		int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, timeBucketSize);
		if (Integer.compare(tsStartBucket, bucketMap.firstKey()) < 0) {
			tsStartBucket = bucketMap.firstKey();
			logger.finest(() -> "Corrected query startKey to:" + bucketMap.firstKey());
		}
		SortedMap<Integer, List<TimeWriter>> series = null;
		if (bucketMap.size() <= 1) {
			series = bucketMap;
		} else {
			if (Integer.compare(tsEndBucket, bucketMap.lastKey()) > 0) {
				series = bucketMap.tailMap(tsStartBucket);
				logger.finest(() -> "Endkey exceeds last key, using tailmap instead");
			} else {
				tsEndBucket = tsEndBucket + 1;
				series = bucketMap.subMap(tsStartBucket, tsEndBucket);
			}
		}
		logger.fine("Series select size:" + series.size());
		return series;
	}

	/**
	 * Get {@link Reader} with time and value filter predicates pushed-down to it.
	 * 
	 * @param timePredicate
	 * @param valuePredicate
	 * @return point in time instance of reader
	 * @throws IOException
	 */
	public static Reader getReader(TimeWriter writer, Predicate timePredicate, Predicate valuePredicate)
			throws IOException {
		Reader reader = writer.getReader();
		reader.setPredicate(valuePredicate);
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
	public List<Reader> queryReader(long startTime, long endTime, Predicate valuePredicate, Lock readLock)
			throws IOException {
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		List<Reader> readers = new ArrayList<>();
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		SortedMap<Integer, List<TimeWriter>> series = correctTimeRangeScan(startTime, endTime);
		readLock.lock();
		for (List<TimeWriter> writers : series.values()) {
			for (TimeWriter writer : writers) {
				readers.add(getReader(writer, timeRangePredicate, valuePredicate));
			}
		}
		readLock.unlock();
		return readers;
	}

	/**
	 * Add data point with non floating point value
	 * 
	 * @param unit
	 *            of time for the supplied timestamp
	 * @param timestamp
	 *            of this data point
	 * @param timestamp
	 *            of this data point
	 * @throws IOException
	 */
	public void addDataPoint(int tsBucket, long timestamp) throws IOException {
		TimeWriter timeseriesBucket = getOrCreateSeriesBucket(tsBucket, timestamp);
		try {
			timeseriesBucket.add(timestamp);
		} catch (RollOverException e) {
			addDataPoint(tsBucket, timestamp);
		} catch (NullPointerException e) {
			logger.log(Level.SEVERE, "\n\nNPE occurred for add datapoint operation\n\n", e);
		}
	}

	// public void addDataPointLocked(int bucket, long value) throws IOException {
	// TimeWriter timeseriesBucket = getOrCreateSeriesBucketLocked(bucket);
	// try {
	// timeseriesBucket.add(value);
	// } catch (RollOverException e) {
	// addDataPointLocked(bucket, value);
	// } catch (NullPointerException e) {
	// logger.log(Level.SEVERE, "\n\nNPE occurred for add datapoint operation\n\n",
	// e);
	// }
	// }

	/**
	 * Cleans stale series
	 * 
	 * @throws IOException
	 */
	public Map<Integer, List<TimeWriter>> collectGarbage() throws IOException {
		Map<Integer, List<TimeWriter>> collectedGarbageMap = new HashMap<>();
		logger.finer("Retention buckets:" + measurement.getRetentionBuckets().get());
		while (bucketMap.size() > measurement.getRetentionBuckets().get()) {
			int oldSize = bucketMap.size();
			Integer key = bucketMap.firstKey();
			List<TimeWriter> buckets = bucketMap.remove(key);
			List<TimeWriter> gcedBuckets = new ArrayList<>();
			collectedGarbageMap.put(key, gcedBuckets);
			for (TimeWriter bucket : buckets) {
				// bucket.close();
				gcedBuckets.add(bucket);
				logger.log(Level.FINEST,
						"GC," + measurement.getMeasurementName() + ":" + fieldId + " removing bucket:" + key
								+ ": as it passed retention period of:" + measurement.getRetentionBuckets().get()
								+ ":old size:" + oldSize + ":newsize:" + bucketMap.size() + ":");
			}
		}
		if (collectedGarbageMap.size() > 0) {
			logger.fine(() -> "GC," + measurement.getMeasurementName() + " buckets:" + collectedGarbageMap.size()
					+ " retention size:" + measurement.getRetentionBuckets().get());
		}
		return collectedGarbageMap;
	}

	/**
	 * @return the bucketMap
	 */
	public SortedMap<Integer, TimeWriter> getBucketMap() {
		SortedMap<Integer, TimeWriter> map = new TreeMap<>();
		for (Entry<Integer, List<TimeWriter>> entry : bucketMap.entrySet()) {
			List<TimeWriter> value = entry.getValue();
			for (int i = 0; i < value.size(); i++) {
				TimeWriter bucketEntry = value.get(i);
				map.put(entry.getKey() + i, bucketEntry);
			}
		}
		return map;
	}

	public SortedMap<Integer, List<TimeWriter>> getBucketRawMap() {
		return bucketMap;
	}

	/**
	 * @return the seriesId
	 */
	public ByteString getFieldId() {
		return fieldId;
	}

	/**
	 * @param fieldId
	 *            the seriesId to set
	 */
	public void setFieldId(ByteString fieldId) {
		this.fieldId = fieldId;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TimeSeries [bucketMap=" + bucketMap + ", seriesId=" + fieldId + ", timeBucketSize=" + timeBucketSize
				+ "]";
	}

	public void close() throws IOException {
		// TODO close series
	}

	public int getTimeBucketSize() {
		return timeBucketSize;
	}

	/**
	 * Compacts old TimeWriters into one for every single time bucket, this insures
	 * the buffers are compacted as well as provides an opportunity to use a higher
	 * compression rate algorithm for the bucket. All TimeWriters but the last are
	 * read-only therefore performing operations on them does not impact.
	 * 
	 * @param functions
	 * @return returns null if nothing to compact or empty list if all compaction
	 *         attempts fail
	 * @throws IOException
	 */
	@SafeVarargs
	public final List<TimeWriter> compact(Consumer<List<TimeWriter>>... functions) throws IOException {
		Context ctx = null;
		if (StorageEngine.ENABLE_METHOD_METRICS) {
			ctx = timerCompaction.time();
		}
		// this loop only executes if there are any candidate buffers in the set
		// buckets should be moved out of the compaction set once they are
		// compacted
		// size check is to avoid unnecessary calls and exit fast
		if (compactionCandidateSet.isEmpty()) {
			return null;
		}
		List<TimeWriter> compactedTimeWriter = new ArrayList<>();
		Iterator<Entry<Integer, List<TimeWriter>>> iterator = compactionCandidateSet.entrySet().iterator();
		int id = CompressionFactory.getIdByTimeClass(compactionClass);
		while (iterator.hasNext()) {
			// entry.getKey() gives tsBucket string
			Entry<Integer, List<TimeWriter>> entry = iterator.next();
			// remove this entry from compaction set
			iterator.remove();
			List<TimeWriter> list = entry.getValue();
			int listSize = list.size() - 1;
			int pointCount = list.subList(0, listSize).stream().mapToInt(s -> s.getCount()).sum();
			int total = list.subList(0, listSize).stream().mapToInt(s -> s.getPosition()).sum();
			if (total == 0) {
				logger.warning("Ignoring bucket for compaction, not enough bytes. THIS BUG SHOULD BE INVESTIGATED");
				continue;
			}
			TimeWriter writer = getTimeWriterInstance(compactionClass);
			int compactedPoints = 0;
			double bufSize = total * compactionRatio;
			logger.finer("Allocating buffer:" + total + " Vs. " + pointCount * 16 + " max compacted buffer:" + bufSize);
			logger.finer("Getting sublist from:" + 0 + " to:" + (list.size() - 1));
			ByteBuffer buf = ByteBuffer.allocate((int) bufSize);
			buf.put((byte) id);
			// since this buffer will be the first one
			buf.put(1, (byte) 0);
			writer.configure(buf, true, START_OFFSET);
			TimeWriter input = list.get(0);
			// read all but the last writer and insert into new temp writer
			try {
				for (int i = 0; i < list.size() - 1; i++) {
					input = list.get(i);
					Reader reader = input.getReader();
					for (int k = 0; k < reader.getCount(); k++) {
						long pair = reader.read();
						writer.add(pair);
						compactedPoints++;
					}
				}
				writer.makeReadOnly();
			} catch (RollOverException e) {
				logger.warning("Buffer filled up; bad compression ratio; not compacting");
				continue;
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Compaction failed due to unknown exception", e);
			}
			// get the raw compressed bytes
			ByteBuffer rawBytes = writer.getRawBytes();
			// limit how much data needs to be read from the buffer
			rawBytes.limit(rawBytes.position());
			// convert buffer length request to size of 2
			int size = rawBytes.limit() + 1;
			if (size % 2 != 0) {
				size++;
			}
			rawBytes.rewind();
			// create buffer in measurement
			BufferObject newBuf = measurement.getMalloc().createNewBuffer(fieldId, entry.getKey(), size);
			logger.fine("Compacted buffer size:" + size + " vs " + total);
			LinkedByteString bufferId = newBuf.getBufferId();
			buf = newBuf.getBuf();
			writer = getTimeWriterInstance(compactionClass);
			buf.put(rawBytes);
			writer.setBufferId(bufferId);
			writer.configure(buf, false, START_OFFSET);
			writer.makeReadOnly();
			synchronized (list) {
				if (functions != null) {
					for (Consumer<List<TimeWriter>> function : functions) {
						function.accept(list);
					}
				}
				size = listSize - 1;
				logger.finest("Compaction debug size differences size:" + size + " listSize:" + listSize + " curr:"
						+ list.size());
				for (int i = size; i >= 0; i--) {
					compactedTimeWriter.add(list.remove(i));
				}
				list.add(0, writer);
				for (int i = 0; i < list.size(); i++) {
					list.get(i).getRawBytes().put(1, (byte) i);
				}
				logger.fine("Total points:" + compactedPoints + ", original pair count:" + writer.getReader().getCount()
						+ " compression ratio:" + rawBytes.position() + " original:" + total);
			}
		}
		if (StorageEngine.ENABLE_METHOD_METRICS) {
			ctx.stop();
		}
		return compactedTimeWriter;
	}

	/**
	 * FOR UNIT TESTING ONLY
	 * 
	 * @return
	 */
	public Collection<List<TimeWriter>> getCompactionSet() {
		return compactionCandidateSet.values();
	}

	/**
	 * Method to help fix bucket writers directly
	 * 
	 * @param bucket
	 * @param bufList
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public void replaceFirstBuckets(Integer bucket, List<Entry<Long, byte[]>> bufList)
			throws IOException, InstantiationException, IllegalAccessException {
		boolean wasEmpty = false;
		List<TimeWriter> list = bucketMap.get(bucket);
		if (list == null) {
			synchronized (bucketMap) {
				list = Collections.synchronizedList(new ArrayList<>());
				bucketMap.put(bucket, list);
				wasEmpty = true;
			}
		}
		synchronized (list) {
			// insert writers to list
			List<String> cleanupList = insertOrOverwriteTimeWriters(bufList, wasEmpty, list, bucket);
			measurement.getMalloc().cleanupBufferIds(new HashSet<>(cleanupList));
		}
	}

	private List<String> insertOrOverwriteTimeWriters(List<Entry<Long, byte[]>> bufList, boolean wasEmpty,
			List<TimeWriter> list, Integer tsBucket)
			throws IOException, InstantiationException, IllegalAccessException {
		List<String> garbageCollectTimeWriters = new ArrayList<>();
		if (!wasEmpty) {
			if (bufList.size() >= list.size()) {
				throw new IllegalArgumentException(
						"Buffer can't be replaced since local buffers are smaller than the replacing buffers");
			}
		}
		for (int i = 0; i < bufList.size(); i++) {
			if (!wasEmpty) {
				TimeWriter removedTimeWriter = list.remove(i);
				garbageCollectTimeWriters.add(removedTimeWriter.getBufferId().toString());
			}
			Entry<Long, byte[]> bs = bufList.get(i);
			BufferObject bufPair = measurement.getMalloc().createNewBuffer(fieldId, tsBucket, bs.getValue().length);
			ByteBuffer buf = bufPair.getBuf();
			buf.put(bs.getValue());
			buf.rewind();
			TimeWriter writer = CompressionFactory.getTimeClassById(buf.get(0)).newInstance();
			writer.setBufferId(bufPair.getBufferId());
			writer.configure(bufPair.getBuf(), false, START_OFFSET);
			list.add(i, writer);
		}
		return garbageCollectTimeWriters;
	}

	// public TimeWriter getOrCreateSeriesBucketLocked(int tsBucket) throws
	// IOException {
	// Context ctx = null;
	// if (StorageEngine.ENABLE_METHOD_METRICS) {
	// ctx = timerGetCreateSeriesBuckets.time();
	// }
	// List<TimeWriter> list = bucketMap.get(tsBucket);
	// if (list == null) {
	// // potential opportunity to load bucket information from some other
	// // non-memory
	// // location
	// synchronized (bucketMap) {
	// if ((list = bucketMap.get(tsBucket)) == null) {
	// list = Collections.synchronizedList(new ArrayList<>());
	// createNewTimeWriter(tsBucket, list);
	// bucketMap.put(tsBucket, list);
	// logger.fine(() -> "Creating new time series bucket:" + fieldId +
	// ",measurement:"
	// + measurement.getMeasurementName());
	// }
	// }
	// }
	//
	// synchronized (list) {
	// TimeWriter ans = list.get(list.size() - 1);
	// if (ans.isFull()) {
	// if ((ans = list.get(list.size() - 1)).isFull()) {
	// final TimeWriter ansTmp = ans;
	// logger.fine(() -> "Requesting new writer for:" + fieldId + ",measurement:"
	// + measurement.getMeasurementName() + " bucketcount:" + bucketMap.size() + "
	// pos:"
	// + ansTmp.getPosition());
	// ans = createNewTimeWriter(tsBucket, list);
	// // if there are more than 2 buffers in the list then it is a
	// // candidate for
	// // compaction else not because 2 or less buffers means there
	// // is at least 1
	// // writable buffer which can't be compacted
	// // #COMPACTHRESHOLD
	// if (compactionEnabled && list.size() > COMPACTION_THRESHOLD) {//
	// // add older bucket to compaction queue
	// final List<TimeWriter> listTmp = list;
	// logger.fine(() -> "Adding bucket to compaction set:" + listTmp.size());
	// compactionCandidateSet.put(tsBucket, list);
	// }
	// }
	// }
	// if (StorageEngine.ENABLE_METHOD_METRICS) {
	// ctx.stop();
	// }
	// return ans;
	// }
	// }

	// Old code used for thread safety checks
	// try {
	// int idx = list.indexOf(ans);
	// if (idx != (list.size() - 1)) {
	// System.out.println("\n\nThread safety error\t" + idx + "\t" +
	// list.size() +
	// "\n\n");
	// }
	// } catch (Exception e) {
	// logger.log(Level.SEVERE, "Create new:" + "\tList:" + list +
	// "\tbucket:" +
	// tsBucket + "\t" + bucketMap,
	// e);
	// throw e;
	// }

}
