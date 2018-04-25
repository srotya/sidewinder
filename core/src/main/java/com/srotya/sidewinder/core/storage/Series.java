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
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.predicates.BetweenPredicate;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * @author ambud
 */
public class Series {

	public static final String TS = "TS";
	private static final Logger logger = Logger.getLogger(Series.class.getName());
	private ByteString seriesId;
	private Map<String, Boolean> fieldTypeMap;
	private SortedMap<Integer, Map<String, Field>> bucketFieldMap;
	private int fieldMapIndex;
	private ReentrantReadWriteLock lock;
	private ReadLock readLock;
	private WriteLock writeLock;

	public Series(Measurement measurement, ByteString seriesId, int fieldMapIndex) {
		this.seriesId = seriesId;
		this.fieldMapIndex = fieldMapIndex;
		fieldTypeMap = new ConcurrentHashMap<>();
		bucketFieldMap = new ConcurrentSkipListMap<>();
		this.lock = new ReentrantReadWriteLock();
		readLock = lock.readLock();
		writeLock = lock.writeLock();
	}

	public Set<String> getFields() {
		return fieldTypeMap.keySet();
	}

	protected Field getOrCreateSeries(int timeBucket, String valueFieldName, boolean fp, Measurement measurement)
			throws IOException {
		Map<String, Field> map = bucketFieldMap.get(timeBucket);
		Field field = map.get(valueFieldName);
		if (field == null) {
			writeLock.lock();
			if ((field = map.get(valueFieldName)) == null) {
				ByteString fieldId = new ByteString(seriesId + Measurement.SERIESID_SEPARATOR + valueFieldName);
				if (valueFieldName == TS) {
					field = new TimeField(measurement, fieldId, measurement.getTimeBucketSize(), measurement.getConf());
				} else {
					field = new ValueField(measurement, fieldId, measurement.getTimeBucketSize(),
							measurement.getConf());
				}
				if (fieldTypeMap.get(valueFieldName) == null) {
					fieldTypeMap.put(valueFieldName, fp);
				}
				map.put(valueFieldName, field);
				measurement.appendTimeseriesToMeasurementMetadata(fieldId, fp, measurement.getTimeBucketSize(),
						fieldMapIndex);
				final Field tmp = field;
				logger.fine(
						() -> "Created new timeseries:" + tmp + " for measurement:" + measurement.getMeasurementName()
								+ "\t" + seriesId + "\t" + measurement.getMetadata().getRetentionHours() + "\t"
								+ measurement.getSeriesList().size() + " field:" + valueFieldName);
			} else {
				// in case there was contention and we have to re-check the cache
				field = map.get(valueFieldName);
			}
			writeLock.unlock();
		}
		return field;
	}

	public void addPoint(Point dp, Measurement m) throws IOException {
		writeLock.lock();
		try {
			int timeBucket = getOrCreateTimeBucket(dp.getTimestamp(), m.getTimeBucketSize());
			Field timeField = getOrCreateSeries(timeBucket, TS, false, m);
			timeField.addDataPoint(m, dp.getTimestamp());
			for (int i = 0; i < dp.getFpList().size(); i++) {
				Field field = getOrCreateSeries(timeBucket, dp.getValueFieldNameList().get(i), dp.getFpList().get(i),
						m);
				field.addDataPoint(m, dp.getValueList().get(i));
			}
		} finally {
			writeLock.unlock();
		}
	}

	private int getOrCreateTimeBucket(long timestamp, int timeBucketSize) {
		int timeBucketInt = getTimeBucketInt(TimeUnit.MILLISECONDS, timestamp, timeBucketSize);
		Map<String, Field> map = bucketFieldMap.get(timeBucketInt);
		if (map == null) {
			writeLock.lock();
			if ((map = bucketFieldMap.get(timeBucketInt)) == null) {
				map = new ConcurrentHashMap<>();
				bucketFieldMap.put(timeBucketInt, map);
			}
			writeLock.unlock();
		}
		return timeBucketInt;
	}

	public static int getTimeBucketInt(TimeUnit unit, long timestamp, int timeBucketSize) {
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, timeBucketSize);
		return bucket;
	}

	/**
	 * Extract {@link DataPoint}s for the supplied time range and value predicate.
	 * 
	 * Each {@link DataPoint} has the appendFieldBucketValue and appendTags set in
	 * it.
	 * 
	 * @param startTime
	 *            time range beginning
	 * @param endTime
	 *            time range end
	 * @param valuePredicate
	 *            pushed down filter for values
	 * @return list of datapoints
	 * @throws IOException
	 */
	public Map<String, List<DataPoint>> queryDataPoints(Measurement measurement, List<String> valueFieldBucketNames,
			long startTime, long endTime, List<Predicate> valuePredicate) throws IOException {
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		SortedMap<Integer, Map<String, Field>> correctTimeRangeScan = correctTimeRangeScan(startTime, endTime,
				measurement.getTimeBucketSize());
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		Map<String, List<DataPoint>> points = new HashMap<>();
		for (Map<String, Field> map : correctTimeRangeScan.values()) {
			Field timeField = map.get(TS);
			for (int i = 0; i < valueFieldBucketNames.size(); i++) {
				String vfn = valueFieldBucketNames.get(i);
				Field field = map.get(vfn);
				if (field != null) {
					List<DataPoint> list = points.get(vfn);
					if (list == null) {
						list = new ArrayList<>();
						points.put(vfn, list);
					}
					FieldReaderIterator valueIterator = field
							.queryReader(valuePredicate != null ? valuePredicate.get(i) : null, readLock);
					FieldReaderIterator timeIterator = timeField.queryReader(timeRangePredicate, readLock);
					while (true) {
						try {
							long ts = timeIterator.next();
							long value = valueIterator.next();
							list.add(new DataPoint(ts, value));
						} catch (FilteredValueException e) {
							// ignore this
						} catch (IOException e) {
							// terminate read loop
							break;
						}
					}
				}
			}
		}
		return points;
	}

	public FieldReaderIterator[] queryTuples(Measurement measurement, List<String> valueFieldBucketNames,
			long startTime, long endTime) throws IOException {
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		SortedMap<Integer, Map<String, Field>> correctTimeRangeScan = correctTimeRangeScan(startTime, endTime,
				measurement.getTimeBucketSize());
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);

		FieldReaderIterator[] output = new FieldReaderIterator[valueFieldBucketNames.size() + 1];
		for (int i = 0; i < output.length; i++) {
			output[i] = new FieldReaderIterator();
		}
		for (Entry<Integer, Map<String, Field>> entry : correctTimeRangeScan.entrySet()) {
			Map<String, Field> map = entry.getValue();
			Field timeField = map.get(TS);
			output[0].addReader(timeField.queryReader(timeRangePredicate, readLock).getReaders());
			for (int i = 0; i < valueFieldBucketNames.size(); i++) {
				String vfn = valueFieldBucketNames.get(i);
				Field field = map.get(vfn);
				if (field != null) {
					output[i + 1] = field.queryReader(null, readLock);
				}
			}
		}
		return output;
	}

	public List<long[]> queryTuples(Measurement measurement, List<String> valueFieldBucketNames, long startTime,
			long endTime, List<Predicate> valuePredicate) throws IOException {
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		SortedMap<Integer, Map<String, Field>> correctTimeRangeScan = correctTimeRangeScan(startTime, endTime,
				measurement.getTimeBucketSize());
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		List<long[]> points = new ArrayList<>();
		for (Map<String, Field> map : correctTimeRangeScan.values()) {
			Field timeField = map.get(TS);
			FieldReaderIterator[] iterators = new FieldReaderIterator[valueFieldBucketNames.size() + 1];
			iterators[0] = timeField.queryReader(timeRangePredicate, readLock);
			for (int i = 0; i < valueFieldBucketNames.size(); i++) {
				String vfn = valueFieldBucketNames.get(i);
				Field field = map.get(vfn);
				if (field != null) {
					iterators[i + 1] = field.queryReader(valuePredicate != null ? valuePredicate.get(i) : null,
							readLock);
				}
			}
			while (true) {
				try {
					long[] tuple = FieldReaderIterator.extracted(iterators.length, iterators);
					points.add(tuple);
				} catch (FilteredValueException e) {
					// ignore this
				} catch (IOException e) {
					// terminate read loop
					break;
				}
			}
		}
		return points;
	}

	private SortedMap<Integer, Map<String, Field>> correctTimeRangeScan(long startTime, long endTime,
			int timeBucketSize) {
		int tsStartBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, timeBucketSize) - timeBucketSize;
		int tsEndBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, timeBucketSize);
		if (Integer.compare(tsStartBucket, bucketFieldMap.firstKey()) < 0) {
			tsStartBucket = bucketFieldMap.firstKey();
			logger.finest(() -> "Corrected query startKey to:" + bucketFieldMap.firstKey());
		}
		SortedMap<Integer, Map<String, Field>> series = null;
		if (bucketFieldMap.size() <= 1) {
			series = bucketFieldMap;
		} else {
			if (Integer.compare(tsEndBucket, bucketFieldMap.lastKey()) > 0) {
				series = bucketFieldMap.tailMap(tsStartBucket);
				logger.finest(() -> "Endkey exceeds last key, using tailmap instead");
			} else {
				tsEndBucket = tsEndBucket + 1;
				series = bucketFieldMap.subMap(tsStartBucket, tsEndBucket);
			}
		}
		logger.fine("Series select size:" + series.size());
		return series;
	}

	@Override
	public String toString() {
		return "SeriesFieldBucketMap [seriesId=" + seriesId + " seriesMap=" + bucketFieldMap + "]";
	}

	public boolean isFp(String key) throws ItemNotFoundException {
		Boolean v = fieldTypeMap.get(key);
		if (v == null) {
			throw StorageEngine.NOT_FOUND_EXCEPTION;
		}
		return v;
	}

	// /**
	// * Must close before modifying this via iterator
	// *
	// * @return
	// */
	// public Collection<? extends Field> values() {
	// return fieldMap.values();
	// }

	/**
	 * @return the seriesId
	 */
	public ByteString getSeriesId() {
		return seriesId;
	}

	/**
	 * @param seriesId
	 *            the seriesId to set
	 */
	public void setSeriesId(ByteString seriesId) {
		this.seriesId = seriesId;
	}

	public List<Writer> compact() throws IOException {

		return null;
	}

	public SortedMap<Integer, Map<String, Field>> getBucketFieldMap() {
		return bucketFieldMap;
	}

	protected Map<String, Boolean> getFieldTypeMap() {
		return fieldTypeMap;
	}

	protected int getFieldMapIndex() {
		return fieldMapIndex;
	}

	protected ReentrantReadWriteLock getLock() {
		return lock;
	}

	protected ReadLock getReadLock() {
		return readLock;
	}

	protected WriteLock getWriteLock() {
		return writeLock;
	}

	// /**
	// * Converts timeseries to a list of datapoints appended to the supplied list
	// * object. Datapoints are filtered by the supplied predicates before they are
	// * returned. These predicates are pushed down to the reader for efficiency and
	// * performance as it prevents unnecessary object creation.
	// *
	// * @param appendFieldBucketValueName
	// * @param appendTags
	// *
	// * @param points
	// * list data points are appended to
	// * @param writer
	// * to extract the data points from
	// * @param timePredicate
	// * time range filter
	// * @param valuePredicate
	// * value filter
	// * @return the points argument
	// * @throws IOException
	// */
	// public static List<DataPoint> seriesToDataPoints(List<String> appendTags,
	// List<DataPoint> points,
	// ValueWriter writer, Predicate timePredicate, Predicate valuePredicate,
	// boolean isFp) throws IOException {
	// Reader reader = getReader(writer, timePredicate, valuePredicate);
	// DataPoint point = null;
	// while (true) {
	// try {
	// point = reader.readPair();
	// if (point != null) {
	// points.add(point);
	// }
	// } catch (IOException e) {
	// if (e instanceof RejectException) {
	// } else {
	// throw new IOException(e);
	// }
	// break;
	// }
	// }
	// return points;
	// }
	//
	// public static void readerToDataPoints(List<DataPoint> points, Reader reader)
	// throws IOException {
	// DataPoint point = null;
	// while (true) {
	// try {
	// point = reader.readPair();
	// if (point != null) {
	// points.add(point);
	// }
	// } catch (IOException e) {
	// if (e instanceof RejectException) {
	// } else {
	// throw new IOException(e);
	// }
	// break;
	// }
	// }
	// if (reader.getCounter() != reader.getCount() || points.size() <
	// reader.getCounter()) {
	// logger.finest(() -> "SDP:" + points.size() + "/" + reader.getCounter() + "/"
	// + reader.getCount());
	// }
	// }

	// public static void readerToPoints(List<long[]> points, Reader reader) throws
	// IOException {
	// long[] point = null;
	// while (true) {
	// try {
	// point = reader.read();
	// if (point != null) {
	// points.add(point);
	// }
	// } catch (IOException e) {
	// if (e instanceof RejectException) {
	// } else {
	// logger.log(Level.SEVERE, "Non rejectexception while reading datapoints", e);
	// }
	// break;
	// }
	// }
	// if (reader.getCounter() != reader.getCount() || points.size() <
	// reader.getCounter()) {
	// logger.finest(() -> "SDP:" + points.size() + "/" + reader.getCounter() + "/"
	// + reader.getCount());
	// }
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */

}
