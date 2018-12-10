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
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Consumer;
import java.util.logging.Level;
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
	private SortedMap<Integer, Map<String, Field>> bucketFieldMap;
	private int fieldMapIndex;
	private ReentrantReadWriteLock lock;
	private ReadLock readLock;
	private WriteLock writeLock;

	public Series(ByteString seriesId, int fieldMapIndex) {
		this.seriesId = seriesId;
		this.fieldMapIndex = fieldMapIndex;
		bucketFieldMap = new ConcurrentSkipListMap<>();
		this.lock = new ReentrantReadWriteLock();
		readLock = lock.readLock();
		writeLock = lock.writeLock();
	}

	protected Field getOrCreateSeries(int timeBucket, String valueFieldName, boolean fp, Measurement measurement)
			throws IOException {
		Map<String, Field> map = bucketFieldMap.get(timeBucket);
		Field field = map.get(valueFieldName);
		if (field == null) {
			writeLock.lock();
			if ((field = map.get(valueFieldName)) == null) {
				ByteString cachedFieldName = measurement.getFieldCache().get(new ByteString(valueFieldName));
				LinkedByteString fieldId = new LinkedByteString().concat(seriesId)
						.concat(Measurement.SERIESID_SEPARATOR_BS).concat(cachedFieldName);
				if (valueFieldName == TS) {
					field = new TimeField(measurement, fieldId, timeBucket, measurement.getConf());
				} else {
					field = new ValueField(measurement, fieldId, timeBucket, measurement.getConf());
				}
				if (measurement.getFieldTypeMap().get(valueFieldName) == null) {
					measurement.getFieldTypeMap().put(valueFieldName.intern(), fp);
					measurement.appendFieldMetadata(valueFieldName, fp);
				}
				map.put(valueFieldName.intern(), field);
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

	public void loadBuffers(Measurement measurement, String fieldName, List<Entry<Integer, BufferObject>> buffers,
			Map<String, String> conf) throws IOException {
		Map<Field, List<BufferObject>> fieldMap = new HashMap<>();
		for (Entry<Integer, BufferObject> entry : buffers) {
			Map<String, Field> map = bucketFieldMap.get(entry.getKey());
			if (map == null) {
				map = new ConcurrentHashMap<>();
				bucketFieldMap.put(entry.getKey(), map);
			}
			BufferObject value = entry.getValue();
			Field field = map.get(fieldName);
			if (field == null) {
				ByteString cachedFieldName = measurement.getFieldCache().get(new ByteString(fieldName));
				LinkedByteString fieldId = new LinkedByteString().concat(seriesId)
						.concat(Measurement.SERIESID_SEPARATOR_BS).concat(cachedFieldName);
				if (fieldName.equals(TS)) {
					field = new TimeField(measurement, fieldId, entry.getKey(), conf);
				} else {
					field = new ValueField(measurement, fieldId, entry.getKey(), conf);
				}
				map.put(fieldName.intern(), field);
				fieldMap.put(field, new ArrayList<>());
			}
			fieldMap.get(field).add(value);
		}
		for (Entry<Field, List<BufferObject>> entry : fieldMap.entrySet()) {
			entry.getKey().loadBucketMap(measurement, entry.getValue());
		}
	}

	/**
	 * Extract {@link DataPoint}s for the supplied time range and value predicate.
	 * Please note that predicates for this method are individually applied
	 * therefore predicate for one field doesn't impact filtering for another field
	 * even though these fields are for the same series.
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
		SortedMap<Integer, Map<String, Field>> correctTimeRangeScan = correctTimeRangeScan(startTime, Long.MAX_VALUE,
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
					logger.fine(() -> vfn + " " + valueIterator.count() + " ts:" + timeIterator.count());
					FieldReaderIterator[] iterators = new FieldReaderIterator[] { timeIterator, valueIterator };
					while (true) {
						try {
							long[] extracted = FieldReaderIterator.extracted(iterators);
							list.add(new DataPoint(extracted[0], extracted[1]));
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

	/**
	 * Query iterators for this series. The returned array has n+1 elements if the
	 * valueFieldBucketNames is missing the timestamp column, here n is the number
	 * of valueFieldNames, the n+1 entry in the array is timestamp column.
	 * 
	 * @param measurement
	 * @param valueFieldBucketNames
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws IOException
	 */
	public FieldReaderIterator[] queryIterators(Measurement measurement, List<String> valueFieldBucketNames,
			long startTime, long endTime) throws IOException {
		return queryIterators(measurement, valueFieldBucketNames, null, startTime, endTime);
	}

	/**
	 * Return an array of 2 element iterators one for the valueField (index 1) and
	 * another for the timefield (index 0) e.g. 0: vfn1 -> ts, vfn1 1: vfn2 -> ts,
	 * vfn2
	 * 
	 * @param measurement
	 * @param valueFieldBucketNames
	 * @param valuePredicates
	 * @param startTime
	 * @param endTime
	 * @return
	 * @throws IOException
	 */
	public FieldReaderIterator[][] queryTimePairIterators(Measurement measurement, List<String> valueFieldBucketNames,
			List<Predicate> valuePredicates, long startTime, long endTime) throws IOException {
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		FieldReaderIterator[][] result = new FieldReaderIterator[valueFieldBucketNames.size()][2];
		SortedMap<Integer, Map<String, Field>> correctTimeRangeScan = correctTimeRangeScan(startTime, endTime,
				measurement.getTimeBucketSize());
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);
		for (Map<String, Field> map : correctTimeRangeScan.values()) {
			Field timeField = map.get(TS);
			for (int i = 0; i < valueFieldBucketNames.size(); i++) {
				String vfn = valueFieldBucketNames.get(i);
				Field field = map.get(vfn);
				if (vfn == null || field == null) {
					return null;
				}
				FieldReaderIterator[] iterators = result[i];
				if (iterators == null) {
					iterators = new FieldReaderIterator[2];
					result[i] = iterators;
				}
				if (iterators[0] == null) {
					iterators[0] = timeField.queryReader(timeRangePredicate, readLock);
					iterators[0].setFieldName(TS);
				} else {
					iterators[0].addReader(timeField.queryReader(timeRangePredicate, readLock).getReaders());
				}
				if (field != null) {
					if (iterators[1] == null) {
						iterators[1] = field.queryReader(valuePredicates != null ? valuePredicates.get(i) : null,
								readLock);
						iterators[1].setFieldName(vfn);
					} else {
						iterators[1].addReader(
								field.queryReader(valuePredicates != null ? valuePredicates.get(i) : null, readLock)
										.getReaders());
					}
				}
			}
		}
		return result;
	}

	public FieldReaderIterator[] queryIterators(Measurement measurement, List<String> valueFieldBucketNames,
			List<Predicate> valuePredicates, long startTime, long endTime) throws IOException {
		if (valuePredicates != null && valueFieldBucketNames.size() != valuePredicates.size()) {
			throw new IOException("Predicate count doesn't match field count");
		}
		if (startTime > endTime) {
			// swap start and end times if they are off
			startTime = startTime ^ endTime;
			endTime = endTime ^ startTime;
			startTime = startTime ^ endTime;
		}
		SortedMap<Integer, Map<String, Field>> correctTimeRangeScan = correctTimeRangeScan(startTime, endTime,
				measurement.getTimeBucketSize());
		BetweenPredicate timeRangePredicate = new BetweenPredicate(startTime, endTime);

		int length = valueFieldBucketNames.size();
		if (!valueFieldBucketNames.contains(TS) && length > 0) {
			length++;
		}
		FieldReaderIterator[] output = new FieldReaderIterator[length];
		for (int i = 0; i < length; i++) {
			output[i] = new FieldReaderIterator();
		}
		for (Entry<Integer, Map<String, Field>> entry : correctTimeRangeScan.entrySet()) {
			Map<String, Field> map = entry.getValue();
			Field timeField = map.get(TS);
			if (length > valueFieldBucketNames.size()) {
				output[length - 1].addReader(timeField.queryReader(timeRangePredicate, readLock).getReaders());
			}
			for (int i = 0; i < valueFieldBucketNames.size(); i++) {
				String vfn = valueFieldBucketNames.get(i);
				Field field = map.get(vfn);
				if (field != null) {
					Predicate predicate = valuePredicates != null ? valuePredicates.get(i) : null;
					output[i].addReader(field.queryReader(predicate, readLock).getReaders());
					output[i].setFieldName(vfn);
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
					long[] tuple = FieldReaderIterator.extracted(iterators);
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
		if (startTime < 0) {
			startTime = 0;
		}
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
			if (Integer.compare(tsEndBucket, bucketFieldMap.lastKey()) > 0 || tsEndBucket < 0) {
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

	@SuppressWarnings("unchecked")
	public List<Writer> compact(Measurement measurement, Consumer<List<? extends Writer>>... functions)
			throws IOException {
		List<Writer> compact = new ArrayList<>();
		for (Map<String, Field> map : bucketFieldMap.values()) {
			for (Field field : map.values()) {
				List<Writer> tmp = field.compact(measurement, writeLock);
				if (tmp != null) {
					compact.addAll(tmp);
				} else {
					logger.info(() -> "Nothing compacted for field:" + field.getFieldId().toString());
				}
			}
		}
		logger.fine("Compaction completed for series:" + seriesId + " compacted buffers:" + compact.size());
		return compact;
	}

	public SortedMap<Integer, Map<String, Field>> getBucketMap() {
		return bucketFieldMap;
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

	public Map<Integer, List<Writer>> collectGarbage(Measurement measurement) throws IOException {
		Map<Integer, List<Writer>> collectedGarbageMap = new HashMap<>();
		logger.finer("Retention buckets:" + measurement.getRetentionBuckets().get());
		while (getBucketMap().size() > measurement.getRetentionBuckets().get()) {
			writeLock.lock();
			int oldSize = getBucketMap().size();
			Integer key = getBucketMap().firstKey();
			Map<String, Field> fieldMap = getBucketMap().remove(key);
			List<Writer> gcedBuckets = new ArrayList<>();
			collectedGarbageMap.put(key, gcedBuckets);
			for (Field field : fieldMap.values()) {
				// bucket.close();
				gcedBuckets.addAll(field.getWriters());
				logger.log(Level.FINEST,
						"GC," + measurement.getMeasurementName() + ":" + seriesId + " removing bucket:" + key
								+ ": as it passed retention period of:" + measurement.getRetentionBuckets().get()
								+ ":old size:" + oldSize + ":newsize:" + getBucketMap().size() + ":");
			}
			writeLock.unlock();
		}
		if (collectedGarbageMap.size() > 0) {
			logger.fine(() -> "GC," + measurement.getMeasurementName() + " buckets:" + collectedGarbageMap.size()
					+ " retention size:" + measurement.getRetentionBuckets().get());
		}
		return collectedGarbageMap;

	}

}
