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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.codahale.metrics.Counter;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.functions.iterative.FunctionIteratorFactory;
import com.srotya.sidewinder.core.functions.list.Function;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.ByteString.ByteStringCache;
import com.srotya.sidewinder.core.storage.archival.Archiver;
import com.srotya.sidewinder.core.storage.archival.TimeSeriesArchivalObject;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;
import com.srotya.sidewinder.core.storage.compression.Writer;

/**
 * @author ambud
 */
public interface Measurement {

	public static final RejectException INDEX_REJECT = new RejectException("Invalid tag, rejecting index");
	public static final RejectException SEARCH_REJECT = new RejectException("Invalid tag, rejecting index search");
	public static final String TAG_KV_SEPARATOR = "=";
	public static final String SERIESID_SEPARATOR = "#";
	public static final ByteString SERIESID_SEPARATOR_BS = new ByteString(SERIESID_SEPARATOR);
	public static final String USE_QUERY_POOL = "use.query.pool";
	public static final String TAG_SEPARATOR = "^";
	public static final TagComparator TAG_COMPARATOR = new TagComparator();
	public static final Exception NOT_FOUND_EXCEPTION = null;

	public void configure(Map<String, String> conf, StorageEngine engine, int defaultTimeBucketSize, String dbName,
			String measurementName, String baseIndexDirectory, String dataDirectory, DBMetadata metadata,
			ScheduledExecutorService bgTaskPool) throws IOException;

	public Set<ByteString> getSeriesKeys();

	public default Series getSeriesUsingKey(ByteString key) {
		Integer index = getSeriesMap().get(key);
		if (index == null) {
			return null;
		} else {
			return getSeriesList().get(index);
		}
	}

	public TagIndex getTagIndex();

	public void loadTimeseriesInMeasurements() throws IOException;

	public void close() throws IOException;

	public default Series getOrCreateSeries(List<Tag> tags, boolean preSorted) throws IOException {
		if (!preSorted) {
			Collections.sort(tags, TAG_COMPARATOR);
		}
		ByteString seriesId = constructSeriesId(tags);
		int index = 0;
		Series series = getSeriesUsingKey(seriesId);
		if (series == null) {
			getLock().lock();
			try {
				if ((series = getSeriesUsingKey(seriesId)) == null) {
					index = getSeriesList().size();
					Measurement.indexRowKey(getTagIndex(), index, tags);
					series = new Series(seriesId, index);
					getSeriesList().add(series);
					getSeriesMap().put(seriesId, index);

					appendTimeseriesToMeasurementMetadata(seriesId, index);

					if (isEnableMetricsCapture()) {
						getMetricsTimeSeriesCounter().inc();
					}
					final ByteString tmp = seriesId;
					getLogger().fine(() -> "Created new series:" + tmp + "\t");
				} else {
					index = getSeriesMap().get(seriesId);
				}
			} finally {
				getLock().unlock();
			}
		} else {
			index = getSeriesMap().get(seriesId);
		}

		return series;
		/*
		 * lock.lock(); try { if ((series = seriesFieldMap.get(valueFieldName)) == null)
		 * { ByteString seriesId2 = new ByteString(seriesId + SERIESID_SEPARATOR +
		 * valueFieldName); series = new TimeSeries(this, seriesId2, timeBucketSize,
		 * metadata, fp, conf); if (enableMetricsCapture) {
		 * metricsTimeSeriesCounter.inc(); }
		 * seriesFieldMap.getOrCreateSeries(valueFieldName, series);
		 * appendTimeseriesToMeasurementMetadata(seriesId2, fp, timeBucketSize, index);
		 * final SeriesFieldMap tmp = seriesFieldMap; logger.fine(() ->
		 * "Created new timeseries:" + tmp + " for measurement:" + measurementName +
		 * "\t" + seriesId + "\t" + metadata.getRetentionHours() + "\t" +
		 * seriesList.size()); } } finally { lock.unlock(); } }
		 * 
		 */
	}

	public static void indexRowKey(TagIndex tagIndex, int rowIdx, List<Tag> tags) throws IOException {
		for (Tag tag : tags) {
			tagIndex.index(tag.getTagKey(), tag.getTagValue(), rowIdx);
		}
	}

	public default ByteString encodeTagsToString(List<Tag> tags) throws IOException {
		StringBuilder builder = new StringBuilder(tags.size() * 5);
		serializeTagForKey(builder, tags.get(0));
		for (int i = 1; i < tags.size(); i++) {
			Tag tag = tags.get(i);
			builder.append(TAG_SEPARATOR);
			serializeTagForKey(builder, tag);
		}
		String rowKey = builder.toString();
		return new ByteString(rowKey);
	}

	public static void serializeTagForKey(StringBuilder output, Tag tag) {
		output.append(tag.getTagKey());
		output.append(TAG_KV_SEPARATOR);
		output.append(tag.getTagValue());
	}

	public default ByteString constructSeriesId(List<Tag> tags) throws IOException {
		return encodeTagsToString(tags);
	}

	public default List<Tag> decodeStringToTags(ByteString tagString) throws IOException {
		List<Tag> tagList = new ArrayList<>();
		if (tagString == null || tagString.isEmpty()) {
			return tagList;
		}
		for (ByteString tag : tagString.split(TAG_SEPARATOR)) {
			ByteString[] split = tag.split(TAG_KV_SEPARATOR);
			if (split.length != 2) {
				throw SEARCH_REJECT;
			}
			tagList.add(Tag.newBuilder().setTagKey(split[0].toString()).setTagValue(split[1].toString()).build());
		}
		return tagList;
	}

	public String getMeasurementName();

	public default List<List<Tag>> getTagsForMeasurement() throws Exception {
		Set<ByteString> keySet = getSeriesKeys();
		List<List<Tag>> tagList = new ArrayList<>();
		for (ByteString entry : keySet) {
			List<Tag> tags = decodeStringToTags(entry);
			tagList.add(tags);
		}
		return tagList;
	}

	public default Set<ByteString> getTagFilteredRowKeys(TagFilter tagFilterTree) throws IOException {
		return getTagIndex().searchRowKeysForTagFilter(tagFilterTree);
	}

	public default void addPointWithLocking(Point dp, boolean preSorted) throws IOException {
		Series fieldMap = getOrCreateSeries(new ArrayList<>(dp.getTagsList()), preSorted);
		fieldMap.addPoint(dp, this);
	}

	public default void addPointWithoutLocking(Point dp, boolean preSorted) throws IOException {
		Series fieldMap = getOrCreateSeries(new ArrayList<>(dp.getTagsList()), preSorted);
		fieldMap.addPoint(dp, this);
	}

	public int getTimeBucketSize();

	public default Set<String> collectGarbage(Archiver archiver) throws IOException {
		return runCleanupOperation("garbage collection", series -> {
			try {
				Map<Integer, List<Writer>> collectedGarbage = series.collectGarbage(this);
				List<Writer> output = new ArrayList<>();
				if (collectedGarbage.size() > 0) {
					getLogger()
							.fine("Collected garbage:" + collectedGarbage.size() + " series:" + series.getSeriesId());
				}
				if (collectedGarbage != null) {
					for (Entry<Integer, List<Writer>> entry : collectedGarbage.entrySet()) {
						for (Writer writer : entry.getValue()) {
							if (archiver != null) {
								byte[] buf = Archiver.writerToByteArray(writer);
								TimeSeriesArchivalObject archivalObject = new TimeSeriesArchivalObject(getDbName(),
										getMeasurementName(), series.getSeriesId(), entry.getKey(), buf);
								try {
									archiver.archive(archivalObject);
								} catch (ArchiveException e) {
									getLogger().log(Level.SEVERE,
											"Series failed to archive, series:" + series.getSeriesId() + " db:"
													+ getDbName() + " m:" + getMeasurementName(),
											e);
								}
							}
							output.add(writer);
						}
					}
				}
				return output;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@SuppressWarnings("unchecked")
	public default Set<String> compact() throws IOException {
		if (getMetricsCompactionCounter() != null) {
			getMetricsCompactionCounter().inc();
		}
		return runCleanupOperation("compacting", series -> {
			try {
				return series.compact(this);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public default Set<String> runCleanupOperation(String operation,
			java.util.function.Function<Series, List<Writer>> op) throws IOException {
		Set<String> cleanupList = new HashSet<>();
		getLock().lock();
		try {
			List<Series> seriesList = getSeriesList();
			Set<String> temp = new HashSet<>();
			for (int i = 0; i < seriesList.size(); i++) {
				Series entry = seriesList.get(i);
				try {
					List<Writer> list = op.apply(entry);
					if (list == null) {
						continue;
					}
					for (Writer timeSeriesBucket : list) {
						if (getMetricsCleanupBufferCounter() != null) {
							getMetricsCleanupBufferCounter().inc();
						}
						String buf = timeSeriesBucket.getBufferId().toString();
						temp.add(buf);
						cleanupList.add(buf);
						getLogger().fine("Adding buffer to cleanup " + operation + " for bucket:" + entry.getSeriesId()
								+ " Offset:" + timeSeriesBucket.currentOffset());
					}
					getLogger().fine("Buffers " + operation + " for time series:" + entry.getSeriesId());
					if (i % 100 == 0) {
						if (temp.size() > 0) {
							getMalloc().cleanupBufferIds(temp);
							temp = new HashSet<>();
						}
					}
				} catch (Exception e) {
					getLogger().log(Level.SEVERE, "Error collecing " + operation, e);
				}
			}
			// cleanup these buffer ids
			if (cleanupList.size() > 0) {
				getLogger().info(
						"For measurement:" + getMeasurementName() + " cleaned=" + cleanupList.size() + " buffers");
			}
			getMalloc().cleanupBufferIds(cleanupList);
		} finally {
			getLock().unlock();
		}
		return cleanupList;
	}

	public default Series getSeriesField(List<Tag> tags) throws IOException {
		Collections.sort(tags, TAG_COMPARATOR);
		ByteString rowKey = constructSeriesId(tags);
		// check and create timeseries
		Series map = getSeriesUsingKey(rowKey);
		return map;
	}

	public default Set<String> getFieldsForMeasurement() {
		return getFields();
	}

	public default void queryDataPoints(String valueFieldNamePattern, long startTime, long endTime, TagFilter tagFilter,
			Predicate valuePredicate, List<SeriesOutput> resultMap, Function function) throws IOException {
		final Set<ByteString> rowKeys;
		if (tagFilter == null) {
			rowKeys = getSeriesKeys();
		} else {
			rowKeys = getTagFilteredRowKeys(tagFilter);
		}
		getLogger()
				.fine(() -> "Filtered row keys to query(" + valueFieldNamePattern + "," + tagFilter + "):" + rowKeys);
		final Pattern p;
		try {
			p = Pattern.compile(valueFieldNamePattern);
		} catch (Exception e) {
			throw new IOException("Invalid regex for value field name:" + e.getMessage());
		}
		Set<ByteString> outputKeys = new HashSet<>();
		final Map<ByteString, List<String>> fields = new HashMap<>();
		if (rowKeys != null) {
			for (ByteString key : rowKeys) {
				List<String> fieldMap = new ArrayList<>();
				List<String> fieldSet = new ArrayList<>(getFields());
				getLogger().fine(() -> "Row key:" + key + " Fields:" + fieldSet);
				for (String fieldSetEntry : fieldSet) {
					if (p.matcher(fieldSetEntry).matches() && !fieldSetEntry.equalsIgnoreCase(Series.TS)) {
						fieldMap.add(fieldSetEntry);
					}
				}
				if (fieldMap.size() > 0) {
					fields.put(key, fieldMap);
					outputKeys.add(key);
				}
			}
		}

		Stream<ByteString> stream = outputKeys.stream();
		if (useQueryPool()) {
			stream = stream.parallel();
		}
		getLogger().fine(() -> "Output keys:" + outputKeys.size());
		stream.forEach(entry -> {
			try {
				List<String> valueFieldNames = fields.get(entry);
				if (valueFieldNames == null) {
					throw new NullPointerException(
							"NPEfor:" + entry + " rowkeys:" + fields + " vfn:" + valueFieldNamePattern);
				}
				populateDataPointsInResult(valueFieldNames, entry, startTime, endTime, valuePredicate, p, resultMap,
						function);
			} catch (Exception e) {
				getLogger().log(Level.SEVERE, "Failed to query data points: " + entry, e);
			}
		});
	}

	public default void queryReaders(List<String> valueFieldNames, List<Predicate> valuePredicate, boolean regex,
			long startTime, long endTime, TagFilter tagFilter, ConcurrentMap<ByteString, FieldReaderIterator[]> readers)
			throws IOException {
		final Set<ByteString> rowKeys;
		if (tagFilter == null) {
			rowKeys = getSeriesKeys();
		} else {
			rowKeys = getTagFilteredRowKeys(tagFilter);
		}
		getLogger().fine(() -> "Filtered row keys to query(" + valueFieldNames + "," + tagFilter + "):" + rowKeys);
		Pattern p = checkAndBuildPattern(valueFieldNames, regex);
		Set<String> fieldSet = getFields();

		filterFieldsBasedOnRegex(regex, p, fieldSet);

		List<Series> seriesList = new ArrayList<>();
		if (rowKeys != null) {
			for (ByteString key : rowKeys) {
				Series series = getSeriesUsingKey(key);
				getLogger().fine(() -> "Row key queried" + key + " Fields:" + getFields());
				seriesList.add(series);
			}
		}

		Stream<Series> stream = seriesList.stream();
		if (useQueryPool()) {
			stream = stream.parallel();
		}
		getLogger().fine(() -> "Output keys:" + seriesList.size());
		if (regex) {
			List<String> fields = Arrays.asList(fieldSet.toArray(new String[1]));
			stream.forEach(entry -> {
				try {
					entry.queryIterators(this, fields, startTime, endTime);
				} catch (Exception e) {
					getLogger().log(Level.SEVERE, "Failed to query data points: " + entry, e);
				}
			});
		} else {
			stream.forEach(entry -> {
				try {
					readers.put(entry.getSeriesId(), entry.queryIterators(this, valueFieldNames, startTime, endTime));
				} catch (Exception e) {
					getLogger().log(Level.SEVERE, "Failed to query data points:" + entry.getSeriesId(), e);
				}
			});
		}
	}

	default void filterFieldsBasedOnRegex(boolean regex, Pattern p, Set<String> fieldSet) {
		if (regex) {
			for (Iterator<String> iterator = fieldSet.iterator(); iterator.hasNext();) {
				String fieldSetEntry = iterator.next();
				if (!p.matcher(fieldSetEntry).matches()) {
					iterator.remove();
				}
			}
		}
	}

	default Pattern checkAndBuildPattern(List<String> valueFieldNames, boolean regex) throws IOException {
		Pattern p = null;
		if (regex) {
			try {
				StringBuilder patternBuilder = new StringBuilder();
				for (String vfn : valueFieldNames) {
					patternBuilder.append(vfn + "|");
				}
				p = Pattern.compile(patternBuilder.toString());
			} catch (Exception e) {
				throw new IOException("Invalid regex for value field name:" + e.getMessage());
			}
		}
		return p;
	}

	public default Set<String> getFields() {
		return new TreeSet<>(getFieldTypeMap().keySet());
	}

	public default void populateDataPointsInResult(List<String> valueFieldNames, ByteString rowKey, long startTime,
			long endTime, Predicate valuePredicate, Pattern p, List<SeriesOutput> resultMap, Function function)
			throws IOException {
		List<Tag> seriesTags = decodeStringToTags(rowKey);
		Series series = getSeriesUsingKey(rowKey);

		FieldReaderIterator[][] queryTimePairIterators = series.queryTimePairIterators(this, valueFieldNames, null,
				startTime, endTime);
		if (queryTimePairIterators == null) {
			return;
		}
		try {
			for (FieldReaderIterator[] pairIterator : queryTimePairIterators) {
				// ignore entries that are null
				if (pairIterator == null) {
					continue;
				}
				List<DataPoint> dpList = new ArrayList<>();
				// extract all datapoints
				while (true) {
					try {
						long[] extracted = FieldReaderIterator.extracted(pairIterator);
						dpList.add(new DataPoint(extracted[0], extracted[1]));
					} catch (FilteredValueException e) {
						// ignore this
					} catch (IOException e) {
						// terminate read loop
						break;
					}
				}
				// apply function
				String vfn = pairIterator[1].getFieldName();
				getLogger().fine(() -> "Reading datapoints for:" + vfn + " " + dpList);
				SeriesOutput seriesQueryOutput = new SeriesOutput(getMeasurementName(), vfn, seriesTags);
				seriesQueryOutput.setFp(isFieldFp(vfn));
				seriesQueryOutput.setDataPoints(dpList);
				if (function != null) {
					resultMap.addAll(function.apply(Arrays.asList(seriesQueryOutput)));
				} else {
					resultMap.add(seriesQueryOutput);
				}
			}
		} catch (Exception e) {
			getLogger().severe("Failed to populate data points for: " + rowKey.toString() + " " + valueFieldNames + " "
					+ Arrays.toString(queryTimePairIterators[0]));
			throw e;
		}
	}

	// for (Entry<String, List<DataPoint>> entry : queryDataPoints.entrySet()) {
	// getLogger().fine(() -> "Reading datapoints for:" + entry.getKey() + " " +
	// entry.getValue());
	// SeriesOutput seriesQueryOutput = new SeriesOutput(getMeasurementName(),
	// entry.getKey(), seriesTags);
	// seriesQueryOutput.setFp(isFieldFp(entry.getKey()));
	// seriesQueryOutput.setDataPoints(entry.getValue());
	// if (function != null) {
	// resultMap.addAll(function.apply(Arrays.asList(seriesQueryOutput)));
	// } else {
	// resultMap.add(seriesQueryOutput);
	// }
	// }

	// public default void queryReaders(String valueFieldName, long startTime, long
	// endTime,
	// LinkedHashMap<ValueReader, Boolean> readers) throws IOException {
	// for (ByteString entry : getSeriesKeys()) {
	// Series m = getSeriesFromKey(new ByteString(entry));
	// TimeBucket series = m.get(valueFieldName);
	// if (series == null) {
	// continue;
	// }
	// List<Tag> seriesTags = decodeStringToTags(getTagIndex(), entry);
	// for (ValueReader reader : series.queryReader(valueFieldName, seriesTags,
	// startTime, endTime, null)) {
	// readers.put(reader, series.isFp());
	// }
	// }
	// }
	//
	// public default void queryReadersWithMap(String valueFieldName, long
	// startTime, long endTime,
	// LinkedHashMap<ValueReader, List<Tag>> readers) throws IOException {
	// for (ByteString entry : getSeriesKeys()) {
	// Series m = getSeriesFromKey(new ByteString(entry));
	// TimeBucket series = m.get(valueFieldName);
	// if (series == null) {
	// continue;
	// }
	// List<Tag> seriesTags = decodeStringToTags(getTagIndex(), entry);
	// for (ValueReader reader : series.queryReader(valueFieldName, seriesTags,
	// startTime, endTime, null)) {
	// readers.put(reader, seriesTags);
	// }
	// }
	// }

	public default Collection<String> getTagKeys() throws IOException {
		return getTagIndex().getTagKeys();
	}

	// public default Collection<FieldBucket> getTimeSeries() {
	// List<FieldBucket> series = new ArrayList<>();
	// for (Series seriesFieldMap : getSeriesList()) {
	// series.addAll(seriesFieldMap.values());
	// }
	// return series;
	// }

	/**
	 * List all series field maps
	 * 
	 * @return
	 */
	public List<Series> getSeriesList();

	public Logger getLogger();

	public default SortedMap<Integer, List<Writer>> createNewBucketMap(ByteString seriesId) {
		return new ConcurrentSkipListMap<>();
	}

	public ReentrantLock getLock();

	public boolean useQueryPool();

	public String getDbName();

	public Malloc getMalloc();

	public default Collection<String> getTagValues(String tagKey) {
		return getTagIndex().getTagValues(tagKey);
	}

	public default boolean isFieldFp(String valueFieldName) throws ItemNotFoundException {
		Boolean fp = getFieldTypeMap().get(valueFieldName);
		if (fp != null) {
			return fp;
		}
		throw StorageEngine.NOT_FOUND_EXCEPTION;
	}

	public static class TagComparator implements Comparator<Tag> {

		@Override
		public int compare(Tag o1, Tag o2) {
			int r = o1.getTagKey().compareTo(o2.getTagKey());
			if (r != 0) {
				return r;
			} else {
				return o1.getTagValue().compareTo(o2.getTagValue());
			}
		}
	}

	public DBMetadata getMetadata();

	public default void appendTimeseriesToMeasurementMetadata(ByteString fieldId, int seriesIdx) throws IOException {
		// do nothing default implementation
	}

	Map<ByteString, Integer> getSeriesMap();

	boolean isEnableMetricsCapture();

	Counter getMetricsTimeSeriesCounter();

	Counter getMetricsCompactionCounter();

	Counter getMetricsCleanupBufferCounter();

	/**
	 * Update retention hours for this TimeSeries
	 * 
	 * @param retentionHours
	 */
	public default void setRetentionHours(int retentionHours) {
		int val = (int) (((long) retentionHours * 3600) / getTimeBucketSize());
		if (val < 1) {
			getLogger().fine("Incorrect bucket(" + getTimeBucketSize() + ") or retention(" + retentionHours
					+ ") configuration; correcting to 1 bucket for measurement:" + getMeasurementName());
			val = 1;
		}
		getRetentionBuckets().set(val);
	}

	public AtomicInteger getRetentionBuckets();

	public default void appendFieldMetadata(String valueFieldName, boolean fp) throws IOException {
	}

	public SortedMap<String, Boolean> getFieldTypeMap();

	ByteStringCache getFieldCache();

	public default void queryDataPointsv2(String valueFieldNamePattern, long startTime, long endTime,
			TagFilter tagFilter, Predicate valuePredicate, List<SeriesOutputv2> result,
			FunctionIteratorFactory template) throws IOException {
		final Set<ByteString> rowKeys;
		if (tagFilter == null) {
			rowKeys = getSeriesKeys();
		} else {
			rowKeys = getTagFilteredRowKeys(tagFilter);
		}
		getLogger()
				.fine(() -> "Filtered row keys to query(" + valueFieldNamePattern + "," + tagFilter + "):" + rowKeys);
		final Pattern p;
		try {
			p = Pattern.compile(valueFieldNamePattern);
		} catch (Exception e) {
			throw new IOException("Invalid regex for value field name:" + e.getMessage());
		}
		Set<ByteString> outputKeys = new HashSet<>();
		final Map<ByteString, List<String>> fields = new HashMap<>();
		if (rowKeys != null) {
			for (ByteString key : rowKeys) {
				List<String> fieldMap = new ArrayList<>();
				List<String> fieldSet = new ArrayList<>(getFields());
				getLogger().fine(() -> "Row key:" + key + " Fields:" + fieldSet);
				for (String fieldSetEntry : fieldSet) {
					if (p.matcher(fieldSetEntry).matches() && !fieldSetEntry.equalsIgnoreCase(Series.TS)) {
						fieldMap.add(fieldSetEntry);
					}
				}
				if (fieldMap.size() > 0) {
					fields.put(key, fieldMap);
					outputKeys.add(key);
				}
			}
		}

		getLogger().fine(() -> "Output keys:" + outputKeys.size());
		for (ByteString entry : outputKeys) {
			try {
				List<String> valueFieldNames = fields.get(entry);
				if (valueFieldNames == null) {
					throw new NullPointerException(
							"NPEfor:" + entry + " rowkeys:" + fields + " vfn:" + valueFieldNamePattern);
				}
				populateResultsIterators(valueFieldNames, entry, startTime, endTime, valuePredicate, p, result,
						template);
			} catch (Exception e) {
				getLogger().log(Level.SEVERE, "Failed to query data points: " + entry, e);
			}
		}
	}

	public default void populateResultsIterators(List<String> valueFieldNames, ByteString rowKey, long startTime,
			long endTime, Predicate valuePredicate, Pattern p, List<SeriesOutputv2> result,
			FunctionIteratorFactory template) throws Exception {
		List<Tag> seriesTags = decodeStringToTags(rowKey);
		Series series = getSeriesUsingKey(rowKey);

		FieldReaderIterator[][] queryTimePairIterators = series.queryTimePairIterators(this, valueFieldNames, null,
				startTime, endTime);
		if (queryTimePairIterators == null) {
			return;
		}
		try {
			for (FieldReaderIterator[] pairIterator : queryTimePairIterators) {
				// ignore entries that are null
				if (pairIterator == null || pairIterator[1] == null) {
					continue;
				}
				// apply function
				String vfn = pairIterator[1].getFieldName();
				getLogger().fine(() -> "Reading datapoints for:" + vfn + " ");
				SeriesOutputv2 seriesQueryOutput = new SeriesOutputv2(getMeasurementName(), vfn, seriesTags);
				seriesQueryOutput.setFp(isFieldFp(vfn));
				seriesQueryOutput.setIterator(new DataPointIterator(pairIterator[0], pairIterator[1]));
				if (template != null) {
					DataPointIterator functionedIterator = template.build(seriesQueryOutput.getIterator(),
							seriesQueryOutput.isFp());
					seriesQueryOutput.setIterator(functionedIterator);
				}
				result.add(seriesQueryOutput);
			}
		} catch (Exception e) {
			getLogger().severe("Failed to populate data points for: " + rowKey.toString() + " " + valueFieldNames + " "
					+ Arrays.toString(queryTimePairIterators[0]));
			throw e;
		}
	}
}