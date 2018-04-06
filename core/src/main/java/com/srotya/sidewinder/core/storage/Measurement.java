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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.codahale.metrics.Counter;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.archival.TimeSeriesArchivalObject;
import com.srotya.sidewinder.core.storage.compression.CompressionFactory;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;

/**
 * @author ambud
 */
public interface Measurement {

	public static final RejectException INDEX_REJECT = new RejectException("Invalid tag, rejecting index");
	public static final RejectException SEARCH_REJECT = new RejectException("Invalid tag, rejecting index search");
	public static final String TAG_KV_SEPARATOR = "=";
	public static final String SERIESID_SEPARATOR = "#";
	public static final String USE_QUERY_POOL = "use.query.pool";
	public static final String TAG_SEPARATOR = "^";
	public static final TagComparator TAG_COMPARATOR = new TagComparator();
	public static final Exception NOT_FOUND_EXCEPTION = null;

	public void configure(Map<String, String> conf, StorageEngine engine, int defaultTimeBucketSize, String dbName, String measurementName,
			String baseIndexDirectory, String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool)
			throws IOException;

	public Set<ByteString> getSeriesKeys();

	public default SeriesFieldMap getSeriesFromKey(ByteString key) {
		Integer index = getSeriesMap().get(key);
		if (index == null) {
			return null;
		} else {
			return getSeriesList().get(index);
		}
	}

	public TagIndex getTagIndex();

	public void loadTimeseriesFromMeasurements() throws IOException;

	public void close() throws IOException;

	public default SeriesFieldMap getOrCreateSeriesFieldMap(List<Tag> tags, boolean preSorted) throws IOException {
		if (!preSorted) {
			Collections.sort(tags, TAG_COMPARATOR);
		}
		ByteString seriesId = constructSeriesId(tags, getTagIndex());
		int index = 0;
		SeriesFieldMap seriesFieldMap = getSeriesFromKey(seriesId);
		if (seriesFieldMap == null) {
			getLock().lock();
			try {
				if ((seriesFieldMap = getSeriesFromKey(seriesId)) == null) {
					index = getSeriesList().size();
					Measurement.indexRowKey(getTagIndex(), index, tags);
					seriesFieldMap = new SeriesFieldMap(seriesId, index);
					getSeriesList().add(seriesFieldMap);
					getSeriesMap().put(seriesId, index);
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

		return seriesFieldMap;
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

	public default void setCodecsForTimeseries(Map<String, String> conf) {
		String compressionCodec = conf.getOrDefault(StorageEngine.COMPRESSION_CODEC,
				StorageEngine.DEFAULT_COMPRESSION_CODEC);
		String compactionCodec = conf.getOrDefault(StorageEngine.COMPACTION_CODEC,
				StorageEngine.DEFAULT_COMPACTION_CODEC);
		TimeSeries.compactionClass = CompressionFactory.getClassByName(compactionCodec);
		TimeSeries.compressionClass = CompressionFactory.getClassByName(compressionCodec);
	}

	public default ByteString encodeTagsToString(TagIndex tagIndex, List<Tag> tags) throws IOException {
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

	public static void serializeTagForKey(StringBuilder builder, Tag tag) {
		builder.append(tag.getTagKey());
		builder.append(TAG_KV_SEPARATOR);
		builder.append(tag.getTagValue());
	}

	public default ByteString constructSeriesId(List<Tag> tags, TagIndex index) throws IOException {
		return encodeTagsToString(index, tags);
	}

	public static List<Tag> decodeStringToTags(TagIndex tagIndex, ByteString tagString) throws IOException {
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
			List<Tag> tags = decodeStringToTags(getTagIndex(), entry);
			tagList.add(tags);
		}
		return tagList;
	}

	public default Set<ByteString> getTagFilteredRowKeys(TagFilter tagFilterTree) throws IOException {
		return getTagIndex().searchRowKeysForTagFilter(tagFilterTree);
	}

	public default void addDataPoint(String valueFieldName, List<Tag> tags, long timestamp, long value, boolean fp, boolean presorted)
			throws IOException {
		SeriesFieldMap fieldMap = getOrCreateSeriesFieldMap(tags, presorted);
		fieldMap.addDataPointLocked(valueFieldName, getTimeBucketSize(), fp, TimeUnit.MILLISECONDS, timestamp, value, this);
	}
	
	public default void addDataPoint(String valueFieldName, List<Tag> tags, long timestamp, long value, boolean presorted)
			throws IOException {
		addDataPoint(valueFieldName, tags, timestamp, value, false, presorted);
	}
	
	public default void addDataPoint(String valueFieldName, List<Tag> tags, long timestamp, double value, boolean fp, boolean presorted)
			throws IOException {
		addDataPoint(valueFieldName, tags, timestamp, Double.doubleToLongBits(value), true, presorted);
	}

	public default void addPointLocked(Point dp, boolean preSorted) throws IOException {
		SeriesFieldMap fieldMap = getOrCreateSeriesFieldMap(new ArrayList<>(dp.getTagsList()), preSorted);
		fieldMap.addPointLocked(dp, getTimeBucketSize(), this);
	}
	
	public default void addPointUnlocked(Point dp, boolean preSorted) throws IOException {
		SeriesFieldMap fieldMap = getOrCreateSeriesFieldMap(new ArrayList<>(dp.getTagsList()), preSorted);
		fieldMap.addPointUnlocked(dp, getTimeBucketSize(), this);
	}

	public int getTimeBucketSize();

	public default void collectGarbage(Archiver archiver) throws IOException {
		runCleanupOperation("garbage collection", ts -> {
			try {
				Map<Integer, List<Writer>> collectedGarbage = ts.collectGarbage();
				List<Writer> output = new ArrayList<>();
				getLogger().fine("Collected garbage:" + collectedGarbage.size());
				if (archiver != null && collectedGarbage != null) {
					for (Entry<Integer, List<Writer>> entry : collectedGarbage.entrySet()) {
						for (Writer writer : entry.getValue()) {
							byte[] buf = Archiver.writerToByteArray(writer);
							TimeSeriesArchivalObject archivalObject = new TimeSeriesArchivalObject(getDbName(),
									getMeasurementName(), ts.getFieldId(), entry.getKey(), buf);
							try {
								archiver.archive(archivalObject);
							} catch (ArchiveException e) {
								getLogger().log(Level.SEVERE, "Series failed to archive, series:" + ts.getFieldId()
										+ " db:" + getDbName() + " m:" + getMeasurementName(), e);
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

	public default Set<String> compact() throws IOException {
		return runCleanupOperation("compacting", ts -> {
			try {
				return ts.compact();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public default Set<String> runCleanupOperation(String operation,
			java.util.function.Function<TimeSeries, List<Writer>> op) throws IOException {
		Set<String> cleanupList = new HashSet<>();
		getLock().lock();
		try {
			for (TimeSeries entry : getTimeSeries()) {
				try {
					List<Writer> list = op.apply(entry);
					if (list == null) {
						continue;
					}
					for (Writer timeSeriesBucket : list) {
						cleanupList.add(timeSeriesBucket.getBufferId().toString());
						getLogger().fine("Adding buffer to cleanup " + operation + " for bucket:" + entry.getFieldId()
								+ " Offset:" + timeSeriesBucket.currentOffset());
					}
					getLogger().fine("Buffers " + operation + " for time series:" + entry.getFieldId());
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

	public default SeriesFieldMap getSeriesField(List<Tag> tags) throws IOException {
		Collections.sort(tags, TAG_COMPARATOR);
		ByteString rowKey = constructSeriesId(tags, getTagIndex());
		// check and create timeseries
		SeriesFieldMap map = getSeriesFromKey(rowKey);
		return map;
	}

	public default Set<String> getFieldsForMeasurement() {
		Set<String> results = new HashSet<>();
		Set<ByteString> keySet = getSeriesKeys();
		for (ByteString key : keySet) {
			SeriesFieldMap map = getSeriesFromKey(key);
			results.addAll(map.getFields());
		}
		return results;
	}

	// public default Set<String> getSeriesIdsWhereTags(List<String> tags) throws
	// IOException {
	// Set<String> series = new HashSet<>();
	// if (tags != null) {
	// for (String tag : tags) {
	// String[] split = tag.split(TAG_KV_SEPARATOR);
	// if (split.length != 2) {
	// throw SEARCH_REJECT;
	// }
	// String tagKey = split[0];
	// String tagValue = split[1];
	// Collection<String> keys = getTagIndex().searchRowKeysForTag(tagKey,
	// tagValue);
	// if (keys != null) {
	// series.addAll(keys);
	// }
	// }
	// } else {
	// series.addAll(getSeriesKeys());
	// }
	// return series;
	// }

	public default void queryDataPoints(String valueFieldNamePattern, long startTime, long endTime, TagFilter tagFilter,
			Predicate valuePredicate, List<Series> resultMap) throws IOException {
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
				Set<String> fieldSet = getSeriesFromKey(key).getFields();
				for (String fieldSetEntry : fieldSet) {
					if (p.matcher(fieldSetEntry).matches()) {
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
		stream.forEach(entry -> {
			try {
				List<String> valueFieldNames = fields.get(entry);
				if (valueFieldNames == null) {
					throw new NullPointerException(
							"NPEfor:" + entry + " rowkeys:" + fields + " vfn:" + valueFieldNamePattern);
				}
				populateDataPoints(valueFieldNames, entry, startTime, endTime, valuePredicate, p, resultMap);
			} catch (Exception e) {
				getLogger().log(Level.SEVERE, "Failed to query data points", e);
			}
		});
	}

	public default void populateDataPoints(List<String> valueFieldNames, ByteString rowKey, long startTime,
			long endTime, Predicate valuePredicate, Pattern p, List<Series> resultMap) throws IOException {
		List<DataPoint> points = null;
		List<Tag> seriesTags = decodeStringToTags(getTagIndex(), rowKey);
		for (String valueFieldName : valueFieldNames) {
			TimeSeries value = getSeriesFromKey(rowKey).get(valueFieldName);
			if (value == null) {
				getLogger().severe("Invalid time series value " + rowKey + "\t" + "\t" + "\n\n");
				return;
			}
			points = value.queryDataPoints(valueFieldName, startTime, endTime, valuePredicate);
			if (points != null && points.size() > 0) {
				Series seriesQueryOutput = new Series(getMeasurementName(), valueFieldName, seriesTags);
				seriesQueryOutput.setFp(value.isFp());
				seriesQueryOutput.setDataPoints(points);
				resultMap.add(seriesQueryOutput);
			}
		}
	}

	public default void queryReaders(String valueFieldName, long startTime, long endTime,
			LinkedHashMap<Reader, Boolean> readers) throws IOException {
		for (ByteString entry : getSeriesKeys()) {
			SeriesFieldMap m = getSeriesFromKey(new ByteString(entry));
			TimeSeries series = m.get(valueFieldName);
			if (series == null) {
				continue;
			}
			List<Tag> seriesTags = decodeStringToTags(getTagIndex(), entry);
			for (Reader reader : series.queryReader(valueFieldName, seriesTags, startTime, endTime, null)) {
				readers.put(reader, series.isFp());
			}
		}
	}

	public default void queryReadersWithMap(String valueFieldName, long startTime, long endTime,
			LinkedHashMap<Reader, List<Tag>> readers) throws IOException {
		for (ByteString entry : getSeriesKeys()) {
			SeriesFieldMap m = getSeriesFromKey(new ByteString(entry));
			TimeSeries series = m.get(valueFieldName);
			if (series == null) {
				continue;
			}
			List<Tag> seriesTags = decodeStringToTags(getTagIndex(), entry);
			for (Reader reader : series.queryReader(valueFieldName, seriesTags, startTime, endTime, null)) {
				readers.put(reader, seriesTags);
			}
		}
	}

	public default Collection<String> getTagKeys() throws IOException {
		return getTagIndex().getTagKeys();
	}

	public default Collection<TimeSeries> getTimeSeries() {
		List<TimeSeries> series = new ArrayList<>();
		for (SeriesFieldMap seriesFieldMap : getSeriesList()) {
			series.addAll(seriesFieldMap.values());
		}
		return series;
	}

	/**
	 * List all series field maps
	 * 
	 * @return
	 */
	public List<SeriesFieldMap> getSeriesList();

	public Logger getLogger();

	public SortedMap<Integer, List<Writer>> createNewBucketMap(ByteString seriesId);

	public ReentrantLock getLock();

	public boolean useQueryPool();

	public String getDbName();

	public Malloc getMalloc();

	public default Collection<String> getTagValues(String tagKey) {
		return getTagIndex().getTagValues(tagKey);
	}

	public default boolean isFieldFp(String valueFieldName) throws ItemNotFoundException {
		for (ByteString entry : getSeriesKeys()) {
			SeriesFieldMap seriesFromKey = getSeriesFromKey(entry);
			if (seriesFromKey.get(valueFieldName) != null) {
				return seriesFromKey.get(valueFieldName).isFp();
			}
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

	public default void appendTimeseriesToMeasurementMetadata(ByteString fieldId, boolean fp, int timeBucketSize,
			int idx) throws IOException {
		// do nothing default implementation
	}

	public Map<String, String> getConf();

	Map<ByteString, Integer> getSeriesMap();

	boolean isEnableMetricsCapture();

	Counter getMetricsTimeSeriesCounter();

}
