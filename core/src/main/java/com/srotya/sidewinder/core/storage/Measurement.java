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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.archival.TimeSeriesArchivalObject;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;

/**
 * @author ambud
 */
public interface Measurement {

	public static final String SERIESID_SEPARATOR = "#";
	public static final String USE_QUERY_POOL = "use.query.pool";
	public static final String TAG_SEPARATOR = "^";

	public void configure(Map<String, String> conf, StorageEngine engine, String dbName, String measurementName,
			String baseIndexDirectory, String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool)
			throws IOException;

	public Set<String> getSeriesKeys();

	public SeriesFieldMap getSeriesFromKey(String key);

	public TagIndex getTagIndex();

	public void loadTimeseriesFromMeasurements() throws IOException;

	public void close() throws IOException;

	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException;

	public static void indexRowKey(TagIndex tagIndex, String rowKey, List<String> tags) throws IOException {
		for (String tag : tags) {
			tagIndex.index(tag, rowKey);
		}
	}

	public default String encodeTagsToString(TagIndex tagIndex, List<String> tags) throws IOException {
		StringBuilder builder = new StringBuilder(tags.size() * 5);
		builder.append(tagIndex.mapTag(tags.get(0)));
		for (int i = 1; i < tags.size(); i++) {
			String tag = tags.get(i);
			builder.append(TAG_SEPARATOR);
			builder.append(tagIndex.mapTag(tag));
		}
		return builder.toString();
	}

	public default String constructSeriesId(List<String> tags, TagIndex index) throws IOException {
		return encodeTagsToString(index, tags);
	}

	public static List<String> decodeStringToTags(TagIndex tagLookupTable, String tagString) throws IOException {
		List<String> tagList = new ArrayList<>();
		if (tagString == null || tagString.isEmpty()) {
			return tagList;
		}
		for (String tag : tagString.split("\\" + TAG_SEPARATOR)) {
			tagList.add(tagLookupTable.getTagMapping(tag));
		}
		return tagList;
	}

	public String getMeasurementName();

	public default List<List<String>> getTagsForMeasurement() throws Exception {
		Set<String> keySet = getSeriesKeys();
		List<List<String>> tagList = new ArrayList<>();
		for (String entry : keySet) {
			List<String> tags = decodeStringToTags(getTagIndex(), entry);
			tagList.add(tags);
		}
		return tagList;
	}

	public default Set<String> getTagFilteredRowKeys(Filter<List<String>> tagFilterTree, List<String> rawTags)
			throws IOException {
		Set<String> filteredSeries = getSeriesIdsWhereTags(rawTags);
		for (Iterator<String> iterator = filteredSeries.iterator(); iterator.hasNext();) {
			String rowKey = iterator.next();
			List<String> seriesTags = decodeStringToTags(getTagIndex(), rowKey);
			if (tagFilterTree != null && !tagFilterTree.isRetain(seriesTags)) {
				iterator.remove();
			}
		}
		return filteredSeries;
	}

	public default void collectGarbage(Archiver archiver) throws IOException {
		runCleanupOperation("garbage collection", ts -> {
			try {
				List<Writer> collectedGarbage = ts.collectGarbage();
				if (archiver != null && collectedGarbage != null) {
					for (Writer writer : collectedGarbage) {
						byte[] buf = Archiver.writerToByteArray(writer);
						TimeSeriesArchivalObject archivalObject = new TimeSeriesArchivalObject(getDbName(),
								getMeasurementName(), ts.getSeriesId(), writer.getTsBucket(), buf);
						try {
							archiver.archive(archivalObject);
						} catch (ArchiveException e) {
							getLogger().log(Level.SEVERE, "Series failed to archive, series:" + ts.getSeriesId()
									+ " db:" + getDbName() + " m:" + getMeasurementName(), e);
						}
					}
				}
				return collectedGarbage;
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
						cleanupList.add(timeSeriesBucket.getBufferId());
						getLogger().fine("Adding buffer to cleanup " + operation + " for bucket:" + entry.getSeriesId()
								+ " Offset:" + timeSeriesBucket.currentOffset());
					}
					getLogger().fine("Buffers " + operation + " for time series:" + entry.getSeriesId());
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

	public default SeriesFieldMap getSeriesField(List<String> tags) throws IOException {
		Collections.sort(tags);
		String rowKey = constructSeriesId(tags, getTagIndex());
		// check and create timeseries
		SeriesFieldMap map = getSeriesFromKey(rowKey);
		return map;
	}

	public default Set<String> getFieldsForMeasurement() {
		Set<String> results = new HashSet<>();
		Set<String> keySet = getSeriesKeys();
		for (String key : keySet) {
			SeriesFieldMap map = getSeriesFromKey(key);
			results.addAll(map.getFields());
		}
		return results;
	}

	public default Set<String> getSeriesIdsWhereTags(List<String> tags) throws IOException {
		Set<String> series = new HashSet<>();
		if (tags != null) {
			for (String tag : tags) {
				Collection<String> keys = getTagIndex().searchRowKeysForTag(tag);
				if (keys != null) {
					series.addAll(keys);
				}
			}
		} else {
			series.addAll(getSeriesKeys());
		}
		return series;
	}

	public default void queryDataPoints(String valueFieldNamePattern, long startTime, long endTime,
			List<String> tagList, Filter<List<String>> tagFilter, Predicate valuePredicate, List<Series> resultMap)
			throws IOException {
		final Set<String> rowKeys;
		if (tagList == null || tagList.isEmpty()) {
			rowKeys = getSeriesKeys();
		} else {
			rowKeys = getTagFilteredRowKeys(tagFilter, tagList);
		}
		final Pattern p;
		try {
			p = Pattern.compile(valueFieldNamePattern);
		} catch (Exception e) {
			throw new IOException("Invalid regex for value field name:" + e.getMessage());
		}
		Set<String> outputKeys = new HashSet<>();
		final Map<String, List<String>> fields = new HashMap<>();
		for (String key : rowKeys) {
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
		Stream<String> stream = outputKeys.stream();
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

	public default void populateDataPoints(List<String> valueFieldNames, String rowKey, long startTime, long endTime,
			Predicate valuePredicate, Pattern p, List<Series> resultMap) throws IOException {
		List<DataPoint> points = null;
		List<String> seriesTags = decodeStringToTags(getTagIndex(), rowKey);
		for (String valueFieldName : valueFieldNames) {
			TimeSeries value = getSeriesFromKey(rowKey).get(valueFieldName);
			if (value == null) {
				getLogger().severe("Invalid time series value " + rowKey + "\t" + "\t" + "\n\n");
				return;
			}
			points = value.queryDataPoints(valueFieldName, seriesTags, startTime, endTime, valuePredicate);
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
		for (String entry : getSeriesKeys()) {
			SeriesFieldMap m = getSeriesFromKey(entry);
			TimeSeries series = m.get(valueFieldName);
			if (series == null) {
				continue;
			}
			List<String> seriesTags = decodeStringToTags(getTagIndex(), entry);
			for (Reader reader : series.queryReader(valueFieldName, seriesTags, startTime, endTime, null)) {
				readers.put(reader, series.isFp());
			}
		}
	}

	public default Collection<String> getTags() throws IOException {
		return getTagIndex().getTags();
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
	public Collection<SeriesFieldMap> getSeriesList();

	public Logger getLogger();

	public SortedMap<String, List<Writer>> createNewBucketMap(String seriesId);

	public ReentrantLock getLock();

	public boolean useQueryPool();

	public String getDbName();

	public Malloc getMalloc();

}
