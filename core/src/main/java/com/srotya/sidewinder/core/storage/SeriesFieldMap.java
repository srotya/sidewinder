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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.rpc.Point;

/**
 * @author ambud
 */
public class SeriesFieldMap {

	private static final Logger logger = Logger.getLogger(SeriesFieldMap.class.getName());
	private Object seriesId;
	private Map<String, TimeSeries> fieldMap;
	private int fieldMapIndex;

	public SeriesFieldMap(Object seriesId, int fieldMapIndex) {
		this.seriesId = seriesId;
		this.fieldMapIndex = fieldMapIndex;
		fieldMap = new ConcurrentHashMap<>();
	}

	public Set<String> getFields() {
		return fieldMap.keySet();
	}

	public TimeSeries getOrCreateSeriesLocked(String valueFieldName, int timeBucketSize, boolean fp, Measurement measurement)
			throws IOException {
		TimeSeries series = get(valueFieldName);
		if (series == null) {
			synchronized (seriesId) {
				if ((series = get(valueFieldName)) == null) {
					ByteString fieldId = new ByteString(seriesId + Measurement.SERIESID_SEPARATOR + valueFieldName);
					series = new TimeSeries(measurement, fieldId, timeBucketSize, measurement.getMetadata(), fp,
							measurement.getConf());
					fieldMap.put(valueFieldName, series);
					measurement.appendTimeseriesToMeasurementMetadata(fieldId, fp, timeBucketSize, fieldMapIndex);
					final TimeSeries tmp = series;
					logger.fine(() -> "Created new timeseries:" + tmp + " for measurement:"
							+ measurement.getMeasurementName() + "\t" + seriesId + "\t"
							+ measurement.getMetadata().getRetentionHours() + "\t"
							+ measurement.getSeriesList().size());
				} else {
					// in case there was contention and we have to re-check the cache
					series = get(valueFieldName);
				}
			}
		}
		return series;
	}

	public void addDataPointLocked(String valueFieldName, int timeBucketSize, boolean fp, TimeUnit unit, long timestamp,
			long value, Measurement m) throws IOException {
		TimeSeries ts = getOrCreateSeriesLocked(valueFieldName, timeBucketSize, fp, m);
		if (ts.isFp() != fp) {
			throw StorageEngine.FP_MISMATCH_EXCEPTION;
		}
		ts.addDataPointLocked(TimeUnit.MILLISECONDS, timestamp, value);
	}

	public void addPointUnlocked(Point dp, int timeBucketSize, Measurement m) throws IOException {
		for (int i = 0; i < dp.getFpList().size(); i++) {
			TimeSeries ts = getOrCreateSeriesLocked(dp.getValueFieldNameList().get(i), timeBucketSize, dp.getFpList().get(i),
					m);
			ts.addDataPointUnlocked(TimeUnit.MILLISECONDS, dp.getTimestamp(), dp.getValueList().get(i));
		}
	}

	public synchronized void addPointLocked(Point dp, int timeBucketSize, Measurement m) throws IOException {
		for (int i = 0; i < dp.getFpList().size(); i++) {
			TimeSeries ts = getOrCreateSeriesLocked(dp.getValueFieldNameList().get(i), timeBucketSize, dp.getFpList().get(i),
					m);
			ts.addDataPointUnlocked(TimeUnit.MILLISECONDS, dp.getTimestamp(), dp.getValueList().get(i));
		}
	}

	public TimeSeries get(String valueFieldName) {
		return fieldMap.get(valueFieldName);
	}

	/**
	 * Must close before modifying this via iterator
	 * 
	 * @return
	 */
	public Collection<? extends TimeSeries> values() {
		return fieldMap.values();
	}

	/**
	 * @return the seriesId
	 */
	public Object getSeriesId() {
		return seriesId;
	}

	/**
	 * @param seriesId
	 *            the seriesId to set
	 */
	public void setSeriesId(String seriesId) {
		this.seriesId = seriesId;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SeriesFieldMap [seriesId=" + seriesId + " seriesMap=" + fieldMap + "]";
	}

}
