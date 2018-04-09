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
package com.srotya.sidewinder.core.storage.archival;

import com.srotya.sidewinder.core.storage.ByteString;

/**
 * @author ambud
 */
public class TimeSeriesArchivalObject {

	private String db;
	private String measurement;
	private ByteString seriesKey;
	private Integer tsBucket;
	private byte[] data;
	
	public TimeSeriesArchivalObject() {
	}

	public TimeSeriesArchivalObject(String db, String measurement, ByteString seriesKey, Integer tsBucket, byte[] data) {
		this.db = db;
		this.measurement = measurement;
		this.seriesKey = seriesKey;
		this.tsBucket = tsBucket;
		this.data = data;
	}

	/**
	 * @return the db
	 */
	public String getDb() {
		return db;
	}

	/**
	 * @param db
	 *            the db to set
	 */
	public void setDb(String db) {
		this.db = db;
	}

	/**
	 * @return the measurement
	 */
	public String getMeasurement() {
		return measurement;
	}

	/**
	 * @param measurement
	 *            the measurement to set
	 */
	public void setMeasurement(String measurement) {
		this.measurement = measurement;
	}

	/**
	 * @return the seriesKey
	 */
	public ByteString getSeriesKey() {
		return seriesKey;
	}

	/**
	 * @param seriesKey the seriesKey to set
	 */
	public void setSeriesKey(ByteString seriesKey) {
		this.seriesKey = seriesKey;
	}

	/**
	 * @return the data
	 */
	public byte[] getData() {
		return data;
	}

	/**
	 * @param data the data to set
	 */
	public void setData(byte[] data) {
		this.data = data;
	}

	/**
	 * @return the tsBucket
	 */
	public Integer getTsBucket() {
		return tsBucket;
	}

	/**
	 * @param tsBucket the tsBucket to set
	 */
	public void setTsBucket(Integer tsBucket) {
		this.tsBucket = tsBucket;
	}

}