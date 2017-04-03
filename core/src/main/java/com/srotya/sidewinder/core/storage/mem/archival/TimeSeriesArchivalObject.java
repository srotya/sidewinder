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
package com.srotya.sidewinder.core.storage.mem.archival;

import com.srotya.sidewinder.core.storage.TimeSeriesBucket;

/**
 * @author ambud
 */
public class TimeSeriesArchivalObject {

	private String db;
	private String measurement;
	private String key;
	private TimeSeriesBucket bucket;
	
	public TimeSeriesArchivalObject() {
	}

	public TimeSeriesArchivalObject(String db, String measurement, String key, TimeSeriesBucket bucket) {
		this.db = db;
		this.measurement = measurement;
		this.key = key;
		this.bucket = bucket;
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
	 * @return the key
	 */
	public String getKey() {
		return key;
	}

	/**
	 * @param key
	 *            the key to set
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * @return the bucket
	 */
	public TimeSeriesBucket getBucket() {
		return bucket;
	}

	/**
	 * @param bucket
	 *            the bucket to set
	 */
	public void setBucket(TimeSeriesBucket bucket) {
		this.bucket = bucket;
	}

}