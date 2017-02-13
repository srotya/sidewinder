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

import java.io.Serializable;
import java.util.List;

/**
 * Object representation of a {@link DataPoint}. This class services DAO and DTO
 * needs inside Sidewinder.
 * 
 * @author ambud
 */
public class DataPoint implements Serializable {

	private static final long serialVersionUID = 1L;
	private boolean isFp;
	private String dbName;
	private String measurementName;
	private String valueFieldName;
	private List<String> tags;
	private long timestamp;
	private long value;

	public DataPoint(long timestamp, long value) {
		this.timestamp = timestamp;
		this.value = value;
		isFp = false;
	}
	
	public DataPoint(long timestamp, double value) {
		this.timestamp = timestamp;
		this.value = Double.doubleToLongBits(value);
		isFp = true;
	}

	public DataPoint(String dbName, String measurementName, String valueFieldName, List<String> tags, long timestamp,
			long value) {
		this.dbName = dbName;
		this.measurementName = measurementName;
		this.valueFieldName = valueFieldName;
		this.tags = tags;
		this.timestamp = timestamp;
		this.value = value;
		this.isFp = false;
	}

	public DataPoint(String dbName, String seriesName, String valueFieldName, List<String> tags, long timestamp,
			double value) {
		this.dbName = dbName;
		this.measurementName = seriesName;
		this.valueFieldName = valueFieldName;
		this.tags = tags;
		this.timestamp = timestamp;
		this.value = Double.doubleToLongBits(value);
		this.isFp = true;
	}

	/**
	 * @return the seriesName
	 */
	public String getMeasurementName() {
		return measurementName;
	}

	/**
	 * @param measurementName
	 *            the measurementName to set
	 */
	public void setMeasurementName(String measurementName) {
		this.measurementName = measurementName;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp
	 *            the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @return
	 */
	public long getLongValue() {
		return value;
	}

	/**
	 * @return the value
	 */
	public double getValue() {
		return Double.longBitsToDouble(value);
	}

	public void setValue(double value) {
		this.value = Double.doubleToLongBits(value);
	}
	
	/**
	 * @param value
	 *            the value to set
	 */
	public void setLongValue(long value) {
		this.value = value;
	}

	/**
	 * @return the isFp
	 */
	public boolean isFp() {
		return isFp;
	}

	/**
	 * @param isFp
	 *            the isFp to set
	 */
	public void setFp(boolean isFp) {
		this.isFp = isFp;
	}

	/**
	 * @return the tags
	 */
	public List<String> getTags() {
		return tags;
	}

	/**
	 * @param tags
	 *            the tags to set
	 */
	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	/**
	 * @return the dbName
	 */
	public String getDbName() {
		return dbName;
	}

	/**
	 * @param dbName
	 *            the dbName to set
	 */
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	/**
	 * @return the valueFieldName
	 */
	public String getValueFieldName() {
		return valueFieldName;
	}

	/**
	 * @param valueFieldName
	 *            the valueFieldName to set
	 */
	public void setValueFieldName(String valueFieldName) {
		this.valueFieldName = valueFieldName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (isFp) {
			return "DataPoint [measurementName=" + measurementName + ", timestamp=" + timestamp + ", value="
					+ getValue() + "]";
		} else {
			return "DataPoint [measurementName=" + measurementName + ", timestamp=" + timestamp + ", value="
					+ getLongValue() + "]";
		}
	}

}
