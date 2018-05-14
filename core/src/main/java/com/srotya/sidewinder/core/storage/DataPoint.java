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

import java.io.Serializable;
import java.util.Date;

/**
 * Object representation of a {@link DataPoint}. This class services DAO and DTO
 * needs inside Sidewinder.
 * 
 * @author ambud
 */
public class DataPoint implements Serializable {

	private static final long serialVersionUID = 1L;
	private long timestamp;
	private long value;

	public DataPoint() {
	}

	public DataPoint(long timestamp, long value) {
		this.timestamp = timestamp;
		this.value = value;
	}

	public DataPoint(long timestamp, double value) {
		this.timestamp = timestamp;
		setValue(value);
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

	@Override
	public String toString() {
		return "[ts:" + new Date(timestamp) + " v:" + value + "]";
	}

}
