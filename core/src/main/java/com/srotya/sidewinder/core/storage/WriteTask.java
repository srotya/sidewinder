/**
 * Copyright 2016 Ambud Sharma
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

/**
 * @author ambudsharma
 *
 */
public class WriteTask {

	private byte[] seriesName;
	private byte[] rowKey;
	private long timestamp;
	private byte[] value;
	private Callback callback;
	
	public WriteTask() {
	}

	public WriteTask(byte[] seriesName, byte[] rowKey, long timestamp, byte[] value, Callback callback) {
		this.seriesName = seriesName;
		this.rowKey = rowKey;
		this.timestamp = timestamp;
		this.value = value;
		this.callback = callback;
	}

	/**
	 * @return the rowKey
	 */
	public byte[] getRowKey() {
		return rowKey;
	}

	/**
	 * @param rowKey the rowKey to set
	 */
	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @return the value
	 */
	public byte[] getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(byte[] value) {
		this.value = value;
	}

	/**
	 * @return the callback
	 */
	public Callback getCallback() {
		return callback;
	}

	/**
	 * @return
	 */
	public byte[] getSeriesName() {
		return seriesName;
	}

	/**
	 * @param seriesName
	 */
	public void setSeriesName(byte[] seriesName) {
		this.seriesName = seriesName;
	}
	
}
