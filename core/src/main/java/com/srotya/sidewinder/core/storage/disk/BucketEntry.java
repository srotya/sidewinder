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
package com.srotya.sidewinder.core.storage.disk;

import java.io.IOException;

import com.srotya.sidewinder.core.storage.TimeSeriesBucket;
import com.srotya.sidewinder.core.storage.disk.BucketEntry;

/**
 * @author ambud
 */
public class BucketEntry {

	private BucketEntry next, prev;
	private String key;
	private TimeSeriesBucket value;

	public BucketEntry(String key, TimeSeriesBucket value) {
		this.key = key;
		this.value = value;
	}

	/**
	 * @return the next
	 */
	public BucketEntry getNext() {
		return next;
	}

	/**
	 * @param next
	 *            the next to set
	 */
	public void setNext(BucketEntry next) {
		this.next = next;
	}

	/**
	 * @return the prev
	 */
	public BucketEntry getPrev() {
		return prev;
	}

	/**
	 * @param prev
	 *            the prev to set
	 */
	public void setPrev(BucketEntry prev) {
		this.prev = prev;
	}

	/**
	 * @return the value
	 */
	public TimeSeriesBucket getValue() {
		return value;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(TimeSeriesBucket value) {
		this.value = value;
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

	@Override
	protected void finalize() throws Throwable {
		value.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "BucketEntry [next=" + next + ", prev=" + prev + ", key=" + key + ", value=" + value + "]";
	}

	public void close() {
		if (value != null) {
			try {
				value.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public void delete() {
		if (value != null) {
			try {
				value.delete();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

}