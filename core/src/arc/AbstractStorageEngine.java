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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.srotya.sidewinder.core.utils.ByteUtils;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * @author ambudsharma
 *
 */
public abstract class AbstractStorageEngine implements StorageEngine, Runnable {

	public static final int BUCKET_SIZE = 4096;
	private ArrayBlockingQueue<WriteTask> taskQueue = new ArrayBlockingQueue<>(1024 * 4);
	private AtomicBoolean control = new AtomicBoolean(true);
	
	@Override
	public void run() {
		while (control.get() || taskQueue.size() > 0) {
			WriteTask poll;
			try {
				poll = taskQueue.take();
				writeSeriesPoint(poll);
				poll.getCallback().complete();
			} catch (InterruptedException e) {
				break;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			flush();
		} catch (IOException e) {
		}
	}

	public abstract byte[] indexIdentifier(String identifier) throws IOException;

	public byte[] buildRowKey(String seriesName, List<String> tags, TimeUnit unit, long timestamp) throws IOException {
		/**
		 * 3 bytes for each tag, 3 bytes for series name and 4 bytes for the
		 * time bucket
		 */
		byte[] rowKey = new byte[3 * (tags.size() + 1) + 4];
		int bucket = TimeUtils.getTimeBucket(unit, timestamp, BUCKET_SIZE);
		byte[] bucketBytes = ByteUtils.intToByteMSB(bucket);
		byte[] seriesBytes = indexIdentifier(seriesName);
		System.arraycopy(seriesBytes, 0, rowKey, 0, seriesBytes.length);
		int pos = seriesBytes.length;
		Collections.sort(tags);
		for (String tag : tags) {
			byte[] tagBytes = indexIdentifier(tag);
			System.arraycopy(tagBytes, 0, rowKey, pos, tagBytes.length);
			pos += tagBytes.length;
		}
		System.arraycopy(bucketBytes, 0, rowKey, pos, bucketBytes.length);
		return rowKey;
	}

	@Override
	public void writeSeries(String dbName, String seriesName, List<String> tags, TimeUnit unit, long timestamp, long value,
			Callback callback) throws IOException {
		byte[] valueBytes = ByteUtils.longToBytes(value);
		writeSeries(dbName, seriesName, tags, unit, timestamp, valueBytes, callback);
	}

	public void writeSeries(String dbName, String seriesName, List<String> tags, TimeUnit unit, long timestamp, byte[] value,
			Callback callback) throws IOException {
		byte[] rowKey = buildRowKey(seriesName, tags, unit, timestamp);
		try {
			taskQueue.put(new WriteTask(("db_"+dbName+"series_" + seriesName).getBytes(), rowKey, timestamp, value, callback));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void writeSeries(String dbName, String seriesName, List<String> tags, TimeUnit unit, long timestamp, double value,
			Callback callback) throws IOException {
		byte[] valueBytes = ByteUtils.doubleToBytes(value);
		writeSeries(dbName, seriesName, tags, unit, timestamp, valueBytes, callback);
	}

	public abstract void writeSeriesPoint(WriteTask point) throws IOException;

	public abstract TreeMap<Long, byte[]> getTreeFromDS(byte[] rowKey) throws Exception;

	public abstract void flush() throws IOException;
	
	public abstract void print() throws Exception;
	
	/**
	 * 
	 */
	public void stop() {
		control.set(false);
	}
}
