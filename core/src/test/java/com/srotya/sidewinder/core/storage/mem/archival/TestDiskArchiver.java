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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.srotya.sidewinder.core.compression.gorilla.GorillaWriter;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.mem.ArchiveException;
import com.srotya.sidewinder.core.storage.mem.Archiver;
import com.srotya.sidewinder.core.storage.mem.Reader;
import com.srotya.sidewinder.core.storage.mem.TimeSeriesBucket;

/**
 * @author ambud
 */
public class TestDiskArchiver {
	
	private String className = GorillaWriter.class.getName();

//	@Test
	public void testStreamSerDe() throws IOException {
		long ts = System.currentTimeMillis();
		TimeSeriesBucket bucket = new TimeSeriesBucket(className, ts);
		for (int i = 0; i < 1000; i++) {
			bucket.addDataPoint(ts + i * 1000, i);
		}
		Reader reader = bucket.getReader(null, null);
		for (int i = 0; i < 1000; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 1000, pair.getTimestamp());
		}
		ByteArrayOutputStream str = new ByteArrayOutputStream();
		DataOutputStream bos = new DataOutputStream(str);
		DiskArchiver.serializeToStream(bos, new TimeSeriesArchivalObject("test", "cpu", "2", bucket));
		DiskArchiver.serializeToStream(bos, new TimeSeriesArchivalObject("test", "cpu", "3", bucket));
		bos.close();
		str.close();
		byte[] serializedBucket = str.toByteArray();
		DataInputStream bis = new DataInputStream(new ByteArrayInputStream(serializedBucket));
		TimeSeriesArchivalObject output = DiskArchiver.deserializeFromStream(bis);
		assertEquals("test", output.getDb());
		assertEquals("cpu", output.getMeasurement());
		assertEquals("2", output.getKey());
		bucket = output.getBucket();
		assertEquals(ts, bucket.getHeaderTimestamp());
		assertEquals(1000, bucket.getCount());
		reader = bucket.getReader(null, null);
		for (int i = 0; i < 999; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 1000, pair.getTimestamp());
			assertEquals(i, pair.getLongValue());
		}

		output = DiskArchiver.deserializeFromStream(bis);
		assertEquals("test", output.getDb());
		assertEquals("cpu", output.getMeasurement());
		assertEquals("3", output.getKey());
		bucket = output.getBucket();
		assertEquals(ts, bucket.getHeaderTimestamp());
		assertEquals(1000, bucket.getCount());
		reader = bucket.getReader(null, null);
		for (int i = 0; i < 999; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 1000, pair.getTimestamp());
			assertEquals(i, pair.getLongValue());
		}
	}

//	@Test
	public void testDiskArchiver() throws IOException, ArchiveException {
		Archiver archiver = new DiskArchiver();
		Map<String, String> conf = new HashMap<>();
		conf.put(DiskArchiver.ARCHIVAL_DISK_DIRECTORY, "target/test-diskbackup-" + System.currentTimeMillis());
		archiver.init(conf);
		long ts = System.currentTimeMillis();
		TimeSeriesBucket bucket = new TimeSeriesBucket(className, ts);
		for (int i = 0; i < 1000; i++) {
			bucket.addDataPoint(ts + i * 1000, i);
		}
		Reader reader = bucket.getReader(null, null);
		for (int i = 0; i < 1000; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 1000, pair.getTimestamp());
		}
		archiver.archive(new TimeSeriesArchivalObject("test", "cpu", "2", bucket));
		archiver.archive(new TimeSeriesArchivalObject("test", "cpu", "3", bucket));
		List<TimeSeriesArchivalObject> unarchive = archiver.unarchive();
		assertEquals(2, unarchive.size());
	}

}
