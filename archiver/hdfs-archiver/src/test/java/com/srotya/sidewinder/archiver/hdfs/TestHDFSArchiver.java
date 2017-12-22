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
package com.srotya.sidewinder.archiver.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.storage.ArchiveException;
import com.srotya.sidewinder.core.storage.Archiver;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;
import com.srotya.sidewinder.core.storage.mem.archival.TimeSeriesArchivalObject;

/**
 * @author ambud
 */
public class TestHDFSArchiver {

	@BeforeClass
	public static void beforeClass() throws IOException {
	}

	@AfterClass
	public static void afterClass() {
	}

	@Test
	public void testHDFSArchive() throws IOException, ArchiveException {
		Archiver archiver = new HDFSArchiver();
		Map<String, String> conf = new HashMap<>();
		conf.put(HDFSArchiver.HDFS_ARCHIVE_DIRECTORY, "target/test-hdfs-" + System.currentTimeMillis());
		archiver.init(conf);
		long ts = System.currentTimeMillis();
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(conf, ByteBuffer.allocate(1024 * 1024 * 2), true, 4, true);
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 1000; i++) {
			writer.addValue(ts + i * 1000, i * 1L);
		}
		Reader reader = writer.getReader();
		for (int i = 0; i < 1000; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 1000, pair.getTimestamp());
		}
		archiver.archive(
				new TimeSeriesArchivalObject("test", "cpu", "2", "asdsasd", Archiver.writerToByteArray(writer)));
		archiver.archive(
				new TimeSeriesArchivalObject("test", "cpu", "3", "asdsadas", Archiver.writerToByteArray(writer)));
		List<TimeSeriesArchivalObject> unarchive = archiver.unarchive();
		assertEquals(2, unarchive.size());
	}

}
