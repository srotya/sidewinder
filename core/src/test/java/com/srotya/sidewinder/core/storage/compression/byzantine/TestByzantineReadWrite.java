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
package com.srotya.sidewinder.core.storage.compression.byzantine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.Writer;
import com.srotya.sidewinder.core.storage.compression.dod.DodWriter;

/**
 * @author ambud
 */
public class TestByzantineReadWrite {

	@Test
	public void testByzantineReaderInit() throws IOException {
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(new HashMap<>());
		assertEquals(0, writer.getCount());
		assertEquals(ByzantineWriter.DEFAULT_BUFFER_INIT_SIZE, writer.getBuf().capacity());
		assertEquals(0, writer.getDelta());
		assertEquals(0, writer.getPrevTs());
		assertNotNull(writer.getRead());
		assertNotNull(writer.getWrite());

		long ts = System.currentTimeMillis();
		writer = new ByzantineWriter(ts, new byte[1024]);
		assertEquals(0, writer.getCount());
		assertEquals(1024, writer.getBuf().capacity());
		assertEquals(0, writer.getDelta());
		assertEquals(ts, writer.getPrevTs());
		assertNotNull(writer.getRead());
		assertNotNull(writer.getWrite());
	}

	@Test
	public void testWriteDataPoint() throws IOException {
		ByzantineWriter bwriter = new ByzantineWriter();
		Writer writer = bwriter;
		writer.configure(new HashMap<>());

		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 10; i++) {
			writer.addValue(ts + i, i);
		}
		assertEquals(10, bwriter.getCount());
		assertEquals(bwriter.getPrevTs(), bwriter.getLastTs());
		assertEquals(ts + 9, bwriter.getLastTs());
		ByteBuffer buf = bwriter.getBuf();
		buf.flip();
		assertEquals(ts, buf.getLong());
	}

	@Test
	public void testReadWriteDataPoints() throws IOException {
		Writer writer = new ByzantineWriter();
		writer.configure(new HashMap<>());
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 100; i++) {
			writer.addValue(ts + i * 10, i);
		}

		Reader reader = writer.getReader();
		for (int i = 0; i < 100; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 10, pair.getTimestamp());
			assertEquals(i, pair.getLongValue());
		}

		for (int i = 0; i < 100; i++) {
			writer.addValue(1000 + ts + i * 10, i);
		}

		reader = writer.getReader();
		for (int i = 0; i < 200; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 10, pair.getTimestamp());
			assertEquals(i % 100, pair.getLongValue());
		}
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
	}

	@Test
	public void testWriteRead() throws IOException {
		Writer writer = new ByzantineWriter();
		writer.configure(new HashMap<>());
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 10000; i++) {
			writer.addValue(ts + i * 1000, i);
		}
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
	}

	/*
	 * 100M Read/Writer performance test
	 * 
	 * Gorilla Write time:5631 Gorilla Read time:3505 data size:450000219
	 * 
	 * DoD Write time:100x Byzantine data size: 40x Byzantine
	 * 
	 * Byzantine Write time:2543 Byzantine Read time:476 data size:300787360
	 * 
	 * Disk Byzantine Write time:2723 Disk Byzantine Read time:476
	 * 
	 */
	@Test
	public void testDiskWrites() throws IOException, InterruptedException {
		try {
			ByzantineWriter writer = new ByzantineWriter();
			HashMap<String, String> conf = new HashMap<>();
			new File("/tmp/sidewinder/data").mkdirs();
			conf.put(StorageEngine.PERSISTENCE_DISK, "true");
			writer.setSeriesId("test_1M_writes" + 0);
			writer.configure(conf);
			long ts = System.currentTimeMillis();
			writer.setHeaderTimestamp(ts);
			int limit = 1_000_000;
			for (int i = 0; i < limit; i++) {
				writer.addValue(ts + i * 1000, i);
			}
			ts = (System.currentTimeMillis() - ts);
			System.out.println("Byzantine Write time:" + ts + " data size:" + writer.getBuf().position());
			ts = System.currentTimeMillis();
			Reader reader = writer.getReader();
			for (int i = 0; i < limit; i++) {
				reader.readPair();
			}
			ts = (System.currentTimeMillis() - ts);
			System.out.println("Byzantine Read time:" + ts);
		} catch (Exception e) {
			throw e;
		}
	}

	@Test
	public void testDoDWritePerformance() {
		DodWriter writer = new DodWriter();
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 1_000_000; i++) {
			writer.addValue(ts + i * 1000, i);
		}
		ts = (System.currentTimeMillis() - ts);
		System.out.println("DoD Write time:" + ts + " data size:" + writer.getWriter().getBuffer().position());
	}

}
