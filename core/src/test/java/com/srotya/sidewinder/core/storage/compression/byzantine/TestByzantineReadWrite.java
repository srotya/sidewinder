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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.compression.dod.DodWriter;

/**
 * Unit tests for Byzantine compression: {@link ByzantineWriter} and
 * {@link ByzantineReader}
 * 
 * @author ambud
 */
public class TestByzantineReadWrite {

	@Test
	public void testByzantineReaderInit() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(new HashMap<>(), buf, false);
		assertEquals(0, writer.getCount());
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
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		ByzantineWriter bwriter = new ByzantineWriter();
		Writer writer = bwriter;
		writer.configure(new HashMap<>(), buf, true);

		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 10; i++) {
			writer.addValue(ts + i, i);
		}
		assertEquals(10, bwriter.getCount());
		assertEquals(bwriter.getPrevTs(), bwriter.getLastTs());
		assertEquals(ts + 9, bwriter.getLastTs());
		buf = bwriter.getBuf();
		buf.flip();
		assertEquals(10, buf.getInt());
		assertEquals(ts, buf.getLong());
	}

	@Test
	public void testReadWriteDataPoints() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		Writer writer = new ByzantineWriter();
		writer.configure(new HashMap<>(), buf, true);
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
		ByteBuffer buf = ByteBuffer.allocateDirect(1024*100);
		Writer writer = new ByzantineWriter();
		writer.configure(new HashMap<>(), buf, true);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 10000; i++) {
			writer.addValue(ts + i * 1000, i);
		}
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
	}

	@Test
	public void testBootstrapDiskRecovery() throws IOException, InterruptedException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(new HashMap<>(), buf, true);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValue(ots + i * 1000, i);
		}
		Reader reader = writer.getReader();
		for (int i = 0; i < limit; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ots + i * 1000, pair.getTimestamp());
			assertEquals(i, pair.getLongValue());
		}
		ByteBuffer rawBytes = writer.getRawBytes();
		try {
			writer = new ByzantineWriter();
			writer.configure(new HashMap<>(), buf, false);
			writer.bootstrap(rawBytes);
			reader = writer.getReader();
			for (int i = 0; i < limit; i++) {
				DataPoint pair = reader.readPair();
				assertEquals(ots + i * 1000, pair.getTimestamp());
				assertEquals(i, pair.getLongValue());
			}
			reader = writer.getReader();
			for (int i = 0; i < limit; i++) {
				DataPoint pair = reader.readPair();
				assertEquals(ots + i * 1000, pair.getTimestamp());
				assertEquals(i, pair.getLongValue());
			}
		} catch (Exception e) {
			throw e;
		}
	}

	@Test
	public void testDiskRecovery() throws IOException, InterruptedException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024*1024*10);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(new HashMap<>(), buf, true);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValue(ots + i * 1000, i);
		}
		long ts = (System.currentTimeMillis() - ots);
		System.out.println("==>Byzantine Write time:" + ts + " data size:" + writer.getBuf().position());
		Reader reader = writer.getReader();
		assertEquals(limit, reader.getPairCount());
		try {
			for (int i = 0; i < limit; i++) {
				DataPoint pair = reader.readPair();
				assertEquals(ots + i * 1000, pair.getTimestamp());
				assertEquals(i, pair.getLongValue());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		System.out.println("Completed phase 1 reads");
		writer = new ByzantineWriter();
		// writer.setSeriesId("test_byzantine_disk_writes" + 0);
		buf.rewind();
		writer.configure(new HashMap<>(), buf, false);
		assertEquals(1000000, writer.getCount());
		writer.addValue(ts + 10000, 1);
		try {
			reader = writer.getReader();
			for (int i = 0; i < limit; i++) {
				DataPoint pair = reader.readPair();
				assertEquals(ots + i * 1000, pair.getTimestamp());
				assertEquals(i, pair.getLongValue());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
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
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(new HashMap<>(), buf, false);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValue(ts + i * 1000, i);
		}
		ts = (System.currentTimeMillis() - ts);
		System.out.println("Byzantine Write time:" + ts + " data size:" + writer.getBuf().position());
		ts = System.currentTimeMillis();
		int i = 0;
		try {
			Reader reader = writer.getReader();
			for (i = 0; i < limit; i++) {
				reader.readPair();
			}
			ts = (System.currentTimeMillis() - ts);
			System.out.println("Byzantine Read time:" + ts);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("EOF:" + i);
			throw e;
		}
	}

	@Test
	public void testDoDWritePerformance() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(1024 * 1024 * 100);
		DodWriter writer = new DodWriter();
		writer.configure(new HashMap<>(), buf, true);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 1_000_000; i++) {
			writer.addValue(ts + i * 1000, i);
		}
		ts = (System.currentTimeMillis() - ts);
		System.out.println("DoD Write time:" + ts + " data size:" + writer.getWriter().getBuffer().position());
	}

}
