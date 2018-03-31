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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.srotya.sidewinder.core.predicates.GreaterThanEqualsPredicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.RollOverException;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * Unit tests for Byzantine compression: {@link ByzantineWriter} and
 * {@link ByzantineReader}
 * 
 * @author ambud
 */
public class TestByzantineReadWrite {

	private int startOffset = 1;

	@Test
	public void testXorExtraction() {
		double p1 = 1.1;
		double p2 = 2.2;
		long p1l = Double.doubleToLongBits(p1);
		long p2l = Double.doubleToLongBits(p2);
		long xor = p1l ^ p2l;
		int numberOfLeadingZeros = Long.numberOfLeadingZeros(p1l);
		int numberOfTrailingZeros = Long.numberOfTrailingZeros(p1l);
		System.out.println(xor + "\t\t" + numberOfLeadingZeros + "\t" + numberOfTrailingZeros);
		System.out.println(p2l - p1l);
	}

	@Test
	public void testByzantineReaderInit() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(buf, false, startOffset, true);
		assertEquals(0, writer.getCount());
		assertEquals(0, writer.getDelta());
		assertEquals(0, writer.getPrevTs());
		assertNotNull(writer.getReadLock());
		assertNotNull(writer.getWriteLock());

		long ts = System.currentTimeMillis();
		writer = new ByzantineWriter(ts, new byte[1024]);
		assertEquals(0, writer.getCount());
		assertEquals(1024, writer.getBuf().capacity());
		assertEquals(0, writer.getDelta());
		assertEquals(ts, writer.getPrevTs());
		assertNotNull(writer.getReadLock());
		assertNotNull(writer.getWriteLock());
	}

	@Test
	public void testWriteDataPoint() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		ByzantineWriter bwriter = new ByzantineWriter();
		Writer writer = bwriter;
		writer.configure(buf, true, startOffset, false);

		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 10; i++) {
			writer.addValueLocked(ts + i, i);
		}
		assertEquals(10, bwriter.getCount());
		assertEquals(ts + 9, bwriter.getPrevTs());
		buf = bwriter.getBuf();
		buf.flip();
		buf.get();
		assertEquals(10, buf.getInt());
		assertEquals(ts, buf.getLong());
	}

	@Test
	public void testReadWriteDataPoints() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		Writer writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, false);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 100; i++) {
			writer.addValueLocked(ts + i * 10, i);
		}

		Reader reader = writer.getReader();
		for (int i = 0; i < 100; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 10, pair.getTimestamp());
			assertEquals(i, pair.getLongValue());
		}

		for (int i = 0; i < 100; i++) {
			DataPoint dp = new DataPoint(1000 + ts + i * 10, i);
			writer.write(dp);
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
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 100);
		Writer writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, false);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		int LIMIT = 10000;
		for (int i = 0; i < LIMIT; i++) {
			writer.addValueLocked(ts + i * 1000, i);
		}
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
		Reader reader = writer.getReader();
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = reader.readPair();
			assertEquals(ts + i * 1000, dp.getTimestamp());
		}

		buf.rewind();
		writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, true);
		ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < LIMIT; i++) {
			writer.addValueLocked(ts + i * 1000, i * 1.1);
		}
		reader = writer.getReader();
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = reader.readPair();
			assertEquals(ts + i * 1000, dp.getTimestamp());
			assertEquals(i * 1.1, dp.getValue(), startOffset);
		}

		buf.rewind();
		writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, false);
		ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = MiscUtils.buildDataPoint(ts + i * 1000, i);
			writer.write(dp);
		}
		reader = writer.getReader();
		assertEquals(LIMIT, reader.getPairCount());
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = reader.readPair();
			assertEquals(ts + i * 1000, dp.getTimestamp());
		}

		buf.rewind();
		writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, true);
		ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		List<DataPoint> dps = new ArrayList<>();
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = MiscUtils.buildDataPoint(ts + i * 1000, i);
			dps.add(dp);
		}
		writer.write(dps);
		reader = writer.getReader();
		assertEquals(LIMIT, reader.getPairCount());
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = reader.readPair();
			assertEquals(ts + i * 1000, dp.getTimestamp());
		}
	}

	@Test
	public void testWriteReadConcurrent() throws IOException, InterruptedException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);
		Writer writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, true);
		final long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);

		int LIMIT = 10000;
		final AtomicInteger wait = new AtomicInteger(0);
		int THREAD_COUNT = 4;
		ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
		for (int j = 0; j < THREAD_COUNT; j++) {
			final int o = j * LIMIT;
			es.submit(() -> {
				long t = ts + o;
				for (int i = 0; i < LIMIT; i++) {
					try {
						writer.addValueLocked(t + i * 100, i);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				wait.incrementAndGet();
			});
		}
		es.shutdown();
		es.awaitTermination(100, TimeUnit.MILLISECONDS);
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
		while (wait.get() != THREAD_COUNT) {
			Thread.sleep(1000);
		}
		Reader reader = writer.getReader();
		assertEquals(LIMIT * THREAD_COUNT, reader.getPairCount());
	}

	@Test
	public void testWriteReadNoLock() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1000);
		Writer writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, false);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		int LIMIT = 100000;
		for (int i = 0; i < LIMIT; i++) {
			writer.addValueLocked(ts + i * 1000, i);
		}
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
		Reader reader = writer.getReader();
		for (int i = 0; i < LIMIT; i++) {
			DataPoint dp = reader.readPair();
			assertEquals("Iteration:" + i, ts + i * 1000, dp.getTimestamp());
		}
	}

	@Test
	public void testBootstrapDiskRecovery() throws IOException, InterruptedException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, true);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValueLocked(ots + i * 1000, i);
		}
		Reader reader = writer.getReader();
		for (int i = 0; i < limit; i++) {
			DataPoint pair = reader.readPair();
			assertEquals("Iteration:" + i, ots + i * 1000, pair.getTimestamp());
			assertEquals("Iteration:" + i, i, pair.getLongValue());
		}
		ByteBuffer rawBytes = writer.getRawBytes();
		try {
			writer = new ByzantineWriter();
			writer.configure(buf, false, startOffset, true);
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
	public void testWriteReject() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, true);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValueLocked(ots + i * 1000, i);
		}
		writer.makeReadOnly();
		try {
			writer.addValueLocked(ots, 2L);
			fail("Must throw exception once the buffer is marked as closed");
		} catch (RejectException e) {
		}
	}

	@Test
	public void testDiskRecovery() throws IOException, InterruptedException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, true);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValueLocked(ots + i * 1000, i);
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
		writer.configure(buf, false, startOffset, true);
		assertEquals(1000000, writer.getCount());
		writer.addValueLocked(ts + 10000, 1);
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

	@Test
	public void testBufferFull() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, true);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		assertEquals(ots, writer.getHeaderTimestamp());
		int limit = 1_000_000;
		try {
			for (int i = 0; i < limit; i++) {
				writer.addValueLocked(ots + i * 1000, i);
			}
			fail("Must fill up buffer");
		} catch (RollOverException e) {
		}
		assertTrue(writer.isFull());
	}

	@Test
	public void testArrayReads() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, true);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		assertEquals(ots, writer.getHeaderTimestamp());
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValueLocked(ots + i * 1000, i);
		}
		ByzantineReader reader = writer.getReader();
		int c = 0;
		assertEquals(limit, reader.getPairCount());
		try {
			for (int i = 0; i < limit; i++) {
				long[] read = reader.read();
				if (read != null) {
					assertEquals(ots + i * 1000, read[0]);
					assertEquals(i, read[1]);
					c++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		assertEquals(limit, c);
	}

	@Test
	public void testPredicateFilter() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(buf, true, startOffset, true);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValueLocked(ots + i * 1000, i);
		}
		Reader reader = writer.getReader();
		reader.setTimePredicate(new GreaterThanEqualsPredicate(ots + 1000));
		int c = 0;
		assertEquals(limit, reader.getPairCount());
		try {
			for (int i = 0; i < limit; i++) {
				DataPoint pair = reader.readPair();
				if (pair != null) {
					assertEquals(ots + i * 1000, pair.getTimestamp());
					assertEquals(i, pair.getLongValue());
					c++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		assertEquals(limit - 1, c);

		reader = writer.getReader();
		reader.setValuePredicate(new GreaterThanEqualsPredicate(1000));
		c = 0;
		assertEquals(limit, reader.getPairCount());
		try {
			for (int i = 0; i < limit; i++) {
				DataPoint pair = reader.readPair();
				if (pair != null) {
					assertEquals(ots + i * 1000, pair.getTimestamp());
					assertEquals(i, pair.getLongValue());
					c++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		
		try {
			reader.readPair();
			fail("Must throw end of stream exception");
		} catch (RejectException e) {
		}
		try {
			reader.read();
			fail("Must throw end of stream exception");
		} catch (RejectException e) {
		}
		assertEquals(limit - 1000, c);
		assertEquals(limit, reader.getCounter());
		
		
		reader = writer.getReader();
		reader.setTimePredicate(new GreaterThanEqualsPredicate(ots + 1000));
		c = 0;
		assertEquals(limit, reader.getPairCount());
		try {
			for (int i = 0; i < limit; i++) {
				long[] pair = reader.read();
				if (pair != null) {
					assertEquals(ots + i * 1000, pair[0]);
					assertEquals(i, pair[1]);
					c++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		assertEquals(limit - 1, c);

		reader = writer.getReader();
		reader.setValuePredicate(new GreaterThanEqualsPredicate(1000));
		c = 0;
		assertEquals(limit, reader.getPairCount());
		try {
			for (int i = 0; i < limit; i++) {
				long[] pair = reader.read();
				if (pair != null) {
					assertEquals(ots + i * 1000, pair[0]);
					assertEquals(i, pair[1]);
					c++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		assertEquals(limit - 1000, c);
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
		writer.configure(buf, true, startOffset, true);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValueLocked(ts + i * 1000, i);
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

}
