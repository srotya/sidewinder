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
package com.srotya.sidewinder.core.storage.compression.byzantine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.srotya.sidewinder.core.predicates.GreaterThanEqualsPredicate;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.RollOverException;
import com.srotya.sidewinder.core.storage.compression.TimeWriter;

/**
 * Unit tests for Byzantine compression: {@link ByzantineValueWriter} and
 * {@link ByzantineValueReader}
 * 
 * @author ambud
 */
public class TestByzantineTimestampReadWrite {

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
		ByzantineTimestampWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, false, startOffset);
		assertEquals(0, writer.getCount());
		assertEquals(0, writer.getDelta());
		assertEquals(0, writer.getPrevTs());

		long ts = System.currentTimeMillis();
		writer = new ByzantineTimestampWriter(ts, new byte[1024]);
		assertEquals(0, writer.getCount());
		assertEquals(1024, writer.getBuf().capacity());
		assertEquals(0, writer.getDelta());
		assertEquals(ts, writer.getPrevTs());
	}

	@Test
	public void testWriteDataPoint() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		ByzantineTimestampWriter bwriter = new ByzantineTimestampWriter();
		TimeWriter writer = bwriter;
		writer.configure(buf, true, startOffset);

		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 10; i++) {
			writer.add(ts + i);
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
		TimeWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 100; i++) {
			writer.add(ts + i * 10);
		}

		Reader reader = writer.getReader();
		for (int i = 0; i < 100; i++) {
			assertEquals(ts + i * 10, reader.read());
		}

		for (int i = 0; i < 100; i++) {
			writer.add(1000 + ts + i * 10);
		}

		reader = writer.getReader();
		for (int i = 0; i < 200; i++) {
			assertEquals(ts + i * 10, reader.read());
		}
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
	}

	@Test
	public void testWriteRead() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 100);
		TimeWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		int LIMIT = 10000;
		for (int i = 0; i < LIMIT; i++) {
			writer.add(ts + i * 1000);
		}
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
		Reader reader = writer.getReader();
		for (int i = 0; i < LIMIT; i++) {
			long dp = reader.read();
			assertEquals(ts + i * 1000, dp);
		}

		buf.rewind();
		writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < LIMIT; i++) {
			writer.add(ts + i * 1000);
		}
		reader = writer.getReader();
		for (int i = 0; i < LIMIT; i++) {
			long dp = reader.read();
			assertEquals(ts + i * 1000, dp);
		}

		buf.rewind();
		writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < LIMIT; i++) {
			writer.add(ts + i * 1000);
		}
		reader = writer.getReader();
		assertEquals(LIMIT, reader.getCount());
		for (int i = 0; i < LIMIT; i++) {
			long dp = reader.read();
			assertEquals(ts + i * 1000, dp);
		}
	}

	@Test
	public void testBootstrapDiskRecovery() throws IOException, InterruptedException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineTimestampWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.add(ots + i * 1000);
		}
		Reader reader = writer.getReader();
		for (int i = 0; i < limit; i++) {
			long pair = reader.read();
			assertEquals("Iteration:" + i, ots + i * 1000, pair);
		}
		ByteBuffer rawBytes = writer.getRawBytes();
		try {
			writer = new ByzantineTimestampWriter();
			writer.configure(buf, false, startOffset);
			writer.bootstrap(rawBytes);
			reader = writer.getReader();
			for (int i = 0; i < limit; i++) {
				long pair = reader.read();
				assertEquals(ots + i * 1000, pair);
			}
			reader = writer.getReader();
			for (int i = 0; i < limit; i++) {
				long pair = reader.read();
				assertEquals(ots + i * 1000, pair);
			}
		} catch (Exception e) {
			throw e;
		}
	}

	@Test
	public void testWriteReject() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineTimestampWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.add(ots + i * 1000);
		}
		writer.makeReadOnly();
		try {
			writer.add(ots);
			fail("Must throw exception once the buffer is marked as closed");
		} catch (RejectException e) {
		}
	}

	@Test
	public void testDiskRecovery() throws IOException, InterruptedException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineTimestampWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.add(ots + i * 1000);
		}
		long ts = (System.currentTimeMillis() - ots);
		System.out.println("==>Byzantine Write time:" + ts + " data size:" + writer.getBuf().position());
		Reader reader = writer.getReader();
		assertEquals(limit, reader.getCount());
		try {
			for (int i = 0; i < limit; i++) {
				assertEquals(ots + i * 1000, reader.read());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		System.out.println("Completed phase 1 reads");
		writer = new ByzantineTimestampWriter();
		// writer.setSeriesId("test_byzantine_disk_writes" + 0);
		buf.rewind();
		writer.configure(buf, false, startOffset);
		assertEquals(1000000, writer.getCount());
		writer.add(ts + 10000);
		try {
			reader = writer.getReader();
			for (int i = 0; i < limit; i++) {
				assertEquals(ots + i * 1000, reader.read());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Test
	public void testBufferFull() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 512);
		ByzantineTimestampWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		assertEquals(ots, writer.getHeaderTimestamp());
		int limit = 1_000_000;
		try {
			for (int i = 0; i < limit; i++) {
				writer.add(ots + i * 1000);
			}
			fail("Must fill up buffer");
		} catch (RollOverException e) {
		}
		assertTrue(writer.isFull());
	}

	@Test
	public void testArrayReads() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 10);
		ByzantineTimestampWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		assertEquals(ots, writer.getHeaderTimestamp());
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.add(ots + i * 1000);
		}
		ByzantineTimestampReader reader = writer.getReader();
		int c = 0;
		assertEquals(limit, reader.getCount());
		try {
			for (int i = 0; i < limit; i++) {
				long read = reader.read();
				assertEquals(ots + i * 1000, read);
				c++;
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
		ByzantineTimestampWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		long ots = System.currentTimeMillis();
		writer.setHeaderTimestamp(ots);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.add(ots + i * 1000);
		}
		Reader reader = writer.getReader();
		reader.setPredicate(new GreaterThanEqualsPredicate(ots + 1000));
		int c = 0;
		assertEquals(limit, reader.getCount());
		try {
			for (int i = 0; i < limit; i++) {
				try {
					long pair = reader.read();
					assertEquals(ots + i * 1000, pair);
					c++;
				} catch (FilteredValueException e) {
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		assertEquals(limit - 1, c);

		reader = writer.getReader();
		reader.setPredicate(new GreaterThanEqualsPredicate(ots + 1000 * 1000));
		c = 0;
		assertEquals(limit, reader.getCount());
		try {
			for (int i = 0; i < limit; i++) {
				try {
					long pair = reader.read();
					assertEquals(ots + i * 1000, pair);
					c++;
				} catch (FilteredValueException e) {
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

		try {
			reader.read();
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
		reader.setPredicate(new GreaterThanEqualsPredicate(ots + 1000));
		c = 0;
		assertEquals(limit, reader.getCount());
		try {
			for (int i = 0; i < limit; i++) {
				try {
					long pair = reader.read();
					assertEquals(ots + i * 1000, pair);
					c++;
				} catch (FilteredValueException e) {
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		assertEquals(limit - 1, c);

		reader = writer.getReader();
		reader.setPredicate(new GreaterThanEqualsPredicate(ots + 1000 * 1000));
		c = 0;
		assertEquals(limit, reader.getCount());
		try {
			for (int i = 0; i < limit; i++) {
				try {
					long pair = reader.read();
					assertEquals(ots + i * 1000, pair);
					c++;
				} catch (FilteredValueException e) {
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
		ByzantineTimestampWriter writer = new ByzantineTimestampWriter();
		writer.configure(buf, true, startOffset);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		int limit = 1_000_000;
		for (int i = 0; i < limit; i++) {
			writer.add(ts + i * 1000);
		}
		ts = (System.currentTimeMillis() - ts);
		System.out.println("Byzantine Write time:" + ts + " data size:" + writer.getBuf().position());
		ts = System.currentTimeMillis();
		int i = 0;
		try {
			Reader reader = writer.getReader();
			for (i = 0; i < limit; i++) {
				reader.read();
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
