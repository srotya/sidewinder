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
package com.srotya.sidewinder.core.compression.byzantine;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.mem.Reader;
import com.srotya.sidewinder.core.storage.mem.Writer;

/**
 * @author ambud
 */
public class TestByzantineReadWrite {

	@Test
	public void testByzantineReaderInit() {
		ByzantineWriter writer = new ByzantineWriter();
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
	public void testWriteDataPoint() {
		ByzantineWriter bwriter = new ByzantineWriter();
		Writer writer = bwriter;

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
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 10000; i++) {
			writer.addValue(ts + i * 1000, i);
		}
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
	}

}
