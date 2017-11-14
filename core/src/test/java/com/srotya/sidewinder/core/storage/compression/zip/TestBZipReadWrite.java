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
package com.srotya.sidewinder.core.storage.compression.zip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineReader;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;

/**
 * Unit tests for Byzantine compression: {@link ByzantineWriter} and
 * {@link ByzantineReader}
 * 
 * @author ambud
 */
public class TestBZipReadWrite {
	
	private static final HashMap<String, String> CONF = new HashMap<>();
	private int startOffset = 1;
	
	@BeforeClass
	public static void beforeClass() {
		CONF.put("zip.block.size", "9");
	}

	@Test
	public void testZipReaderInit() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		Writer writer = new BZip2Writer();
		writer.configure(CONF, buf, false, startOffset, false);
		assertEquals(0, writer.getCount());

		writer = new BZip2Writer();
		assertEquals(0, writer.getCount());
	}

	@Test
	public void testWriteDataPoint() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		Writer bwriter = new BZip2Writer();
		Writer writer = bwriter;
		writer.configure(CONF, buf, true, startOffset, false);

		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 10; i++) {
			writer.addValue(ts + i, i);
		}
		writer.makeReadOnly();
		buf.flip();
		buf.get();
		assertEquals(10, bwriter.getCount());
		assertEquals(10, buf.getInt());
	}

	@Test
	public void testReadWriteDataPoints() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		Writer writer = new BZip2Writer();
		writer.configure(CONF, buf, true, startOffset, false);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 100; i++) {
			writer.addValue(ts + i * 10, i);
		}
		
		writer.makeReadOnly();
		Reader reader = writer.getReader();
		for (int i = 0; i < 100; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 10, pair.getTimestamp());
			assertEquals(i, pair.getLongValue());
		}

		reader = writer.getReader();
		assertEquals(100, reader.getPairCount());
		for (int i = 0; i < 100; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 10, pair.getTimestamp());
			assertEquals(i, pair.getLongValue());
		}
		System.out.println("Compression Ratio:" + writer.getCompressionRatio());
		
		try {
			writer.addValue(ts, ts);
			fail("Must throw exception since the writer should be read-only");
		} catch (Exception e) {
		}
	}

}
