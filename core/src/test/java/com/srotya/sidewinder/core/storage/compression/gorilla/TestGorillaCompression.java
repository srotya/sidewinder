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
package com.srotya.sidewinder.core.storage.compression.gorilla;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.compression.Reader;

public class TestGorillaCompression {

	@Test
	public void testValueCompressor() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		ByteBufferBitOutput out = new ByteBufferBitOutput(buf);
		ValueCompressor c = new ValueCompressor(out);
		for (int i = 0; i < 100; i++) {
			c.compressValue(i);
		}
		out.flush();
		buf.rewind();
		ValueDecompressor d = new ValueDecompressor(new ByteBufferBitInput(buf));
		for (int i = 0; i < 100; i++) {
			assertEquals(i, d.nextValue());
		}
	}

	@Test
	public void testCompressUncompress() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		GorillaTimestampWriter writer = new GorillaTimestampWriter();
		writer.configure(buf, true, 0);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 100; i++) {
			writer.add(ts + i * 100);
		}
		writer.makeReadOnly(false);
		Reader reader = writer.getReader();
		assertEquals(100, reader.getCount());
		for (int i = 0; i < 100; i++) {
			assertEquals(ts + i * 100, reader.read());
		}
	}

	@Test
	public void testCompressUncompressFloating() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		GorillaValueWriter writer = new GorillaValueWriter();
		writer.configure(buf, true, 0);
		for (int i = 0; i < 100; i++) {
			writer.add(i * 1.1);
		}
		writer.makeReadOnly(false);
		Reader reader = writer.getReader();
		assertEquals(100, reader.getCount());
		for (int i = 0; i < 100; i++) {
			assertEquals(i * 1.1, reader.readDouble(), 0.01);
		}
	}

	@Test
	public void testRecovery() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		GorillaTimestampWriter writer = new GorillaTimestampWriter();
		writer.configure(buf, true, 0);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 100; i++) {
			writer.add(ts + i * 100);
		}
		writer.makeReadOnly(false);
		ByteBuffer rawBytes = writer.getRawBytes();

		writer = new GorillaTimestampWriter();
		writer.configure(rawBytes, false, 0);
		Reader reader = writer.getReader();
		assertEquals(100, reader.getCount());
		for (int i = 0; i < 100; i++) {
			assertEquals(ts + i * 100, reader.read());
		}
	}

}
