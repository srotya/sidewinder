/**
 * Copyright 2018 Ambud Sharma
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

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.compression.Reader;

public class TestGorillaCompression {

	@Test
	public void testCompressUncompress() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		GorillaWriter writer = new GorillaWriter();
		writer.configure(new HashMap<>(), buf, true, 0, false);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 100; i++) {
			writer.addValue(ts + i * 100, i);
		}
		writer.makeReadOnly();
		Reader reader = writer.getReader();
		assertEquals(100, reader.getPairCount());
		for (int i = 0; i < 100; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 100, pair.getTimestamp());
			assertEquals(i, pair.getLongValue());
		}
	}

	@Test
	public void testCompressUncompressFloating() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(1024);
		GorillaWriter writer = new GorillaWriter();
		writer.configure(new HashMap<>(), buf, true, 0, false);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 100; i++) {
			writer.addValue(ts + i * 100, i * 1.1);
		}
		writer.makeReadOnly();
		Reader reader = writer.getReader();
		assertEquals(100, reader.getPairCount());
		for (int i = 0; i < 100; i++) {
			DataPoint pair = reader.readPair();
			assertEquals(ts + i * 100, pair.getTimestamp());
			assertEquals(i * 1.1, Double.longBitsToDouble(pair.getLongValue()), 0.01);
		}
	}

}
