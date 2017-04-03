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

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * @author ambud
 */
public class TestBitWriter {

	@Test
	public void testInitalize() {
		BitWriter writer = new BitWriter();
		assertEquals(BitWriter.DEFAULT_BUFFER_SIZE, writer.getBuffer().capacity());
		writer = new BitWriter(4096);
		assertEquals(4096, writer.getBuffer().capacity());
	}

	@Test
	public void testBufferResize() {
		BitWriter writer = new BitWriter();
		for (int i = 0; i < 100; i++) {
			writer.writeBits(i, 32);
		}
		assertEquals(512, writer.getBuffer().capacity());
	}

	@Test
	public void testWriteBits() {
		// write 1011_0111
		BitWriter writer = new BitWriter(1);
		writer.writeBit(true); // 1
		writer.writeBit(false); // 0
		writer.writeBit(true); // 1
		writer.writeBit(true); // 1
		writer.writeBit(false); // 0
		writer.writeBit(true); // 1
		writer.writeBit(true); // 1
		writer.writeBit(true); // 1
		ByteBuffer buff = writer.getBuffer();
		buff.rewind();
		int b = buff.get();
		assertEquals((byte) 183, b);
	}

	@Test
	public void testWriteNumbers() {
		BitWriter writer = new BitWriter();
		int n1 = 23123123;
		writer.writeBits(n1, 32);
		short n2 = 2311;
		writer.writeBits(n2, 16);
		byte n3 = 21;
		writer.writeBits(n3, 8);
		long n4 = 312342353542334L;
		writer.writeBits(n4, 64);
		ByteBuffer buf = writer.getBuffer();
		buf.flip();
		assertEquals(n1, buf.getInt());
		assertEquals(n2, buf.getShort());
		assertEquals(n3, buf.get());
		assertEquals(n4, buf.getLong());

		buf = writer.getBuffer();
		buf.rewind();
		assertEquals(n1, buf.getInt());
		assertEquals(n2, buf.getShort());
		assertEquals(n3, buf.get());
		assertEquals(n4, buf.getLong());
	}
}
