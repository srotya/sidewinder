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
package com.srotya.sidewinder.core.storage.compression.dod;

import java.nio.ByteBuffer;

/**
 * @author ambud
 */
public class BitWriter {

	public static final double RESIZE_FACTOR = 2;
	public static final int DEFAULT_BUFFER_SIZE = 128;
	private int bitsLeft = Byte.SIZE;
	private byte currentByte;
	private ByteBuffer buffer;
	private int counter;

	public BitWriter() {
		this(DEFAULT_BUFFER_SIZE);
	}
	
	public BitWriter(ByteBuffer buf) {
		buffer = buf;
	}
	
	public BitWriter(byte[] buf) {
		this(buf.length);
		buffer.put(buf);
	}

	public BitWriter(int bufferSize) {
		this.buffer = ByteBuffer.allocate(bufferSize);
	}

	public void writeBit(boolean bit) {
		if (bit) {
			currentByte |= (byte) (1 << bitsLeft - 1);
		}
		bitsLeft--;
		flush();
	}

	public void flush() {
		if (!buffer.hasRemaining()) {
			ByteBuffer temp = ByteBuffer.allocate((int) (buffer.capacity() * RESIZE_FACTOR));
			int pos = buffer.position();
			buffer.flip();
			temp.put(buffer);
			temp.position(pos);
			buffer = temp;
		}
		buffer.put(currentByte);
		if (bitsLeft > 0) {
			buffer.position(buffer.position() - 1);
		} else {
			bitsLeft = Byte.SIZE;
			currentByte = 0;
		}
		counter++;
	}

	public void writeBits(long value, int bits) {
		while (bits > 0) {
			int bit = (int) (value >> --bits) & 1;
			writeBit(bit == 1);
		}
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}

	public byte getCurrentByte() {
		return currentByte;
	}

	public static void main(String[] args) {
		int co = 1024 * 1024;
		long ts = System.currentTimeMillis();
		BitWriter writer = new BitWriter(co);
		ByteBuffer buffer = writer.getBuffer();
		for (int i = 0; i < (8 * co); i++) {
			try {
				writer.writeBit(i % 2 == 0);
			} catch (Exception e) {
				System.err.println("ii:" + i + "\t" + buffer.position() + "\t" + writer.counter);
				throw e;
			}
		}
		buffer = writer.getBuffer();
		buffer.flip();
		System.out.println("ts:" + (System.currentTimeMillis() - ts));
		BitReader reader = new BitReader(buffer);
		for (int i = 0; i < 8 * co; i++) {
			try {
				boolean r = reader.readBit();
				if (i % 2 == 0 && !r) {
					System.out.println("Bad bit");
				}
			} catch (Exception e) {
				System.out.println("Marker:" + i + "\t" + buffer.capacity() + " vs " + (1024 * co));
				e.printStackTrace();
				break;
			}
		}

		writer = new BitWriter(8);
		ts = 1490462861155L;
		writer.writeBits(ts, 64);
		writer.writeBits(256, 16);
		buffer = writer.getBuffer();
		buffer.flip();
		reader = new BitReader(buffer);
		long output = reader.readBits(64);
		int output2 = (int) reader.readBits(16);
		if (output != ts)
			System.out.println(
					"\n" + Long.toBinaryString(ts) + "\n" + Long.toBinaryString(output) + "\t" + output + "\t" + ts);

		if (output2 != 256) {
			System.out.println("Error reading: 16 bit");
		}
	}

	/**
	 * if (deltaOfDelta == 0) { writer.writeBit(false); } else if
	 * (deltaOfDelta > -63 && deltaOfDelta < 64) { writer.writeBit(true);
	 * writer.writeBit(false); writer.writeBits(deltaOfDelta, 7); } else if
	 * (deltaOfDelta > -255 && deltaOfDelta < 256) { writer.writeBit(true);
	 * writer.writeBit(true); writer.writeBit(false);
	 * writer.writeBits(deltaOfDelta, 9); } else if (deltaOfDelta > -2047 &&
	 * deltaOfDelta < 2048) { writer.writeBit(true); writer.writeBit(true);
	 * writer.writeBit(true); writer.writeBit(false);
	 * writer.writeBits(deltaOfDelta, 12); } else { writer.writeBit(true);
	 * writer.writeBit(true); writer.writeBit(true); writer.writeBit(true);
	 * writer.writeBits(deltaOfDelta, 32); } System.out.println("Writer
	 * DOD:" + deltaOfDelta);
	 **/

}
