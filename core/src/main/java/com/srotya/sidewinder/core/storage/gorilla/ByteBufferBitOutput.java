/**
 * Copyright 2016 Michael Burman
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
package com.srotya.sidewinder.core.storage.gorilla;

import java.nio.ByteBuffer;

/**
 * An implementation of BitOutput interface that uses off-heap storage.
 *
 * @author Michael Burman
 * 
 */
public class ByteBufferBitOutput implements BitOutput {

	public static final int DEFAULT_ALLOCATION = 4096;
	private static final boolean USE_DIRECT_BUFFER = Boolean.parseBoolean(System.getProperty("buffer.direct", "true"));
	private ByteBuffer bb;
	private byte b;
	private int bitsLeft = Byte.SIZE;

	/**
	 * Creates a new ByteBufferBitOutput with a default allocated size of 4096
	 * bytes.
	 */
	public ByteBufferBitOutput() {
		this(DEFAULT_ALLOCATION);
	}

	/**
	 * Give an initialSize different than DEFAULT_ALLOCATIONS. Recommended to
	 * use values which are dividable by 4096.
	 *
	 * @param initialSize
	 *            New initialsize to use
	 */
	public ByteBufferBitOutput(int initialSize) {
		if (USE_DIRECT_BUFFER) {
			bb = ByteBuffer.allocateDirect(initialSize);
		} else {
			bb = ByteBuffer.allocate(initialSize);
		}
		b = bb.get(bb.position());
	}

	private void expandAllocation() {
		ByteBuffer largerBB = null;
		if (USE_DIRECT_BUFFER) {
			largerBB = ByteBuffer.allocateDirect(bb.capacity() * 2);
		} else {
			largerBB = ByteBuffer.allocate(bb.capacity() * 2);
		}
		bb.flip();
		largerBB.put(bb);
		largerBB.position(bb.capacity());
		bb = largerBB;
	}

	private void flipByte() {
		if (bitsLeft == 0) {
			bb.put(b);
			if (!bb.hasRemaining()) {
				expandAllocation();
			}
			b = bb.get(bb.position());
			bitsLeft = Byte.SIZE;
		}
	}

	/**
	 * Sets the next bit (or not) and moves the bit pointer.
	 *
	 * @param bit
	 *            true == 1 or false == 0
	 */
	public void writeBit(boolean bit) {
		if (bit) {
			b |= (1 << (bitsLeft - 1));
		}
		bitsLeft--;
		flipByte();
	}

	/**
	 * Writes the given long to the stream using bits amount of meaningful bits.
	 *
	 * @param value
	 *            Value to be written to the stream
	 * @param bits
	 *            How many bits are stored to the stream
	 */
	public void writeBits(long value, int bits) {
		while (bits > 0) {
			int bitsToWrite = (bits > bitsLeft) ? bitsLeft : bits;
			if (bits > bitsLeft) {
				int shift = bits - bitsLeft;
				b |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
			} else {
				int shift = bitsLeft - bits;
				b |= (byte) (value << shift);
			}
			bits -= bitsToWrite;
			bitsLeft -= bitsToWrite;
			flipByte();
		}
	}

	/**
	 * Causes the currently handled byte to be written to the stream
	 */
	@Override
	public void flush() {
		bitsLeft = 0;
		flipByte(); // Causes write to the ByteBuffer
	}

	/**
	 * Returns the underlying DirectByteBuffer
	 *
	 * @return ByteBuffer of type DirectByteBuffer
	 */
	public ByteBuffer getByteBuffer() {
		return this.bb;
	}
}
