package com.srotya.sidewinder.core.storage.compression.gorilla;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.srotya.sidewinder.core.storage.compression.RollOverException;

/**
 * An implementation of BitOutput interface that uses off-heap storage.
 *
 * @author Michael Burman
 */
public class ByteBufferBitOutput implements BitOutput {
	public static final int DEFAULT_ALLOCATION = 4096;

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
	
	public ByteBufferBitOutput(ByteBuffer buf) {
		bb = buf;
		b = bb.get(bb.position());
	}

	/**
	 * Give an initialSize different than DEFAULT_ALLOCATIONS. Recommended to use
	 * values which are dividable by 4096.
	 *
	 * @param initialSize
	 *            New initialsize to use
	 */
	public ByteBufferBitOutput(int initialSize) {
		bb = ByteBuffer.allocateDirect(initialSize);
		b = bb.get(bb.position());
	}

	private void expandAllocation() throws RollOverException {
		// expansion is not allowed
		throw new RollOverException();
		// ByteBuffer largerBB = ByteBuffer.allocateDirect(bb.capacity() * 2);
		// bb.flip();
		// largerBB.put(bb);
		// largerBB.position(bb.capacity());
		// bb = largerBB;
	}

	private void flipByte() throws IOException{
		if (bitsLeft == 0) {
			bb.put(b);
			if (!bb.hasRemaining()) {
				expandAllocation();
			}
			b = bb.get(bb.position());
			bitsLeft = Byte.SIZE;
		}
	}

	@Override
	public void writeBit() throws IOException {
		b |= (1 << (bitsLeft - 1));
		bitsLeft--;
		flipByte();
	}

	@Override
	public void skipBit() throws IOException {
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
	 * @throws IOException 
	 */
	public void writeBits(long value, int bits) throws IOException {
		while (bits > 0) {
			int shift = bits - bitsLeft;
			if (shift >= 0) {
				b |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
				bits -= bitsLeft;
				bitsLeft = 0;
			} else {
				shift = bitsLeft - bits;
				b |= (byte) (value << shift);
				bitsLeft -= bits;
				bits = 0;
			}
			flipByte();
		}
	}

	/**
	 * Causes the currently handled byte to be written to the stream
	 * @throws IOException 
	 */
	@Override
	public void flush() throws IOException {
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
